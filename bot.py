import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from telebot import TeleBot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

BOT_TOKEN = os.environ["BOT_TOKEN"]
bot = TeleBot(BOT_TOKEN)
DB_PATH = "/app/data/timetrack.db"

# ─── Роли ─────────────────────────────────────────────────────────────────────
ROLE_LABELS = {
    "worker":     "👷 Рабочий",
    "manager":    "👔 Начальник Цеха",
    "admin":      "⚙️ Администратор",
    "superadmin": "👑 Главный админ",
}
SUPERVISOR_ROLES = ("manager", "admin", "superadmin")
ADMIN_MANAGED    = ("worker", "manager")
SUPER_MANAGED    = ("worker", "manager", "admin")

ORDER_STATUS_LABELS = {
    "new":        "🆕 Новый",
    "accepted":   "✅ Принят",
    "in_progress":"🔧 В работе",
    "ready":      "🏁 Готов",
    "shipping":   "📦 На отгрузке",
    "shipped":    "✅ Отгружен",
}

MONTHS_RU = {
    1:"Январь", 2:"Февраль", 3:"Март",    4:"Апрель",
    5:"Май",    6:"Июнь",    7:"Июль",    8:"Август",
    9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}

INITIAL_NOMENCLATURE = [
    ("Гарпун Вид 1 (уз) белый",     "м",  "Намотка 200м"),
    ("Гарпун Вид 1 (уз) чёрный",    "м",  "Намотка 200м"),
    ("Гарпун Вид 2 (шир) белый",    "м",  "Намотка 200м"),
    ("Гарпун Вид 2 (шир) чёрный",   "м",  "Намотка 200м"),
    ("Вставка Т «Элит»",            "м",  "Первичное сырьё, намотка 50/150м"),
    ("Вставка Т чёрная",            "м",  "Намотка 50/150м"),
    ("Вставка Т",                   "м",  "Намотка 50/150м"),
    ("Вставка Уголок белая",        "м",  "Намотка 50/100м"),
    ("Вставка Уголок чёрная",       "м",  "Намотка 50/100м"),
    ("Багет ПВХ стеновой 150 г/м",  "м",  "Вид 1, для пистолета, 2м/2.5м"),
    ("Багет ПВХ стеновой 140 г/м",  "м",  "Вид 2, 2м/2.5м"),
    ("Платформа унив. 60-110",      "шт", "Серая, 50шт/короб, 150шт/мешок"),
    ("Платформа 90",                "шт", "Серая, 50шт/короб, 150шт/мешок"),
]

# ─── Состояния диалога ────────────────────────────────────────────────────────
user_states = {}
user_data   = {}

# ─── База данных ──────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            name        TEXT NOT NULL,
            role        TEXT NOT NULL,
            daily_rate  REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS time_records (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER NOT NULL,
            check_in       TIMESTAMP NOT NULL,
            check_out      TIMESTAMP,
            status         TEXT DEFAULT "active",
            reminder_count INTEGER DEFAULT 0,
            last_reminder  TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS requests (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            type       TEXT NOT NULL,
            text       TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            status     TEXT DEFAULT "new",
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS nomenclature (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            name   TEXT NOT NULL,
            unit   TEXT NOT NULL,
            notes  TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS counterparties (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT NOT NULL,
            phone   TEXT,
            email   TEXT,
            address TEXT,
            notes   TEXT,
            active  INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS orders (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            counterparty_id INTEGER,
            created_by      INTEGER NOT NULL,
            created_at      TIMESTAMP NOT NULL,
            desired_date    TEXT,
            status          TEXT DEFAULT "new",
            FOREIGN KEY (counterparty_id) REFERENCES counterparties(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id        INTEGER NOT NULL,
            nomenclature_id INTEGER NOT NULL,
            quantity        REAL NOT NULL,
            shipped_qty     REAL DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (nomenclature_id) REFERENCES nomenclature(id)
        );
        CREATE TABLE IF NOT EXISTS shipments (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id     INTEGER NOT NULL,
            created_by   INTEGER NOT NULL,
            created_at   TIMESTAMP NOT NULL,
            confirmed_by INTEGER,
            confirmed_at TIMESTAMP,
            status       TEXT DEFAULT "pending",
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
        CREATE TABLE IF NOT EXISTS shipment_items (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            shipment_id   INTEGER NOT NULL,
            order_item_id INTEGER NOT NULL,
            quantity      REAL NOT NULL,
            FOREIGN KEY (shipment_id) REFERENCES shipments(id),
            FOREIGN KEY (order_item_id) REFERENCES order_items(id)
        );
        CREATE TABLE IF NOT EXISTS daily_production (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            nomenclature_id INTEGER NOT NULL,
            quantity        REAL NOT NULL,
            recorded_by     INTEGER NOT NULL,
            recorded_at     TIMESTAMP NOT NULL,
            FOREIGN KEY (nomenclature_id) REFERENCES nomenclature(id),
            FOREIGN KEY (recorded_by) REFERENCES users(id)
        );
    ''')
    # Заполняем номенклатуру если пустая
    count = conn.execute("SELECT COUNT(*) FROM nomenclature").fetchone()[0]
    if count == 0:
        conn.executemany(
            "INSERT INTO nomenclature (name, unit, notes) VALUES (?, ?, ?)",
            INITIAL_NOMENCLATURE
        )
    conn.commit()
    migrate_db(conn)
    conn.close()

def migrate_db(conn):
    def get_columns(table):
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return [row[1] for row in rows]
        except Exception:
            return []

    users_cols = get_columns("users")
    if "daily_rate" not in users_cols:
        try:
            conn.execute("ALTER TABLE users ADD COLUMN daily_rate REAL DEFAULT 0")
        except Exception:
            pass

    req_cols = get_columns("requests")
    if "status" not in req_cols:
        try:
            conn.execute("ALTER TABLE requests ADD COLUMN status TEXT DEFAULT 'new'")
        except Exception:
            pass

    conn.commit()
    print("✅ Миграция базы данных выполнена")

init_db()

# ─── Вспомогательные функции ──────────────────────────────────────────────────

def get_user(telegram_id):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
    conn.close()
    return user

def notify_supervisors(text):
    conn = get_db()
    rows = conn.execute(
        "SELECT telegram_id FROM users WHERE role IN ('manager','admin','superadmin')"
    ).fetchall()
    conn.close()
    for r in rows:
        try:
            bot.send_message(r["telegram_id"], text, parse_mode="Markdown")
        except Exception:
            pass

def notify_role(role, text):
    conn = get_db()
    rows = conn.execute("SELECT telegram_id FROM users WHERE role = ?", (role,)).fetchall()
    conn.close()
    for r in rows:
        try:
            bot.send_message(r["telegram_id"], text, parse_mode="Markdown")
        except Exception:
            pass

def get_stock(nomenclature_id):
    """Наличие = сумма ежедневного учёта − отгружено (подтверждённые отгрузки)"""
    conn = get_db()
    produced = conn.execute(
        "SELECT COALESCE(SUM(quantity),0) FROM daily_production WHERE nomenclature_id=?",
        (nomenclature_id,)
    ).fetchone()[0]
    shipped = conn.execute(
        """SELECT COALESCE(SUM(si.quantity),0) FROM shipment_items si
           JOIN shipments s ON s.id=si.shipment_id
           JOIN order_items oi ON oi.id=si.order_item_id
           WHERE oi.nomenclature_id=? AND s.status='confirmed'""",
        (nomenclature_id,)
    ).fetchone()[0]
    conn.close()
    return produced - shipped

def order_number(order_id):
    return f"{datetime.now().year}-{order_id:03d}"

def nom_list_text(conn=None):
    close = False
    if conn is None:
        conn = get_db(); close = True
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall()
    if close:
        conn.close()
    lines = ["📋 *Номенклатура:*\n"]
    for i, it in enumerate(items, 1):
        note = f" — _{it['notes']}_" if it["notes"] else ""
        lines.append(f"{i}. {it['name']} ({it['unit']}){note}")
    return "\n".join(lines), items

def cp_list_text():
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall()
    conn.close()
    if not cps:
        return "Контрагентов нет. Добавьте через '➕ Контрагент'.", []
    lines = ["👥 *Контрагенты:*\n"]
    for i, cp in enumerate(cps, 1):
        lines.append(f"{i}. {cp['name']}")
    return "\n".join(lines), cps

def cancel_state(tid):
    user_states.pop(tid, None)
    user_data.pop(tid, None)

# ─── Клавиатуры ───────────────────────────────────────────────────────────────

def worker_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("✅ Пришёл на работу"), KeyboardButton("🚪 Ушёл с работы"))
    kb.row(KeyboardButton("📈 Моя статистика"))
    kb.row(KeyboardButton("📅 Не выйду на работу"), KeyboardButton("🔧 Неисправность оборудования"))
    kb.row(KeyboardButton("📦 Заявка на МТС"), KeyboardButton("❓ Справка"))
    return kb

def manager_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Отчёт за неделю"), KeyboardButton("📊 Отчёт за месяц"))
    kb.row(KeyboardButton("👷 Кто сейчас на смене"))
    kb.row(KeyboardButton("📦 Заказы"), KeyboardButton("📊 Производство за день"))
    kb.row(KeyboardButton("🏭 Склад"), KeyboardButton("✅ Подтвердить отгрузку"))
    kb.row(KeyboardButton("📅 Не выйду на работу"), KeyboardButton("🔧 Неисправность оборудования"))
    kb.row(KeyboardButton("📦 Заявка на МТС"), KeyboardButton("❓ Справка"))
    return kb

def admin_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Отчёт за неделю"), KeyboardButton("📊 Отчёт за месяц"))
    kb.row(KeyboardButton("👷 Кто сейчас на смене"), KeyboardButton("👥 Сотрудники"))
    kb.row(KeyboardButton("📦 Заказы"), KeyboardButton("👥 Контрагенты"))
    kb.row(KeyboardButton("📋 Номенклатура"), KeyboardButton("🏭 Склад"))
    kb.row(KeyboardButton("📅 Не выйду на работу"), KeyboardButton("🔧 Неисправность оборудования"))
    kb.row(KeyboardButton("📦 Заявка на МТС"), KeyboardButton("❓ Справка"))
    return kb

def superadmin_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Отчёт за неделю"), KeyboardButton("📊 Отчёт за месяц"))
    kb.row(KeyboardButton("👷 Кто сейчас на смене"), KeyboardButton("👥 Сотрудники"))
    kb.row(KeyboardButton("📦 Заказы"), KeyboardButton("👥 Контрагенты"))
    kb.row(KeyboardButton("📋 Номенклатура"), KeyboardButton("🏭 Склад"))
    kb.row(KeyboardButton("📅 Не выйду на работу"), KeyboardButton("🔧 Неисправность оборудования"))
    kb.row(KeyboardButton("📦 Заявка на МТС"), KeyboardButton("❓ Справка"))
    return kb

KEYBOARDS = {"worker": worker_kb, "manager": manager_kb, "admin": admin_kb, "superadmin": superadmin_kb}

def send_menu(tid, role, name):
    emoji = {"worker":"👷","manager":"👔","admin":"⚙️","superadmin":"👑"}
    kb = KEYBOARDS.get(role, worker_kb)()
    bot.send_message(tid, f"{emoji.get(role,'')} Привет, {name}!", reply_markup=kb)

# ─── Справки ──────────────────────────────────────────────────────────────────

HELP_TEXTS = {
    "worker": """📖 *Справка — Рабочий*

*Учёт времени:*
✅ Пришёл / 🚪 Ушёл — отметь приход и уход
📈 Моя статистика — часы и заработок

*Заявки:*
📅 Не выйду на работу
🔧 Неисправность оборудования
📦 Заявка на МТС

⚠️ Не забывай отмечать уход!""",

    "manager": """📖 *Справка — Начальник Цеха*

*Отчёты:* за неделю, месяц, кто на смене

*Производство:*
📦 Заказы — просмотр и смена статусов
📊 Производство за день — ввод выпуска
🏭 Склад — наличие по номенклатуре
✅ Подтвердить отгрузку — подтверждение

*Статусы заказа:*
🆕 Новый → ✅ Принят → 🔧 В работе → 🏁 Готов

*Заявки:* неявка, неисправность, МТС""",

    "admin": """📖 *Справка — Администратор*

*Сотрудники:* /add /remove /setrate
*Заказы:* создание и отгрузка
*Контрагенты:* управление базой
*Номенклатура:* просмотр и добавление

*Команды:*
/add [id] [имя] [роль]
/remove [id]
/setrate [id] [сумма]""",

    "superadmin": """📖 *Справка — Главный Админ*

Полный доступ к системе.

*Команды:*
/add [id] [имя] [роль] — роли: worker|manager|admin
/remove [id]
/setrate [id] [сумма]
/setup — первичная регистрация (уже выполнена)"""
}

# ─── /start  /myid  /cancel ───────────────────────────────────────────────────

@bot.message_handler(commands=["start"])
def cmd_start(message):
    cancel_state(message.from_user.id)
    user = get_user(message.from_user.id)
    if user:
        send_menu(message.from_user.id, user["role"], user["name"])
    else:
        bot.send_message(message.from_user.id,
            "👋 Вы не зарегистрированы.\n\nСообщите администратору ваш Telegram ID:\n"
            f"<code>{message.from_user.id}</code>", parse_mode="HTML")

@bot.message_handler(commands=["myid"])
def cmd_myid(message):
    bot.send_message(message.from_user.id,
        f"Ваш Telegram ID: <code>{message.from_user.id}</code>", parse_mode="HTML")

@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    cancel_state(message.from_user.id)
    user = get_user(message.from_user.id)
    if user:
        send_menu(message.from_user.id, user["role"], user["name"])
    else:
        bot.send_message(message.from_user.id, "Отменено.")

# ─── /setup ───────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["setup"])
def cmd_setup(message):
    conn = get_db()
    if conn.execute("SELECT id FROM users WHERE role='superadmin'").fetchone():
        conn.close()
        bot.send_message(message.from_user.id, "⛔ Главный админ уже зарегистрирован.")
        return
    first = message.from_user.first_name or ""
    last  = message.from_user.last_name  or ""
    name  = f"{first} {last}".strip() or f"SuperAdmin_{message.from_user.id}"
    if conn.execute("SELECT id FROM users WHERE telegram_id=?", (message.from_user.id,)).fetchone():
        conn.execute("UPDATE users SET role='superadmin', name=? WHERE telegram_id=?",
                     (name, message.from_user.id))
    else:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,'superadmin')",
                     (message.from_user.id, name))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Вы зарегистрированы как *Главный Админ*!\n\n"
        f"Имя: *{name}*\nID: `{message.from_user.id}`\n\n"
        f"⚠️ Команда /setup заблокирована.\n"
        f"Добавляйте сотрудников: `/add [id] [имя] [роль]`",
        parse_mode="Markdown")
    send_menu(message.from_user.id, "superadmin", name)

# ─── /add /remove /setrate ────────────────────────────────────────────────────

@bot.message_handler(commands=["add"])
def cmd_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 4:
        hint = "`worker`|`manager`|`admin`" if user["role"]=="superadmin" else "`worker`|`manager`"
        bot.send_message(message.from_user.id,
            f"📌 `/add [telegram_id] [имя фамилия] [роль]`\nРоли: {hint}\n"
            f"Пример: `/add 123456789 Иван Иванов worker`", parse_mode="Markdown"); return
    tid_str = parts[1]; role = parts[-1]; name = " ".join(parts[2:-1])
    allowed = SUPER_MANAGED if user["role"]=="superadmin" else ADMIN_MANAGED
    if role not in allowed:
        bot.send_message(message.from_user.id,
            f"❌ Нельзя добавить роль `{role}`.\nДоступные: {', '.join(f'`{r}`' for r in allowed)}",
            parse_mode="Markdown"); return
    try:
        tid = int(tid_str)
    except ValueError:
        bot.send_message(message.from_user.id, "❌ ID должен быть числом."); return
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (tid,name,role))
        conn.commit()
        bot.send_message(message.from_user.id,
            f"✅ Добавлен: *{name}* — {ROLE_LABELS[role]}", parse_mode="Markdown")
        try:
            bot.send_message(tid, f"✅ Вы зарегистрированы как *{name}*.\nНажмите /start",
                             parse_mode="Markdown")
        except Exception:
            pass
    except sqlite3.IntegrityError:
        bot.send_message(message.from_user.id, "⚠️ Пользователь уже есть в системе.")
    finally:
        conn.close()

@bot.message_handler(commands=["remove"])
def cmd_remove(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.from_user.id, "Использование: `/remove [telegram_id]`",
                         parse_mode="Markdown"); return
    try:
        tid = int(parts[1])
    except ValueError:
        bot.send_message(message.from_user.id, "❌ ID должен быть числом."); return
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not target:
        bot.send_message(message.from_user.id, "❌ Пользователь не найден."); conn.close(); return
    allowed = SUPER_MANAGED if user["role"]=="superadmin" else ADMIN_MANAGED
    if target["role"] not in allowed:
        bot.send_message(message.from_user.id,
            f"❌ Нельзя удалить {ROLE_LABELS.get(target['role'],target['role'])}."); conn.close(); return
    conn.execute("DELETE FROM users WHERE telegram_id=?", (tid,)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ *{target['name']}* удалён.", parse_mode="Markdown")

@bot.message_handler(commands=["setrate"])
def cmd_setrate(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "superadmin":
        bot.send_message(message.from_user.id, "⛔ Только Главный Админ."); return
    parts = message.text.split()
    if len(parts) < 3:
        bot.send_message(message.from_user.id,
            "📌 `/setrate [telegram_id] [сумма]`\nПример: `/setrate 123456789 2500`",
            parse_mode="Markdown"); return
    try:
        tid = int(parts[1]); rate = float(parts[2])
    except ValueError:
        bot.send_message(message.from_user.id, "❌ Неверный формат."); return
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not target or target["role"] != "worker":
        bot.send_message(message.from_user.id, "❌ Рабочий не найден."); conn.close(); return
    conn.execute("UPDATE users SET daily_rate=? WHERE telegram_id=?", (rate, tid))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Оклад установлен!\n\n👷 *{target['name']}*\n"
        f"💰 Дневная ставка: *{rate:,.0f} ₽* (за 8 ч.)\n"
        f"⏱ Часовая ставка: *{rate/8:,.2f} ₽/ч*", parse_mode="Markdown")

# ─── Приход / Уход ────────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "✅ Пришёл на работу")
def check_in(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db()
    active = conn.execute(
        "SELECT * FROM time_records WHERE user_id=? AND status='active'", (user["id"],)
    ).fetchone()
    if active:
        t = datetime.fromisoformat(active["check_in"]).strftime("%H:%M")
        bot.send_message(message.from_user.id, f"⚠️ Ты уже на работе с {t}."); conn.close(); return
    now = datetime.now()
    conn.execute("INSERT INTO time_records (user_id,check_in) VALUES (?,?)", (user["id"],now))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Приход зафиксирован в *{now.strftime('%H:%M')}*", parse_mode="Markdown")
    notify_supervisors(f"✅ *{user['name']}* пришёл на работу в {now.strftime('%H:%M')}")

@bot.message_handler(func=lambda m: m.text == "🚪 Ушёл с работы")
def check_out(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db()
    active = conn.execute(
        "SELECT * FROM time_records WHERE user_id=? AND status='active'", (user["id"],)
    ).fetchone()
    if not active:
        bot.send_message(message.from_user.id, "⚠️ Ты не отмечен как на работе!"); conn.close(); return
    now = datetime.now()
    hours = (now - datetime.fromisoformat(active["check_in"])).total_seconds() / 3600
    conn.execute("UPDATE time_records SET check_out=?,status='closed' WHERE id=?", (now,active["id"]))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"👋 Уход зафиксирован в *{now.strftime('%H:%M')}*\n⏱ Отработано: *{hours:.1f} ч.*",
        parse_mode="Markdown")
    notify_supervisors(f"🚪 *{user['name']}* ушёл в {now.strftime('%H:%M')} (отработал {hours:.1f} ч.)")

# ─── Статистика рабочего ──────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "📈 Моя статистика")
def my_stats(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db(); now = datetime.now()
    def calc(since):
        recs = conn.execute(
            "SELECT * FROM time_records WHERE user_id=? AND check_in>=? AND status='closed'",
            (user["id"], since)
        ).fetchall()
        total = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in recs)
        days  = len({datetime.fromisoformat(r["check_in"]).date() for r in recs})
        return days, total
    week_start  = now - timedelta(days=7)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    wd, wh = calc(week_start); md, mh = calc(month_start)
    rate = user["daily_rate"] or 0; hourly = rate/8 if rate>0 else 0
    active = conn.execute(
        "SELECT check_in FROM time_records WHERE user_id=? AND status='active'", (user["id"],)
    ).fetchone(); conn.close()
    lines = [f"📈 *Моя статистика — {user['name']}*\n"]
    if active:
        dt = datetime.fromisoformat(active["check_in"]); hrs = (now-dt).total_seconds()/3600
        lines.append(f"🟢 Сейчас на смене с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)\n")
    wn = now.isocalendar()[1]; mn = MONTHS_RU[now.month]
    lines.append(f"📅 *Неделя №{wn}:* {wd} дн. | {wh:.1f} ч.")
    if rate>0: lines.append(f"💰 Заработок: *{wh*hourly:,.0f} ₽*")
    lines.append("")
    lines.append(f"📆 *{mn} {now.year}:* {md} дн. | {mh:.1f} ч.")
    if rate>0: lines.append(f"💰 Заработок: *{mh*hourly:,.0f} ₽*")
    if rate>0: lines.append(f"\n⏱ Ставка: *{rate:,.0f} ₽/день* | *{hourly:,.2f} ₽/ч*")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

# ─── Заявки ───────────────────────────────────────────────────────────────────

REQUEST_TYPES  = {"📅 Не выйду на работу":"absence","🔧 Неисправность оборудования":"breakdown","📦 Заявка на МТС":"mts"}
REQUEST_LABELS = {"absence":"📅 Не выйдет на работу","breakdown":"🔧 Неисправность оборудования","mts":"📦 Заявка на МТС"}
REQUEST_PROMPTS= {"absence":"📅 Укажи дату и причину отсутствия:","breakdown":"🔧 Опиши неисправность:","mts":"📦 Опиши что нужно:"}

@bot.message_handler(func=lambda m: m.text in REQUEST_TYPES)
def request_start(message):
    user = get_user(message.from_user.id)
    if not user: return
    rtype = REQUEST_TYPES[message.text]
    user_states[message.from_user.id] = f"request_{rtype}"
    bot.send_message(message.from_user.id, REQUEST_PROMPTS[rtype]+"\n\n_/cancel для отмены_",
                     parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id,"").startswith("request_"))
def request_text(message):
    user = get_user(message.from_user.id)
    if not user: return
    state = user_states.pop(message.from_user.id); rtype = state.replace("request_","")
    now = datetime.now()
    conn = get_db()
    conn.execute("INSERT INTO requests (user_id,type,text,created_at) VALUES (?,?,?,?)",
                 (user["id"],rtype,message.text.strip(),now)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, "✅ Заявка отправлена!")
    send_menu(message.from_user.id, user["role"], user["name"])
    notify_supervisors(
        f"📋 *{REQUEST_LABELS[rtype]}*\n\n"
        f"👤 *{user['name']}* ({ROLE_LABELS.get(user['role'],'')})\n"
        f"🕐 {now.strftime('%d.%m.%Y %H:%M')}\n\n📝 {message.text.strip()}")

# ─── Справка ──────────────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "❓ Справка")
def help_handler(message):
    user = get_user(message.from_user.id)
    if not user: return
    bot.send_message(message.from_user.id, HELP_TEXTS.get(user["role"],""), parse_mode="Markdown")

# ─── Отчёты ───────────────────────────────────────────────────────────────────

def generate_report(days):
    conn = get_db(); now = datetime.now()
    if days == 7:
        since = now - timedelta(days=7)
        wn = now.isocalendar()[1]
        label = f"неделю №{wn} ({since.strftime('%d.%m')}–{now.strftime('%d.%m.%Y')})"
    else:
        since = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        label = f"{MONTHS_RU[now.month]} {now.year} (с 01.{now.month:02d} по {now.strftime('%d.%m.%Y')})"
    workers = conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall()
    if not workers:
        conn.close(); return "📊 Нет зарегистрированных рабочих."
    lines = [f"📊 *Отчёт за {label}*\n"]
    for w in workers:
        recs    = conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=?",
                               (w["id"],since)).fetchall()
        closed  = [r for r in recs if r["status"]=="closed"]
        no_mark = [r for r in recs if r["status"]=="no_checkout"]
        total_h = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in closed)
        dw      = len({datetime.fromisoformat(r["check_in"]).date() for r in closed})
        rate    = w["daily_rate"] or 0; hourly = rate/8 if rate>0 else 0
        reg_h = over_h = 0.0
        for r in closed:
            h = (datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600
            reg_h += min(h,8); over_h += max(0,h-8)
        lines.append(f"👷 *{w['name']}*")
        lines.append(f"   Дней: {dw}  |  Часов: {total_h:.1f}")
        if over_h > 0: lines.append(f"   ⏱ Переработка: {over_h:.1f} ч.")
        if rate > 0:   lines.append(f"   💰 Заработок: *{(reg_h+over_h)*hourly:,.0f} ₽*")
        if no_mark:    lines.append(f"   ⚠️ Не отметил уход: {len(no_mark)} раз(а)")
        lines.append("")
    conn.close(); return "\n".join(lines)

@bot.message_handler(func=lambda m: m.text == "📊 Отчёт за неделю")
def report_week(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    bot.send_message(message.from_user.id, generate_report(7), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "📊 Отчёт за месяц")
def report_month(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    bot.send_message(message.from_user.id, generate_report(30), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "👷 Кто сейчас на смене")
def on_shift(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    conn = get_db()
    active = conn.execute(
        "SELECT u.name,t.check_in FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active' ORDER BY t.check_in"
    ).fetchall(); conn.close()
    if not active:
        bot.send_message(message.from_user.id, "👷 Сейчас никого нет на смене."); return
    lines = ["👷 *Сейчас на смене:*\n"]
    for r in active:
        dt = datetime.fromisoformat(r["check_in"]); hrs = (datetime.now()-dt).total_seconds()/3600
        lines.append(f"• *{r['name']}* — с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "👥 Сотрудники")
def list_users(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"): return
    conn = get_db()
    users = conn.execute(
        "SELECT * FROM users WHERE role IN ('worker','manager')" if user["role"]=="admin"
        else "SELECT * FROM users ORDER BY role,name"
    ).fetchall(); conn.close()
    if not users:
        bot.send_message(message.from_user.id, "Список пуст."); return
    lines = ["👥 *Список сотрудников:*\n"]
    for u in users:
        lines.append(f"{ROLE_LABELS.get(u['role'],u['role'])}: *{u['name']}*")
        lines.append(f"`ID: {u['telegram_id']}`")
        if u["role"] == "worker":
            rate = u["daily_rate"] or 0
            if rate > 0:
                lines.append(f"💰 Оклад: {rate:,.0f} ₽/день")
            else:
                lines.append(f"💰 Оклад: _не установлен_")
        lines.append("")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# ЧАСТЬ 2: ПРОИЗВОДСТВО И ЗАКАЗЫ
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Номенклатура ─────────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "📋 Номенклатура")
def show_nomenclature(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"): return
    text, _ = nom_list_text()
    text += "\n\nДля добавления: `/nom_add Название | ед | примечание`"
    bot.send_message(message.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["nom_add"])
def nom_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    raw = message.text.replace("/nom_add","",1).strip()
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 2:
        bot.send_message(message.from_user.id,
            "📌 Формат: `/nom_add Название | ед.изм. | примечание`\n"
            "Пример: `/nom_add Гарпун новый | м | намотка 100м`",
            parse_mode="Markdown"); return
    name = parts[0]; unit = parts[1]; notes = parts[2] if len(parts)>2 else ""
    conn = get_db()
    conn.execute("INSERT INTO nomenclature (name,unit,notes) VALUES (?,?,?)", (name,unit,notes))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Добавлено: *{name}* ({unit})", parse_mode="Markdown")

# ─── Контрагенты ──────────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "👥 Контрагенты")
def show_counterparties(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"): return
    text, cps = cp_list_text()
    text += "\n\n➕ *Добавить:* нажмите '➕ Контрагент'\n🔍 *Детали:* `/cp [номер]`"
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall()
    conn.close()
    lines = ["👥 *Контрагенты:*\n"]
    if not cps:
        lines.append("Список пуст.")
    else:
        for i, cp in enumerate(cps, 1):
            lines.append(f"{i}. *{cp['name']}*")
            if cp["phone"]: lines.append(f"   📞 {cp['phone']}")
            if cp["email"]: lines.append(f"   📧 {cp['email']}")
            if cp["address"]: lines.append(f"   📍 {cp['address']}")
            if cp["notes"]: lines.append(f"   📝 {cp['notes']}")
            lines.append("")
    lines.append("➕ Добавить: `/cp_add Название | телефон | email | адрес | примечание`")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(commands=["cp_add"])
def cp_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    raw   = message.text.replace("/cp_add","",1).strip()
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 1 or not parts[0]:
        bot.send_message(message.from_user.id,
            "📌 Формат:\n`/cp_add Название | телефон | email | адрес | примечание`\n"
            "Пример:\n`/cp_add ООО АРС | +7(985)077-83-46 | pvc@mail.ru | Павловский Посад | оптовый`",
            parse_mode="Markdown"); return
    name    = parts[0]
    phone   = parts[1] if len(parts)>1 else ""
    email   = parts[2] if len(parts)>2 else ""
    address = parts[3] if len(parts)>3 else ""
    notes   = parts[4] if len(parts)>4 else ""
    conn = get_db()
    conn.execute("INSERT INTO counterparties (name,phone,email,address,notes) VALUES (?,?,?,?,?)",
                 (name,phone,email,address,notes))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Контрагент добавлен: *{name}*", parse_mode="Markdown")

@bot.message_handler(commands=["cp_del"])
def cp_del(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.from_user.id, "Использование: `/cp_del [id]`",
                         parse_mode="Markdown"); return
    conn = get_db()
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (parts[1],)).fetchone()
    if not cp:
        bot.send_message(message.from_user.id, "❌ Контрагент не найден."); conn.close(); return
    conn.execute("UPDATE counterparties SET active=0 WHERE id=?", (parts[1],))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ *{cp['name']}* удалён.", parse_mode="Markdown")

# ─── Просмотр склада ──────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "🏭 Склад")
def show_warehouse(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall()
    conn.close()
    lines = ["🏭 *Склад — текущий остаток:*\n"]
    for it in items:
        stock = get_stock(it["id"])
        icon = "✅" if stock > 0 else ("⚠️" if stock == 0 else "🔴")
        lines.append(f"{icon} *{it['name']}*: {stock:,.1f} {it['unit']}")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

# ─── Просмотр заказов ─────────────────────────────────────────────────────────

def format_order(order_id, show_stock=True):
    conn = get_db()
    o  = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    if not o:
        conn.close(); return "Заказ не найден."
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (o["counterparty_id"],)).fetchone()
    cr = conn.execute("SELECT name FROM users WHERE id=?", (o["created_by"],)).fetchone()
    items = conn.execute(
        "SELECT oi.*,n.name,n.unit FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE oi.order_id=?",
        (order_id,)
    ).fetchall()
    conn.close()
    status = ORDER_STATUS_LABELS.get(o["status"], o["status"])
    cp_name = cp["name"] if cp else "—"
    lines = [
        f"📦 *Заказ №{order_number(o['id'])}*",
        f"👥 Контрагент: *{cp_name}*",
        f"📅 Создан: {datetime.fromisoformat(o['created_at']).strftime('%d.%m.%Y %H:%M')}",
        f"⏰ Готовность: {o['desired_date'] or '—'}",
        f"👤 Создал: {cr['name'] if cr else '—'}",
        f"📊 Статус: {status}\n",
        "*Позиции:*"
    ]
    for it in items:
        remaining = it["quantity"] - it["shipped_qty"]
        if show_stock:
            stock = get_stock(it["nomenclature_id"])
            to_produce = max(0, remaining - stock)
            stock_icon = "✅" if stock >= remaining else "⚠️"
            lines.append(
                f"• *{it['name']}*\n"
                f"  Заказано: {it['quantity']:,.1f} {it['unit']} | Отгружено: {it['shipped_qty']:,.1f}\n"
                f"  {stock_icon} На складе: {stock:,.1f} | 🔧 Произвести: {to_produce:,.1f}"
            )
        else:
            lines.append(f"• *{it['name']}*: {it['quantity']:,.1f} {it['unit']} (отгружено: {it['shipped_qty']:,.1f})")
    return "\n".join(lines)

def active_orders_list():
    conn = get_db()
    orders = conn.execute(
        "SELECT o.*,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id WHERE o.status != 'shipped' ORDER BY o.created_at DESC"
    ).fetchall()
    conn.close()
    if not orders:
        return "📦 Активных заказов нет.", []
    lines = ["📦 *Активные заказы:*\n"]
    for i, o in enumerate(orders, 1):
        status = ORDER_STATUS_LABELS.get(o["status"], o["status"])
        date = f"до {o['desired_date']}" if o["desired_date"] else ""
        lines.append(f"{i}. №{order_number(o['id'])} | *{o['cp_name'] or '—'}* | {status} {date}")
    lines.append("\nВведите номер для просмотра или /cancel:")
    return "\n".join(lines), list(orders)

@bot.message_handler(func=lambda m: m.text == "📦 Заказы")
def show_orders(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    text, orders = active_orders_list()
    if not orders:
        bot.send_message(message.from_user.id, text); return
    user_states[message.from_user.id] = "order_list"
    user_data[message.from_user.id]   = {"orders": [dict(o) for o in orders]}
    bot.send_message(message.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "order_list")
def order_list_pick(message):
    user = get_user(message.from_user.id)
    if not user: return
    orders = user_data.get(message.from_user.id, {}).get("orders", [])
    try:
        idx = int(message.text.strip()) - 1
        if idx < 0 or idx >= len(orders): raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите номер из списка."); return
    order = orders[idx]; oid = order["id"]
    cancel_state(message.from_user.id)
    detail = format_order(oid)
    role = user["role"]
    status = order["status"]
    hint = ""
    if role in ("admin","superadmin"):
        hint = "\n\nКоманды:\n/order_ship " + str(oid) + " — оформить отгрузку"
        if status == "shipped":
            hint = "\n\n✅ Заказ полностью отгружен."
    elif role == "manager":
        next_statuses = {"new":"принять","accepted":"в_работу","in_progress":"готов","ready":"готов"}
        if status in next_statuses:
            cmd = next_statuses[status]
            hint = f"\n\nСменить статус: `/order_status {oid} {cmd}`"
    bot.send_message(message.from_user.id, detail + hint, parse_mode="Markdown")

# ─── Смена статуса заказа (менеджер) ─────────────────────────────────────────

STATUS_TRANSITIONS = {
    "принять":   ("new",        "accepted"),
    "в_работу":  ("accepted",   "in_progress"),
    "готов":     ("in_progress","ready"),
}

@bot.message_handler(commands=["order_status"])
def order_status_change(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("manager","admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 3:
        bot.send_message(message.from_user.id,
            "Использование: `/order_status [id] [принять|в_работу|готов]`",
            parse_mode="Markdown"); return
    try:
        oid = int(parts[1])
    except ValueError:
        bot.send_message(message.from_user.id, "❌ Неверный ID заказа."); return
    cmd = parts[2].lower()
    if cmd not in STATUS_TRANSITIONS:
        bot.send_message(message.from_user.id, "❌ Команда: принять | в_работу | готов"); return
    required_from, new_status = STATUS_TRANSITIONS[cmd]
    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not order:
        bot.send_message(message.from_user.id, "❌ Заказ не найден."); conn.close(); return
    if order["status"] != required_from:
        bot.send_message(message.from_user.id,
            f"❌ Текущий статус: {ORDER_STATUS_LABELS.get(order['status'],order['status'])}\n"
            f"Невозможно выполнить команду."); conn.close(); return
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
    conn.commit(); conn.close()
    new_label = ORDER_STATUS_LABELS[new_status]
    bot.send_message(message.from_user.id,
        f"✅ Заказ №{order_number(oid)} → {new_label}", parse_mode="Markdown")
    # Уведомить создателя
    conn = get_db()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.close()
    if creator:
        try:
            bot.send_message(creator["telegram_id"],
                f"📦 Заказ №*{order_number(oid)}* изменил статус:\n{new_label}",
                parse_mode="Markdown")
        except Exception:
            pass

# ─── Создание заказа (admin) ──────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "➕ Новый заказ")
def new_order_start(message):
    show_new_order_menu(message)

def show_new_order_menu(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"): return
    text, cps = cp_list_text()
    if not cps:
        bot.send_message(message.from_user.id,
            "❌ Нет контрагентов. Добавьте через `/cp_add`", parse_mode="Markdown"); return
    user_states[message.from_user.id] = "order_new_cp"
    user_data[message.from_user.id]   = {"cps": [dict(cp) for cp in cps], "items": []}
    bot.send_message(message.from_user.id,
        text + "\n\nВведите номер контрагента или /cancel:", parse_mode="Markdown")

@bot.message_handler(commands=["new_order"])
def new_order_cmd(message):
    show_new_order_menu(message)

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "order_new_cp")
def order_new_cp(message):
    cps = user_data.get(message.from_user.id, {}).get("cps", [])
    try:
        idx = int(message.text.strip()) - 1
        if idx < 0 or idx >= len(cps): raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите номер из списка."); return
    user_data[message.from_user.id]["cp_id"] = cps[idx]["id"]
    user_data[message.from_user.id]["cp_name"] = cps[idx]["name"]
    user_states[message.from_user.id] = "order_new_date"
    bot.send_message(message.from_user.id,
        f"✅ Контрагент: *{cps[idx]['name']}*\n\n"
        f"Введите желаемую дату готовности (ДД.ММ.ГГГГ)\nили напишите *пропустить*:",
        parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "order_new_date")
def order_new_date(message):
    text = message.text.strip()
    if text.lower() in ("пропустить","skip","-"):
        user_data[message.from_user.id]["desired_date"] = None
    else:
        user_data[message.from_user.id]["desired_date"] = text
    user_states[message.from_user.id] = "order_new_items"
    show_order_item_picker(message.from_user.id)

def show_order_item_picker(tid):
    d = user_data.get(tid, {})
    text, _ = nom_list_text()
    items_so_far = d.get("items", [])
    if items_so_far:
        added = "\n*Добавлено:*\n" + "\n".join(f"• {it['name']} — {it['qty']:,.1f} {it['unit']}" for it in items_so_far)
    else:
        added = ""
    bot.send_message(tid,
        text + added + "\n\nВведите номер позиции или /done для завершения:",
        parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "order_new_items")
def order_new_item_pick(message):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall()
    conn.close()
    try:
        idx = int(message.text.strip()) - 1
        if idx < 0 or idx >= len(items): raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите номер позиции или /done."); return
    nom = items[idx]
    user_data[message.from_user.id]["current_nom"] = {"id": nom["id"], "name": nom["name"], "unit": nom["unit"]}
    user_states[message.from_user.id] = "order_new_qty"
    bot.send_message(message.from_user.id,
        f"Введите количество для *{nom['name']}* ({nom['unit']}):",
        parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "order_new_qty")
def order_new_qty(message):
    try:
        qty = float(message.text.strip().replace(",","."))
        if qty <= 0: raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите число больше нуля."); return
    d = user_data[message.from_user.id]
    nom = d["current_nom"]
    d["items"].append({"nom_id": nom["id"], "name": nom["name"], "unit": nom["unit"], "qty": qty})
    d.pop("current_nom", None)
    user_states[message.from_user.id] = "order_new_items"
    bot.send_message(message.from_user.id,
        f"✅ Добавлено: *{nom['name']}* — {qty:,.1f} {nom['unit']}", parse_mode="Markdown")
    show_order_item_picker(message.from_user.id)

@bot.message_handler(commands=["done"])
def order_done(message):
    if user_states.get(message.from_user.id) not in ("order_new_items",):
        bot.send_message(message.from_user.id, "Нечего завершать."); return
    user = get_user(message.from_user.id)
    d = user_data.get(message.from_user.id, {})
    if not d.get("items"):
        bot.send_message(message.from_user.id, "❌ Добавьте хотя бы одну позицию."); return
    now = datetime.now()
    conn = get_db()
    conn.execute(
        "INSERT INTO orders (counterparty_id,created_by,created_at,desired_date,status) VALUES (?,?,?,?,'new')",
        (d["cp_id"], user["id"], now, d.get("desired_date"))
    )
    oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]:
        conn.execute(
            "INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)",
            (oid, it["nom_id"], it["qty"])
        )
    conn.commit(); conn.close()
    cancel_state(message.from_user.id)
    bot.send_message(message.from_user.id,
        f"✅ Заказ №*{order_number(oid)}* создан!\n"
        f"Контрагент: *{d['cp_name']}*\n"
        f"Позиций: {len(d['items'])}",
        parse_mode="Markdown")
    send_menu(message.from_user.id, user["role"], user["name"])
    # Уведомить менеджера
    lines = [f"📦 *Новый заказ №{order_number(oid)}*\n",
             f"👥 Контрагент: *{d['cp_name']}*",
             f"⏰ Готовность: {d.get('desired_date') or '—'}\n",
             "*Позиции:*"]
    for it in d["items"]:
        lines.append(f"• {it['name']} — {it['qty']:,.1f} {it['unit']}")
    lines.append(f"\nДля принятия: `/order_status {oid} принять`")
    notify_role("manager", "\n".join(lines))

# ─── Отгрузка (admin) ─────────────────────────────────────────────────────────

@bot.message_handler(commands=["order_ship"])
def order_ship_start(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.from_user.id,
            "Использование: `/order_ship [id_заказа]`", parse_mode="Markdown"); return
    try:
        oid = int(parts[1])
    except ValueError:
        bot.send_message(message.from_user.id, "❌ Неверный ID."); return
    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not order:
        bot.send_message(message.from_user.id, "❌ Заказ не найден."); conn.close(); return
    items = conn.execute(
        "SELECT oi.*,n.name,n.unit FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE oi.order_id=? AND oi.quantity>oi.shipped_qty",
        (oid,)
    ).fetchall(); conn.close()
    if not items:
        bot.send_message(message.from_user.id, "✅ Все позиции уже отгружены."); return
    user_states[message.from_user.id] = f"ship_{oid}_0"
    user_data[message.from_user.id]   = {
        "order_id": oid,
        "items":    [dict(it) for it in items],
        "ship_qtys": []
    }
    it = items[0]
    remaining = it["quantity"] - it["shipped_qty"]
    bot.send_message(message.from_user.id,
        f"📦 *Отгрузка заказа №{order_number(oid)}*\n\n"
        f"Позиция 1/{len(items)}: *{it['name']}*\n"
        f"Остаток к отгрузке: {remaining:,.1f} {it['unit']}\n\n"
        f"Введите количество для отгрузки (или 0 чтобы пропустить):",
        parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id,"").startswith("ship_"))
def ship_qty_input(message):
    state = user_states.get(message.from_user.id,"")
    parts = state.split("_")
    oid  = int(parts[1]); idx = int(parts[2])
    d    = user_data.get(message.from_user.id, {})
    items = d.get("items", [])
    try:
        qty = float(message.text.strip().replace(",","."))
        if qty < 0: raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите число (0 чтобы пропустить)."); return
    it = items[idx]
    remaining = it["quantity"] - it["shipped_qty"]
    qty = min(qty, remaining)
    d["ship_qtys"].append({"order_item_id": it["id"], "qty": qty, "name": it["name"], "unit": it["unit"]})
    next_idx = idx + 1
    if next_idx < len(items):
        user_states[message.from_user.id] = f"ship_{oid}_{next_idx}"
        nit = items[next_idx]
        nr = nit["quantity"] - nit["shipped_qty"]
        bot.send_message(message.from_user.id,
            f"Позиция {next_idx+1}/{len(items)}: *{nit['name']}*\n"
            f"Остаток: {nr:,.1f} {nit['unit']}\n\n"
            f"Введите количество для отгрузки:",
            parse_mode="Markdown")
    else:
        # Все позиции заполнены — создаём отгрузку
        user = get_user(message.from_user.id)
        now  = datetime.now()
        conn = get_db()
        conn.execute("INSERT INTO shipments (order_id,created_by,created_at,status) VALUES (?,?,?,'pending')",
                     (oid, user["id"], now))
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for sq in d["ship_qtys"]:
            if sq["qty"] > 0:
                conn.execute("INSERT INTO shipment_items (shipment_id,order_item_id,quantity) VALUES (?,?,?)",
                             (sid, sq["order_item_id"], sq["qty"]))
        conn.execute("UPDATE orders SET status='shipping' WHERE id=?", (oid,))
        conn.commit(); conn.close()
        cancel_state(message.from_user.id)
        lines = [f"✅ Отгрузка №{sid} по заказу №{order_number(oid)} создана!\n",
                 "*Позиции:*"]
        for sq in d["ship_qtys"]:
            if sq["qty"] > 0:
                lines.append(f"• {sq['name']}: {sq['qty']:,.1f} {sq['unit']}")
        lines.append(f"\n⏳ Ожидается подтверждение Начальника Цеха.")
        bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")
        send_menu(message.from_user.id, user["role"], user["name"])
        # Уведомляем менеджера
        msg = "\n".join(lines[:len(lines)-1])
        msg += f"\n\nДля подтверждения нажмите *✅ Подтвердить отгрузку*"
        notify_role("manager", msg)

# ─── Подтверждение отгрузки (менеджер) ───────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "✅ Подтвердить отгрузку")
def confirm_shipment_list(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("manager","admin","superadmin"): return
    conn = get_db()
    ships = conn.execute(
        "SELECT s.*,o.id as oid FROM shipments s JOIN orders o ON o.id=s.order_id WHERE s.status='pending'"
    ).fetchall(); conn.close()
    if not ships:
        bot.send_message(message.from_user.id, "📦 Нет отгрузок, ожидающих подтверждения."); return
    lines = ["📦 *Отгрузки для подтверждения:*\n"]
    for i, s in enumerate(ships, 1):
        lines.append(f"{i}. Отгрузка №{s['id']} по заказу №{order_number(s['oid'])}")
    lines.append("\nВведите номер для подтверждения или /cancel:")
    user_states[message.from_user.id] = "confirm_ship"
    user_data[message.from_user.id]   = {"ships": [dict(s) for s in ships]}
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "confirm_ship")
def confirm_ship_pick(message):
    ships = user_data.get(message.from_user.id, {}).get("ships", [])
    try:
        idx = int(message.text.strip()) - 1
        if idx < 0 or idx >= len(ships): raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите номер из списка."); return
    ship = ships[idx]; sid = ship["id"]; oid = ship["oid"]
    user = get_user(message.from_user.id)
    now  = datetime.now()
    conn = get_db()
    # Получаем позиции отгрузки
    sitems = conn.execute(
        "SELECT si.*,n.name,n.unit,oi.nomenclature_id FROM shipment_items si "
        "JOIN order_items oi ON oi.id=si.order_item_id "
        "JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE si.shipment_id=?",
        (sid,)
    ).fetchall()
    # Обновляем отгруженные количества
    for si in sitems:
        conn.execute("UPDATE order_items SET shipped_qty=shipped_qty+? WHERE id=?",
                     (si["quantity"], si["order_item_id"]))
    # Подтверждаем отгрузку
    conn.execute("UPDATE shipments SET status='confirmed',confirmed_by=?,confirmed_at=? WHERE id=?",
                 (user["id"], now, sid))
    # Проверяем — весь ли заказ отгружен
    remaining = conn.execute(
        "SELECT SUM(quantity-shipped_qty) FROM order_items WHERE order_id=?", (oid,)
    ).fetchone()[0] or 0
    new_status = "shipped" if remaining <= 0 else "ready"
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
    conn.commit()
    # Получаем создателя заказа
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.close()
    cancel_state(message.from_user.id)
    result = "полностью ✅" if new_status == "shipped" else "частично ⚠️"
    bot.send_message(message.from_user.id,
        f"✅ Отгрузка №{sid} подтверждена!\nЗаказ №{order_number(oid)} отгружен {result}",
        parse_mode="Markdown")
    send_menu(message.from_user.id, user["role"], user["name"])
    # Уведомляем создателя
    if creator:
        lines = [f"📦 *Отгрузка по заказу №{order_number(oid)} подтверждена!*\n"]
        for si in sitems:
            lines.append(f"• {si['name']}: {si['quantity']:,.1f} {si['unit']}")
        lines.append(f"\nЗаказ: {ORDER_STATUS_LABELS[new_status]}")
        try:
            bot.send_message(creator["telegram_id"], "\n".join(lines), parse_mode="Markdown")
        except Exception:
            pass

# ─── Ежедневное производство (менеджер) ──────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "📊 Производство за день")
def production_start(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("manager","admin","superadmin"): return
    today = datetime.now().strftime("%d.%m.%Y")
    conn = get_db()
    existing = conn.execute(
        "SELECT dp.*,n.name,n.unit FROM daily_production dp JOIN nomenclature n ON n.id=dp.nomenclature_id WHERE dp.date=? AND dp.recorded_by=?",
        (today, user["id"])
    ).fetchall(); conn.close()
    user_states[message.from_user.id] = "prod_items"
    user_data[message.from_user.id]   = {"date": today, "user_id": user["id"], "existing": [dict(e) for e in existing]}
    show_production_picker(message.from_user.id)

def show_production_picker(tid):
    d = user_data.get(tid, {}); today = d.get("date","")
    text, _ = nom_list_text()
    existing = d.get("existing", [])
    if existing:
        added = "\n*Уже введено сегодня:*\n" + "\n".join(f"• {e['name']}: {e['quantity']:,.1f} {e['unit']}" for e in existing)
    else:
        added = ""
    bot.send_message(tid,
        f"📊 *Производство за {today}*\n{added}\n\n{text}\n\nВведите номер позиции или /done:",
        parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "prod_items")
def prod_item_pick(message):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall()
    conn.close()
    try:
        idx = int(message.text.strip()) - 1
        if idx < 0 or idx >= len(items): raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите номер позиции или /done."); return
    nom = items[idx]
    user_data[message.from_user.id]["current_nom"] = {"id": nom["id"], "name": nom["name"], "unit": nom["unit"]}
    user_states[message.from_user.id] = "prod_qty"
    bot.send_message(message.from_user.id,
        f"Введите количество *{nom['name']}* ({nom['unit']}) за сегодня:",
        parse_mode="Markdown")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "prod_qty")
def prod_qty_input(message):
    try:
        qty = float(message.text.strip().replace(",","."))
        if qty < 0: raise ValueError
    except ValueError:
        bot.send_message(message.from_user.id, "Введите число больше или равное нулю."); return
    user = get_user(message.from_user.id)
    d   = user_data[message.from_user.id]
    nom = d["current_nom"]; now = datetime.now(); today = d["date"]
    conn = get_db()
    # Если запись за сегодня уже есть — обновляем, иначе создаём
    existing = conn.execute(
        "SELECT * FROM daily_production WHERE date=? AND nomenclature_id=? AND recorded_by=?",
        (today, nom["id"], user["id"])
    ).fetchone()
    if existing:
        conn.execute("UPDATE daily_production SET quantity=quantity+?,recorded_at=? WHERE id=?",
                     (qty, now, existing["id"]))
    else:
        conn.execute("INSERT INTO daily_production (date,nomenclature_id,quantity,recorded_by,recorded_at) VALUES (?,?,?,?,?)",
                     (today, nom["id"], qty, user["id"], now))
    conn.commit()
    # Обновляем список existing
    updated = conn.execute(
        "SELECT dp.*,n.name,n.unit FROM daily_production dp JOIN nomenclature n ON n.id=dp.nomenclature_id WHERE dp.date=? AND dp.recorded_by=?",
        (today, user["id"])
    ).fetchall(); conn.close()
    d["existing"] = [dict(e) for e in updated]
    d.pop("current_nom", None)
    user_states[message.from_user.id] = "prod_items"
    bot.send_message(message.from_user.id,
        f"✅ Записано: *{nom['name']}* — {qty:,.1f} {nom['unit']}", parse_mode="Markdown")
    show_production_picker(message.from_user.id)

@bot.message_handler(commands=["prod_done"])
def prod_done(message):
    if user_states.get(message.from_user.id) not in ("prod_items","prod_qty"):
        bot.send_message(message.from_user.id, "Нет активного ввода производства."); return
    user = get_user(message.from_user.id)
    d = user_data.get(message.from_user.id, {})
    existing = d.get("existing", [])
    cancel_state(message.from_user.id)
    if not existing:
        bot.send_message(message.from_user.id, "Данные не были введены."); return
    lines = [f"✅ *Производство за {d.get('date','')} сохранено:*\n"]
    for e in existing:
        lines.append(f"• {e['name']}: {e['quantity']:,.1f} {e['unit']}")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")
    send_menu(message.from_user.id, user["role"], user["name"])
    notify_supervisors("\n".join(lines))

# Переопределяем /done чтобы работал и для производства и для заказов
@bot.message_handler(commands=["done"])
def universal_done(message):
    state = user_states.get(message.from_user.id, "")
    if state in ("prod_items","prod_qty"):
        prod_done(message)
    elif state == "order_new_items":
        order_done(message)
    else:
        bot.send_message(message.from_user.id, "Нечего завершать.")

# ─── Просмотр итогов производства ────────────────────────────────────────────

@bot.message_handler(commands=["production"])
def show_production(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    now = datetime.now()
    month_start = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall()
    lines = [f"📊 *Производство — {MONTHS_RU[now.month]} {now.year}:*\n"]
    for it in items:
        total = conn.execute(
            "SELECT COALESCE(SUM(quantity),0) FROM daily_production WHERE nomenclature_id=? AND date>=?",
            (it["id"], month_start.strftime("%d.%m.%Y"))
        ).fetchone()[0]
        if total > 0:
            lines.append(f"• *{it['name']}*: {total:,.1f} {it['unit']}")
    conn.close()
    if len(lines) == 1:
        lines.append("Нет данных за текущий месяц.")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

# ─── Фоновые напоминания ──────────────────────────────────────────────────────

def reminder_loop():
    while True:
        time.sleep(1800)
        try:
            conn = get_db(); now = datetime.now()
            rows = conn.execute(
                "SELECT t.*,u.name,u.telegram_id FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active'"
            ).fetchall()
            for rec in rows:
                check_in_dt = datetime.fromisoformat(rec["check_in"])
                count   = rec["reminder_count"]
                last    = datetime.fromisoformat(rec["last_reminder"]) if rec["last_reminder"] else None
                elapsed = (now - check_in_dt).total_seconds()
                if count == 0 and elapsed >= 3600:
                    bot.send_message(rec["telegram_id"],
                        "⏰ *Напоминание:* ты ещё не отметил уход.\nНажми '🚪 Ушёл с работы'",
                        parse_mode="Markdown")
                    conn.execute("UPDATE time_records SET reminder_count=1,last_reminder=? WHERE id=?", (now,rec["id"]))
                    conn.commit()
                elif count == 1 and last and (now-last).total_seconds() >= 3600:
                    bot.send_message(rec["telegram_id"],
                        "⏰ *Второе напоминание:* отметь уход!\nНажми '🚪 Ушёл с работы'",
                        parse_mode="Markdown")
                    conn.execute("UPDATE time_records SET reminder_count=2,last_reminder=? WHERE id=?", (now,rec["id"]))
                    conn.commit()
                elif count >= 2 and last and (now-last).total_seconds() >= 3600:
                    conn.execute("UPDATE time_records SET status='no_checkout',check_out=? WHERE id=?", (now,rec["id"]))
                    conn.commit()
                    bot.send_message(rec["telegram_id"],
                        "⚠️ Смена закрыта автоматически. Руководство уведомлено.")
                    notify_supervisors(
                        f"⚠️ *{rec['name']}* не отметил уход.\n"
                        f"Смена с {check_in_dt.strftime('%H:%M')}. Отмечено как _пропущенный выход_.")
            conn.close()
        except Exception as e:
            print(f"[reminder error] {e}")

threading.Thread(target=reminder_loop, daemon=True).start()

# ─── Запуск ───────────────────────────────────────────────────────────────────
print("Bot started!")
bot.infinity_polling()
