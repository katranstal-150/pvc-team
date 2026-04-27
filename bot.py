import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from telebot import TeleBot
from telebot.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
bot = TeleBot(BOT_TOKEN)
DB_PATH = "/app/data/timetrack.db"

# ═══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_LABELS = {
    "worker":     "👷 Рабочий",
    "manager":    "👔 Начальник Цеха",
    "admin":      "⚙️ Администратор",
    "superadmin": "👑 Главный Админ",
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
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",
    5:"Май",6:"Июнь",7:"Июль",8:"Август",
    9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}

# Номенклатура — загружается при первом старте
INITIAL_NOMENCLATURE = [
    ("Гарпун Вид 1 (уз) белый",    "м",  "Намотка 200м"),
    ("Гарпун Вид 1 (уз) чёрный",   "м",  "Намотка 200м"),
    ("Гарпун Вид 2 (шир) белый",   "м",  "Намотка 200м"),
    ("Гарпун Вид 2 (шир) чёрный",  "м",  "Намотка 200м"),
    ("Вставка Т «Элит»",           "м",  "Первичное сырьё, намотка 50/150м"),
    ("Вставка Т чёрная",           "м",  "Намотка 50/150м"),
    ("Вставка Т",                  "м",  "Намотка 50/150м"),
    ("Вставка Уголок белая",       "м",  "Намотка 50/100м"),
    ("Вставка Уголок чёрная",      "м",  "Намотка 50/100м"),
    ("Багет ПВХ стеновой 150 г/м", "м",  "Вид 1, для пистолета, 2м/2.5м"),
    ("Багет ПВХ стеновой 140 г/м", "м",  "Вид 2, 2м/2.5м"),
    ("Платформа унив. 60-110",     "шт", "Серая, 50шт/короб, 150шт/мешок"),
    ("Платформа 90",               "шт", "Серая, 50шт/короб, 150шт/мешок"),
]

# Предустановленные пользователи (восстанавливаются после пересоздания БД)
INITIAL_USERS = [
    (915402089, "Katran 150", "superadmin", 0),
]

# Состояния диалогов
user_states = {}
user_data   = {}

# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            daily_rate REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS time_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            check_in TIMESTAMP NOT NULL,
            check_out TIMESTAMP,
            status TEXT DEFAULT "active",
            reminder_count INTEGER DEFAULT 0,
            last_reminder TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            status TEXT DEFAULT "new",
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS nomenclature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            notes TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS counterparties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT, email TEXT, address TEXT, notes TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            counterparty_id INTEGER,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            desired_date TEXT,
            status TEXT DEFAULT "new",
            FOREIGN KEY (counterparty_id) REFERENCES counterparties(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            nomenclature_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            shipped_qty REAL DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (nomenclature_id) REFERENCES nomenclature(id)
        );
        CREATE TABLE IF NOT EXISTS shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            confirmed_by INTEGER,
            confirmed_at TIMESTAMP,
            status TEXT DEFAULT "pending",
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
        CREATE TABLE IF NOT EXISTS shipment_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shipment_id INTEGER NOT NULL,
            order_item_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            FOREIGN KEY (shipment_id) REFERENCES shipments(id),
            FOREIGN KEY (order_item_id) REFERENCES order_items(id)
        );
        CREATE TABLE IF NOT EXISTS daily_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            nomenclature_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            recorded_by INTEGER NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            FOREIGN KEY (nomenclature_id) REFERENCES nomenclature(id),
            FOREIGN KEY (recorded_by) REFERENCES users(id)
        );
    ''')

    # Предустановленная номенклатура
    if conn.execute("SELECT COUNT(*) FROM nomenclature").fetchone()[0] == 0:
        conn.executemany(
            "INSERT INTO nomenclature (name,unit,notes) VALUES (?,?,?)",
            INITIAL_NOMENCLATURE
        )

    # Предустановленные пользователи
    for tid, name, role, rate in INITIAL_USERS:
        existing = conn.execute("SELECT id FROM users WHERE telegram_id=?", (tid,)).fetchone()
        if existing:
            conn.execute("UPDATE users SET role=? WHERE telegram_id=?", (role, tid))
        else:
            conn.execute("INSERT INTO users (telegram_id,name,role,daily_rate) VALUES (?,?,?,?)",
                         (tid, name, role, rate))

    conn.commit()

    # Миграция
    def cols(table):
        try:
            return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        except:
            return []

    if "daily_rate" not in cols("users"):
        try: conn.execute("ALTER TABLE users ADD COLUMN daily_rate REAL DEFAULT 0")
        except: pass
    if "status" not in cols("requests"):
        try: conn.execute("ALTER TABLE requests ADD COLUMN status TEXT DEFAULT 'new'")
        except: pass

    conn.commit(); conn.close()
    print("✅ БД инициализирована")

init_db()

# ═══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════════════════

def get_user(tid):
    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    conn.close(); return u

def notify_supervisors(text):
    conn = get_db()
    rows = conn.execute(
        "SELECT telegram_id FROM users WHERE role IN ('manager','admin','superadmin')"
    ).fetchall(); conn.close()
    for r in rows:
        try: bot.send_message(r["telegram_id"], text, parse_mode="Markdown")
        except: pass

def notify_role(role, text):
    conn = get_db()
    rows = conn.execute("SELECT telegram_id FROM users WHERE role=?", (role,)).fetchall()
    conn.close()
    for r in rows:
        try: bot.send_message(r["telegram_id"], text, parse_mode="Markdown")
        except: pass

def get_stock(nom_id):
    conn = get_db()
    prod = conn.execute("SELECT COALESCE(SUM(quantity),0) FROM daily_production WHERE nomenclature_id=?", (nom_id,)).fetchone()[0]
    ship = conn.execute(
        "SELECT COALESCE(SUM(si.quantity),0) FROM shipment_items si "
        "JOIN shipments s ON s.id=si.shipment_id "
        "JOIN order_items oi ON oi.id=si.order_item_id "
        "WHERE oi.nomenclature_id=? AND s.status='confirmed'", (nom_id,)
    ).fetchone()[0]
    conn.close(); return prod - ship

def onum(oid): return f"{datetime.now().year}-{oid:03d}"

def cancel_state(tid):
    user_states.pop(tid, None); user_data.pop(tid, None)

# ═══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════════════════════

def ik(*rows):
    """Быстрый конструктор инлайн-клавиатуры"""
    kb = InlineKeyboardMarkup()
    for row in rows:
        kb.row(*[InlineKeyboardButton(t, callback_data=cd) for t, cd in row])
    return kb

def ans(call, text="", alert=False):
    bot.answer_callback_query(call.id, text, show_alert=alert)

def edit(call, text, kb=None):
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                              reply_markup=kb, parse_mode="Markdown")
    except: pass

# Нижние клавиатуры (постоянное меню)
def worker_rk():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("✅ Пришёл на работу"), KeyboardButton("🚪 Ушёл с работы"))
    kb.row(KeyboardButton("📈 Статистика"), KeyboardButton("📋 Заявки и справка"))
    return kb

def supervisor_rk():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("👥 Сотрудники"), KeyboardButton("📦 Производство"))
    return kb

def send_menu(tid, role, name):
    emoji = {"worker":"👷","manager":"👔","admin":"⚙️","superadmin":"👑"}
    if role == "worker":
        bot.send_message(tid, f"{emoji[role]} Привет, {name}!", reply_markup=worker_rk())
    else:
        bot.send_message(tid, f"{emoji.get(role,'')} Привет, {name}!", reply_markup=supervisor_rk())

# ─── Инлайн-меню: Раздел Сотрудники ──────────────────────────────────────────

def staff_menu_kb(role):
    rows = [
        [("📊 Отчёт за неделю", "rp:week"), ("📊 Отчёт за месяц", "rp:month")],
        [("👷 Кто сейчас на смене", "shift:now")],
    ]
    if role in ("admin", "superadmin"):
        rows.append([("👥 Список сотрудников", "usr:list")])
    rows.append([("❓ Справка", "help")])
    return ik(*rows)

# ─── Инлайн-меню: Раздел Производство ────────────────────────────────────────

def prod_menu_kb(role):
    rows = [
        [("📦 Заказы", "ord:list")],
    ]
    if role in ("admin", "superadmin"):
        rows.append([("➕ Новый заказ", "ord:new"), ("👥 Контрагенты", "cp:list")])
        rows.append([("📋 Номенклатура", "nm:list"), ("🏭 Склад", "wh")])
    else:
        rows.append([("📊 Производство за день", "pd:start"), ("🏭 Склад", "wh")])
        rows.append([("✅ Подтвердить отгрузку", "cs:list")])
    return ik(*rows)

# ─── Инлайн-меню: Заявки и справка ───────────────────────────────────────────

def req_kb():
    return ik(
        [("📅 Не выйду на работу", "rq:abs")],
        [("🔧 Неисправность оборудования", "rq:brk")],
        [("📦 Заявка на МТС", "rq:mts")],
        [("❓ Справка", "help")],
    )

# ═══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ /start /myid /setup /cancel
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def cmd_start(message):
    cancel_state(message.from_user.id)
    user = get_user(message.from_user.id)
    if user:
        send_menu(message.from_user.id, user["role"], user["name"])
    else:
        bot.send_message(message.from_user.id,
            "👋 Вы не зарегистрированы.\n\nСообщите администратору ваш ID:\n"
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

@bot.message_handler(commands=["setup"])
def cmd_setup(message):
    conn = get_db()
    if conn.execute("SELECT id FROM users WHERE role='superadmin'").fetchone():
        conn.close(); bot.send_message(message.from_user.id, "⛔ Главный Админ уже зарегистрирован."); return
    first = message.from_user.first_name or ""; last = message.from_user.last_name or ""
    name  = f"{first} {last}".strip() or f"SuperAdmin_{message.from_user.id}"
    if conn.execute("SELECT id FROM users WHERE telegram_id=?", (message.from_user.id,)).fetchone():
        conn.execute("UPDATE users SET role='superadmin',name=? WHERE telegram_id=?", (name, message.from_user.id))
    else:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,'superadmin')", (message.from_user.id, name))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Вы — *Главный Админ*!\nИмя: *{name}*\nID: `{message.from_user.id}`\n\n"
        f"⚠️ /setup заблокирован.\nДобавляйте сотрудников: `/add [id] [имя] [роль]`",
        parse_mode="Markdown")
    send_menu(message.from_user.id, "superadmin", name)

# ═══════════════════════════════════════════════════════════════════════════════
# УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(commands=["add"])
def cmd_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 4:
        hint = "`worker`|`manager`|`admin`" if user["role"]=="superadmin" else "`worker`|`manager`"
        bot.send_message(message.from_user.id,
            f"📌 `/add [id] [имя фамилия] [роль]`\nРоли: {hint}\n"
            f"Пример: `/add 123456789 Иван Иванов worker`", parse_mode="Markdown"); return
    tid_str=parts[1]; role=parts[-1]; name=" ".join(parts[2:-1])
    allowed = SUPER_MANAGED if user["role"]=="superadmin" else ADMIN_MANAGED
    if role not in allowed:
        bot.send_message(message.from_user.id,
            f"❌ Нельзя добавить роль `{role}`.\nДоступные: {', '.join(f'`{r}`' for r in allowed)}",
            parse_mode="Markdown"); return
    try: tid = int(tid_str)
    except: bot.send_message(message.from_user.id, "❌ ID должен быть числом."); return
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (tid,name,role))
        conn.commit()
        bot.send_message(message.from_user.id, f"✅ Добавлен: *{name}* — {ROLE_LABELS[role]}", parse_mode="Markdown")
        try: bot.send_message(tid, f"✅ Вы зарегистрированы как *{name}*.\nНажмите /start", parse_mode="Markdown")
        except: pass
    except sqlite3.IntegrityError:
        bot.send_message(message.from_user.id, "⚠️ Пользователь уже есть в системе.")
    finally: conn.close()

@bot.message_handler(commands=["remove"])
def cmd_remove(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.from_user.id, "Использование: `/remove [id]`", parse_mode="Markdown"); return
    try: tid = int(parts[1])
    except: bot.send_message(message.from_user.id, "❌ ID должен быть числом."); return
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not target: bot.send_message(message.from_user.id, "❌ Не найден."); conn.close(); return
    allowed = SUPER_MANAGED if user["role"]=="superadmin" else ADMIN_MANAGED
    if target["role"] not in allowed:
        bot.send_message(message.from_user.id, f"❌ Нельзя удалить {ROLE_LABELS.get(target['role'],'')}."); conn.close(); return
    conn.execute("DELETE FROM users WHERE telegram_id=?", (tid,)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ *{target['name']}* удалён.", parse_mode="Markdown")

@bot.message_handler(commands=["setrate"])
def cmd_setrate(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "superadmin":
        bot.send_message(message.from_user.id, "⛔ Только Главный Админ."); return
    parts = message.text.split()
    if len(parts) < 3:
        bot.send_message(message.from_user.id, "📌 `/setrate [id] [сумма]`\nПример: `/setrate 123456789 2500`", parse_mode="Markdown"); return
    try: tid=int(parts[1]); rate=float(parts[2])
    except: bot.send_message(message.from_user.id, "❌ Неверный формат."); return
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not target or target["role"] != "worker":
        bot.send_message(message.from_user.id, "❌ Рабочий не найден."); conn.close(); return
    conn.execute("UPDATE users SET daily_rate=? WHERE telegram_id=?", (rate,tid)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Оклад установлен!\n👷 *{target['name']}*\n"
        f"💰 {rate:,.0f} ₽/день | {rate/8:,.2f} ₽/ч", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# НИЖНИЕ КНОПКИ — ПРИХОД/УХОД И СЕКЦИИ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "✅ Пришёл на работу")
def check_in(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db()
    active = conn.execute("SELECT * FROM time_records WHERE user_id=? AND status='active'", (user["id"],)).fetchone()
    if active:
        t = datetime.fromisoformat(active["check_in"]).strftime("%H:%M")
        bot.send_message(message.from_user.id, f"⚠️ Ты уже на работе с {t}."); conn.close(); return
    now = datetime.now()
    conn.execute("INSERT INTO time_records (user_id,check_in) VALUES (?,?)", (user["id"],now)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ Приход зафиксирован в *{now.strftime('%H:%M')}*", parse_mode="Markdown")
    notify_supervisors(f"✅ *{user['name']}* пришёл на работу в {now.strftime('%H:%M')}")

@bot.message_handler(func=lambda m: m.text == "🚪 Ушёл с работы")
def check_out(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db()
    active = conn.execute("SELECT * FROM time_records WHERE user_id=? AND status='active'", (user["id"],)).fetchone()
    if not active: bot.send_message(message.from_user.id, "⚠️ Ты не отмечен как на работе!"); conn.close(); return
    now = datetime.now()
    hrs = (now - datetime.fromisoformat(active["check_in"])).total_seconds() / 3600
    conn.execute("UPDATE time_records SET check_out=?,status='closed' WHERE id=?", (now,active["id"])); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"👋 Уход в *{now.strftime('%H:%M')}* | Отработано: *{hrs:.1f} ч.*", parse_mode="Markdown")
    notify_supervisors(f"🚪 *{user['name']}* ушёл в {now.strftime('%H:%M')} ({hrs:.1f} ч.)")

@bot.message_handler(func=lambda m: m.text == "📈 Статистика")
def my_stats(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db(); now = datetime.now()
    def calc(since):
        recs = conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=? AND status='closed'", (user["id"],since)).fetchall()
        h = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in recs)
        d = len({datetime.fromisoformat(r["check_in"]).date() for r in recs})
        return d, h
    wd,wh = calc(now-timedelta(days=7))
    month_start = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    md,mh = calc(month_start)
    rate = user["daily_rate"] or 0; hourly = rate/8 if rate>0 else 0
    active = conn.execute("SELECT check_in FROM time_records WHERE user_id=? AND status='active'", (user["id"],)).fetchone(); conn.close()
    lines = [f"📈 *{user['name']}*\n"]
    if active:
        dt = datetime.fromisoformat(active["check_in"]); hrs = (now-dt).total_seconds()/3600
        lines.append(f"🟢 На смене с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)\n")
    wn = now.isocalendar()[1]; mn = MONTHS_RU[now.month]
    lines.append(f"📅 *Неделя №{wn}:* {wd} дн. | {wh:.1f} ч.")
    if rate>0: lines.append(f"💰 Заработок: *{wh*hourly:,.0f} ₽*")
    lines.append(f"\n📆 *{mn} {now.year}:* {md} дн. | {mh:.1f} ч.")
    if rate>0: lines.append(f"💰 Заработок: *{mh*hourly:,.0f} ₽*")
    if rate>0: lines.append(f"\n⏱ Ставка: *{rate:,.0f} ₽/день*")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "📋 Заявки и справка")
def req_menu(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    bot.send_message(message.from_user.id, "📋 *Заявки и справка:*", reply_markup=req_kb(), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "👥 Сотрудники")
def section_staff(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    bot.send_message(message.from_user.id, "👥 *Раздел: Сотрудники*",
                     reply_markup=staff_menu_kb(user["role"]), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "📦 Производство")
def section_prod(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES: return
    bot.send_message(message.from_user.id, "📦 *Раздел: Производство и заказы*",
                     reply_markup=prod_menu_kb(user["role"]), parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИК ИНЛАЙН-КНОПОК
# ═══════════════════════════════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    user = get_user(call.from_user.id)
    if not user:
        ans(call, "⛔ Вы не зарегистрированы.", True); return
    cd = call.data

    # ── Меню разделов ──────────────────────────────────────────────────────────
    if cd == "mn:staff":
        ans(call); edit(call, "👥 *Раздел: Сотрудники*", staff_menu_kb(user["role"])); return
    if cd == "mn:prod":
        ans(call); edit(call, "📦 *Раздел: Производство и заказы*", prod_menu_kb(user["role"])); return

    # ── Отчёты ─────────────────────────────────────────────────────────────────
    if cd == "rp:week":
        ans(call); edit(call, _gen_report(7))
        return
    if cd == "rp:month":
        ans(call); edit(call, _gen_report(0))
        return
    if cd == "shift:now":
        ans(call); edit(call, _who_on_shift())
        return

    # ── Список сотрудников ─────────────────────────────────────────────────────
    if cd == "usr:list":
        ans(call); edit(call, _staff_list(user["role"]))
        return

    # ── Справка ────────────────────────────────────────────────────────────────
    if cd == "help":
        ans(call); edit(call, _help_text(user["role"]))
        return

    # ── Заявки ─────────────────────────────────────────────────────────────────
    if cd == "rq:abs":
        ans(call); _start_request(call.from_user.id, "absence"); return
    if cd == "rq:brk":
        ans(call); _start_request(call.from_user.id, "breakdown"); return
    if cd == "rq:mts":
        ans(call); _start_request(call.from_user.id, "mts"); return

    # ── Заказы — список ────────────────────────────────────────────────────────
    if cd == "ord:list":
        ans(call); edit(call, *_orders_list_view(user["role"]))
        return

    # ── Заказ — просмотр ───────────────────────────────────────────────────────
    if cd.startswith("ord:v:"):
        oid = int(cd.split(":")[2]); ans(call)
        text, kb = _order_detail_view(oid, user["role"])
        edit(call, text, kb); return

    # ── Заказ — смена статуса ──────────────────────────────────────────────────
    if cd.startswith("ord:s:"):
        parts = cd.split(":"); oid=int(parts[2]); new_status=parts[3]
        ans(call); _change_order_status(call.from_user.id, oid, new_status, user)
        text, kb = _order_detail_view(oid, user["role"])
        edit(call, text, kb); return

    # ── Отгрузка — начало ──────────────────────────────────────────────────────
    if cd.startswith("ord:ship:"):
        oid = int(cd.split(":")[2]); ans(call)
        _start_shipment(call.from_user.id, oid); return

    # ── Новый заказ ────────────────────────────────────────────────────────────
    if cd == "ord:new":
        ans(call); _start_new_order(call.from_user.id); return

    # ── Контрагенты ────────────────────────────────────────────────────────────
    if cd == "cp:list":
        ans(call); edit(call, _cp_list_text()); return

    # ── Выбор контрагента в заказе ─────────────────────────────────────────────
    if cd.startswith("cp:sel:"):
        cp_id = int(cd.split(":")[2]); ans(call)
        _order_cp_selected(call.from_user.id, cp_id); return

    # ── Номенклатура ───────────────────────────────────────────────────────────
    if cd == "nm:list":
        ans(call); edit(call, _nom_list_text()); return

    # ── Выбор позиции в заказе ─────────────────────────────────────────────────
    if cd.startswith("ni:"):
        nom_id = int(cd.split(":")[1]); ans(call)
        _order_item_selected(call.from_user.id, nom_id); return

    # ── Склад ──────────────────────────────────────────────────────────────────
    if cd == "wh":
        ans(call); edit(call, _warehouse_text()); return

    # ── Производство за день ───────────────────────────────────────────────────
    if cd == "pd:start":
        ans(call); _start_production(call.from_user.id); return

    # ── Выбор позиции в производстве ──────────────────────────────────────────
    if cd.startswith("pd:"):
        nom_id = int(cd.split(":")[1]); ans(call)
        _prod_item_selected(call.from_user.id, nom_id); return

    # ── Подтвердить отгрузку ───────────────────────────────────────────────────
    if cd == "cs:list":
        ans(call); edit(call, *_pending_shipments_view()); return

    if cd.startswith("cs:ok:"):
        sid = int(cd.split(":")[2]); ans(call)
        _confirm_shipment(call.from_user.id, sid, user)
        edit(call, "✅ Отгрузка подтверждена!"); return

    if cd.startswith("cs:v:"):
        sid = int(cd.split(":")[2]); ans(call)
        text, kb = _shipment_detail_view(sid)
        edit(call, text, kb); return

    ans(call)

# ═══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИИ ФОРМИРОВАНИЯ КОНТЕНТА
# ═══════════════════════════════════════════════════════════════════════════════

def _gen_report(days):
    conn = get_db(); now = datetime.now()
    if days == 7:
        since = now - timedelta(days=7)
        wn = now.isocalendar()[1]
        label = f"неделю №{wn} ({since.strftime('%d.%m')}–{now.strftime('%d.%m')})"
    else:
        since = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        label = f"{MONTHS_RU[now.month]} {now.year}"
    workers = conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall()
    if not workers: conn.close(); return "📊 Нет рабочих."
    lines = [f"📊 *Отчёт за {label}*\n"]
    for w in workers:
        recs   = conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=?", (w["id"],since)).fetchall()
        closed = [r for r in recs if r["status"]=="closed"]
        no_mrk = [r for r in recs if r["status"]=="no_checkout"]
        th = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in closed)
        dw = len({datetime.fromisoformat(r["check_in"]).date() for r in closed})
        rate = w["daily_rate"] or 0; hourly = rate/8 if rate>0 else 0
        ov   = sum(max(0,(datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600-8) for r in closed)
        lines.append(f"👷 *{w['name']}*")
        lines.append(f"   Дней: {dw} | Часов: {th:.1f}" + (f" (перераб.: {ov:.1f})" if ov>0 else ""))
        if rate>0: lines.append(f"   💰 Заработок: *{th*hourly:,.0f} ₽*")
        if no_mrk: lines.append(f"   ⚠️ Не отметил уход: {len(no_mrk)} раз(а)")
        lines.append("")
    conn.close(); return "\n".join(lines)

def _who_on_shift():
    conn = get_db()
    rows = conn.execute(
        "SELECT u.name,t.check_in FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active' ORDER BY t.check_in"
    ).fetchall(); conn.close()
    if not rows: return "👷 Сейчас никого нет на смене."
    lines = ["👷 *Сейчас на смене:*\n"]
    for r in rows:
        dt = datetime.fromisoformat(r["check_in"]); hrs = (datetime.now()-dt).total_seconds()/3600
        lines.append(f"• *{r['name']}* — с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)")
    return "\n".join(lines)

def _staff_list(role):
    conn = get_db()
    if role == "superadmin":
        users = conn.execute("SELECT * FROM users ORDER BY role,name").fetchall()
    else:
        users = conn.execute("SELECT * FROM users WHERE role IN ('worker','manager') ORDER BY role,name").fetchall()
    conn.close()
    if not users: return "Список пуст."
    lines = ["👥 *Список сотрудников:*\n"]
    for u in users:
        lines.append(f"{ROLE_LABELS.get(u['role'],u['role'])}: *{u['name']}*")
        lines.append(f"`ID: {u['telegram_id']}`")
        if u["role"]=="worker" and (u["daily_rate"] or 0)>0:
            lines.append(f"💰 {u['daily_rate']:,.0f} ₽/день")
        lines.append("")
    return "\n".join(lines)

def _help_text(role):
    texts = {
        "worker": "📖 *Справка — Рабочий*\n\n✅/🚪 Отметь приход и уход\n📈 Статистика — часы и заработок\n📋 Заявки — неявка, неисправность, МТС\n\n⚠️ Не забывай отмечать уход!",
        "manager": "📖 *Справка — Начальник Цеха*\n\n*👥 Сотрудники:* отчёты, кто на смене\n*📦 Производство:*\n• Заказы — просмотр, смена статусов\n• Производство за день — ввод выпуска\n• Склад — наличие\n• Подтвердить отгрузку\n\n*Статусы заказа:*\n🆕 Новый → ✅ Принят → 🔧 В работе → 🏁 Готов",
        "admin": "📖 *Справка — Администратор*\n\n*👥 Сотрудники:* отчёты, список\n*📦 Производство:*\n• Заказы и создание новых\n• Контрагенты\n• Номенклатура\n• Склад\n\n*/add [id] [имя] [роль]*\n*/remove [id]*\n*/setrate [id] [сумма]*\n*/cp_add Назв|тел|email|адрес|прим*\n*/nom_add Назв|ед|прим*",
        "superadmin": "📖 *Справка — Главный Админ*\n\nПолный доступ.\n\n*/add [id] [имя] [роль]* — worker|manager|admin\n*/remove [id]*\n*/setrate [id] [сумма]*\n*/cp_add* / */nom_add*",
    }
    return texts.get(role, "Справка недоступна.")

def _nom_list_text():
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall(); conn.close()
    lines = ["📋 *Номенклатура:*\n"]
    for i, it in enumerate(items,1):
        note = f"\n   _{it['notes']}_" if it["notes"] else ""
        lines.append(f"{i}. *{it['name']}* ({it['unit']}){note}")
    lines.append("\n➕ Добавить: `/nom_add Название | ед | примечание`")
    return "\n".join(lines)

def _cp_list_text():
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps: return "👥 Контрагентов нет.\n\nДобавить: `/cp_add Название | тел | email | адрес | примечание`"
    lines = ["👥 *Контрагенты:*\n"]
    for i, cp in enumerate(cps,1):
        lines.append(f"{i}. *{cp['name']}*")
        if cp["phone"]: lines.append(f"   📞 {cp['phone']}")
        if cp["email"]: lines.append(f"   📧 {cp['email']}")
        if cp["address"]: lines.append(f"   📍 {cp['address']}")
        if cp["notes"]: lines.append(f"   📝 {cp['notes']}")
        lines.append("")
    lines.append("➕ Добавить: `/cp_add Название | тел | email | адрес | примечание`")
    return "\n".join(lines)

def _warehouse_text():
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall(); conn.close()
    lines = [f"🏭 *Склад — остаток на {datetime.now().strftime('%d.%m.%Y')}:*\n"]
    for it in items:
        s = get_stock(it["id"]); icon = "✅" if s>0 else ("⚠️" if s==0 else "🔴")
        lines.append(f"{icon} *{it['name']}*: {s:,.1f} {it['unit']}")
    return "\n".join(lines)

# ─── Заказы ───────────────────────────────────────────────────────────────────

def _orders_list_view(role):
    conn = get_db()
    if role == "manager":
        orders = conn.execute(
            "SELECT o.*,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id "
            "WHERE o.status!='shipped' ORDER BY o.created_at DESC"
        ).fetchall()
    else:
        orders = conn.execute(
            "SELECT o.*,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id "
            "ORDER BY o.created_at DESC LIMIT 20"
        ).fetchall()
    conn.close()
    if not orders: return "📦 Заказов нет.", None
    lines = ["📦 *Заказы:*\n"]
    kb_rows = []
    for o in orders:
        st = ORDER_STATUS_LABELS.get(o["status"],o["status"])
        date_info = f" | до {o['desired_date']}" if o["desired_date"] else ""
        lines.append(f"• №{onum(o['id'])} — *{o['cp_name'] or '—'}* | {st}{date_info}")
        kb_rows.append([(f"№{onum(o['id'])} {o['cp_name'] or '—'}", f"ord:v:{o['id']}")])
    kb_rows.append([("🔙 Назад", "mn:prod")])
    return "\n".join(lines), ik(*kb_rows)

def _order_detail_view(oid, role):
    conn = get_db()
    o = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o: conn.close(); return "Заказ не найден.", None
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (o["counterparty_id"],)).fetchone()
    cr = conn.execute("SELECT name FROM users WHERE id=?", (o["created_by"],)).fetchone()
    items = conn.execute(
        "SELECT oi.*,n.name,n.unit FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE oi.order_id=?",
        (oid,)
    ).fetchall(); conn.close()
    status = ORDER_STATUS_LABELS.get(o["status"],o["status"])
    lines = [
        f"📦 *Заказ №{onum(oid)}*",
        f"👥 {cp['name'] if cp else '—'}",
        f"📅 Создан: {datetime.fromisoformat(o['created_at']).strftime('%d.%m.%Y %H:%M')}",
        f"⏰ Готовность: {o['desired_date'] or '—'}",
        f"👤 Создал: {cr['name'] if cr else '—'}",
        f"📊 Статус: {status}\n",
        "*Позиции:*"
    ]
    for it in items:
        rem = it["quantity"] - it["shipped_qty"]
        stock = get_stock(it["nomenclature_id"])
        to_make = max(0, rem - stock)
        icon = "✅" if stock >= rem else "⚠️"
        lines.append(
            f"• *{it['name']}*\n"
            f"  Заказано: {it['quantity']:,.1f} | Отгружено: {it['shipped_qty']:,.1f} {it['unit']}\n"
            f"  {icon} Склад: {stock:,.1f} | 🔧 Произвести: {to_make:,.1f}"
        )
    # Кнопки действий
    kb_rows = []
    if role == "manager":
        STATUS_BTN = {
            "new":        ("✅ Принять",   f"ord:s:{oid}:accepted"),
            "accepted":   ("🔧 В работу",  f"ord:s:{oid}:in_progress"),
            "in_progress":("🏁 Готово",    f"ord:s:{oid}:ready"),
        }
        if o["status"] in STATUS_BTN:
            kb_rows.append([STATUS_BTN[o["status"]]])
    if role in ("admin","superadmin") and o["status"] in ("ready","in_progress","accepted"):
        kb_rows.append([("📦 Оформить отгрузку", f"ord:ship:{oid}")])
    kb_rows.append([("🔙 К списку заказов", "ord:list")])
    return "\n".join(lines), ik(*kb_rows)

def _change_order_status(tid, oid, new_status, user):
    conn = get_db()
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.commit(); conn.close()
    label = ORDER_STATUS_LABELS.get(new_status, new_status)
    if creator:
        try: bot.send_message(creator["telegram_id"],
            f"📦 Заказ №*{onum(oid)}* → {label}", parse_mode="Markdown")
        except: pass

# ─── Создание заказа ──────────────────────────────────────────────────────────

def _start_new_order(tid):
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps:
        bot.send_message(tid, "❌ Нет контрагентов.\nДобавьте: `/cp_add Название | ...`", parse_mode="Markdown"); return
    user_states[tid] = "ord:cp"; user_data[tid] = {"items": []}
    rows = [(cp["name"], f"cp:sel:{cp['id']}") for cp in cps]
    rows_split = [[r] for r in rows]  # по одному в строку
    kb = ik(*rows_split)
    bot.send_message(tid, "📦 *Новый заказ*\n\nВыберите контрагента:", reply_markup=kb, parse_mode="Markdown")

def _order_cp_selected(tid, cp_id):
    conn = get_db()
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: return
    user_data[tid]["cp_id"] = cp_id; user_data[tid]["cp_name"] = cp["name"]
    user_states[tid] = "ord:date"
    bot.send_message(tid,
        f"✅ Контрагент: *{cp['name']}*\n\n"
        f"Введите желаемую дату готовности (ДД.ММ.ГГГГ)\nили отправьте *—* чтобы пропустить:",
        parse_mode="Markdown")

def _order_item_selected(tid, nom_id):
    conn = get_db()
    nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"] = {"id": nom_id, "name": nom["name"], "unit": nom["unit"]}
    user_states[tid] = "ord:qty"
    bot.send_message(tid, f"Введите количество для *{nom['name']}* ({nom['unit']}):", parse_mode="Markdown")

def _show_item_picker(tid, header="📦 Добавьте позиции в заказ:"):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall(); conn.close()
    d = user_data.get(tid, {})
    added = d.get("items", [])
    text = header + "\n"
    if added:
        text += "\n*Уже добавлено:*\n" + "\n".join(f"• {it['name']} — {it['qty']:,.1f} {it['unit']}" for it in added) + "\n"
    # Кнопки по 2 в ряд
    rows = []
    for i in range(0, len(items), 2):
        row = [(items[i]["name"], f"ni:{items[i]['id']}")]
        if i+1 < len(items): row.append((items[i+1]["name"], f"ni:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("✅ Готово — сохранить заказ", "ord:save")])
    kb = ik(*rows)
    bot.send_message(tid, text, reply_markup=kb, parse_mode="Markdown")

# ─── Отгрузка ─────────────────────────────────────────────────────────────────

def _start_shipment(tid, oid):
    conn = get_db()
    items = conn.execute(
        "SELECT oi.*,n.name,n.unit FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id "
        "WHERE oi.order_id=? AND oi.quantity>oi.shipped_qty", (oid,)
    ).fetchall(); conn.close()
    if not items: bot.send_message(tid, "✅ Все позиции уже отгружены."); return
    user_states[tid] = f"ship:{oid}:0"
    user_data[tid]   = {"order_id":oid, "items":[dict(it) for it in items], "ship_qtys":[]}
    it = items[0]; rem = it["quantity"] - it["shipped_qty"]
    bot.send_message(tid,
        f"📦 *Отгрузка заказа №{onum(oid)}*\n\n"
        f"Позиция 1/{len(items)}: *{it['name']}*\n"
        f"Остаток: {rem:,.1f} {it['unit']}\n\n"
        f"Введите количество для отгрузки:", parse_mode="Markdown")

def _pending_shipments_view():
    conn = get_db()
    ships = conn.execute(
        "SELECT s.*,o.id as oid FROM shipments s JOIN orders o ON o.id=s.order_id WHERE s.status='pending'"
    ).fetchall(); conn.close()
    if not ships: return "📦 Нет отгрузок для подтверждения.", None
    lines = ["📦 *Ожидают подтверждения:*\n"]
    rows  = []
    for s in ships:
        lines.append(f"• Отгрузка №{s['id']} — Заказ №{onum(s['oid'])}")
        rows.append([(f"Просмотреть отгрузку №{s['id']}", f"cs:v:{s['id']}")])
    return "\n".join(lines), ik(*rows)

def _shipment_detail_view(sid):
    conn = get_db()
    sitems = conn.execute(
        "SELECT si.*,n.name,n.unit,oi.order_id FROM shipment_items si "
        "JOIN order_items oi ON oi.id=si.order_item_id "
        "JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE si.shipment_id=?", (sid,)
    ).fetchall()
    ship = conn.execute("SELECT * FROM shipments WHERE id=?", (sid,)).fetchone(); conn.close()
    if not ship: return "Отгрузка не найдена.", None
    lines = [f"📦 *Отгрузка №{sid}* — Заказ №{onum(ship['order_id'])}\n\n*Позиции:*"]
    for si in sitems:
        lines.append(f"• {si['name']}: *{si['quantity']:,.1f} {si['unit']}*")
    return "\n".join(lines), ik(
        [(f"✅ Подтвердить отгрузку №{sid}", f"cs:ok:{sid}")],
        [("🔙 Назад", "cs:list")],
    )

def _confirm_shipment(tid, sid, user):
    now = datetime.now(); conn = get_db()
    sitems = conn.execute(
        "SELECT si.*,oi.nomenclature_id,oi.order_id FROM shipment_items si "
        "JOIN order_items oi ON oi.id=si.order_item_id WHERE si.shipment_id=?", (sid,)
    ).fetchall()
    for si in sitems:
        conn.execute("UPDATE order_items SET shipped_qty=shipped_qty+? WHERE id=?",
                     (si["quantity"], si["order_item_id"]))
    conn.execute("UPDATE shipments SET status='confirmed',confirmed_by=?,confirmed_at=? WHERE id=?",
                 (user["id"], now, sid))
    ship = conn.execute("SELECT * FROM shipments WHERE id=?", (sid,)).fetchone()
    oid  = ship["order_id"]
    rem  = conn.execute("SELECT SUM(quantity-shipped_qty) FROM order_items WHERE order_id=?", (oid,)).fetchone()[0] or 0
    new_status = "shipped" if rem<=0 else "ready"
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
    order   = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.commit(); conn.close()
    result = "полностью ✅" if new_status=="shipped" else "частично ⚠️"
    if creator:
        try: bot.send_message(creator["telegram_id"],
            f"📦 Отгрузка №{sid} по заказу №*{onum(oid)}* подтверждена!\nОтгружен {result}",
            parse_mode="Markdown")
        except: pass

# ─── Ежедневное производство ──────────────────────────────────────────────────

def _start_production(tid):
    today = datetime.now().strftime("%d.%m.%Y")
    user_states[tid] = "prod"; user_data[tid] = {"date": today, "items": []}
    _show_prod_picker(tid)

def _show_prod_picker(tid):
    d = user_data.get(tid, {}); today = d.get("date","")
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY id").fetchall(); conn.close()
    added = d.get("items", [])
    text = f"📊 *Производство за {today}*\n"
    if added:
        text += "\n*Уже введено:*\n" + "\n".join(f"• {it['name']}: {it['qty']:,.1f} {it['unit']}" for it in added) + "\n"
    text += "\nВыберите позицию:"
    rows = []
    for i in range(0, len(items), 2):
        row = [(items[i]["name"], f"pd:{items[i]['id']}")]
        if i+1 < len(items): row.append((items[i+1]["name"], f"pd:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("✅ Сохранить", "pd:done")])
    kb = ik(*rows)
    bot.send_message(tid, text, reply_markup=kb, parse_mode="Markdown")

def _prod_item_selected(tid, nom_id):
    conn = get_db()
    nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"] = {"id":nom_id,"name":nom["name"],"unit":nom["unit"]}
    user_states[tid] = "prod:qty"
    bot.send_message(tid, f"Введите количество *{nom['name']}* ({nom['unit']}) за сегодня:", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ ТЕКСТОВОГО ВВОДА (диалоги)
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id,"").startswith("rq:"))
def handle_request_text(message):
    user = get_user(message.from_user.id)
    if not user: return
    rtype = user_states.pop(message.from_user.id).replace("rq:","")
    labels = {"absence":"📅 Не выйдет на работу","breakdown":"🔧 Неисправность","mts":"📦 МТС"}
    now = datetime.now()
    conn = get_db()
    conn.execute("INSERT INTO requests (user_id,type,text,created_at) VALUES (?,?,?,?)",
                 (user["id"],rtype,message.text.strip(),now)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, "✅ Заявка отправлена!")
    notify_supervisors(
        f"📋 *{labels.get(rtype,rtype)}*\n\n👤 *{user['name']}*\n"
        f"🕐 {now.strftime('%d.%m.%Y %H:%M')}\n\n📝 {message.text.strip()}")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "ord:date")
def order_date_input(message):
    text = message.text.strip()
    user_data[message.from_user.id]["desired_date"] = None if text in ("-","пропустить") else text
    user_states[message.from_user.id] = "ord:items"
    _show_item_picker(message.from_user.id)

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "ord:qty")
def order_qty_input(message):
    try:
        qty = float(message.text.strip().replace(",",".")); assert qty>0
    except:
        bot.send_message(message.from_user.id, "Введите число больше нуля."); return
    d = user_data[message.from_user.id]; nom = d.pop("current_nom")
    d["items"].append({"nom_id":nom["id"],"name":nom["name"],"unit":nom["unit"],"qty":qty})
    user_states[message.from_user.id] = "ord:items"
    bot.send_message(message.from_user.id, f"✅ {nom['name']} — {qty:,.1f} {nom['unit']}")
    _show_item_picker(message.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data == "ord:save")
def order_save(call):
    user = get_user(call.from_user.id)
    d    = user_data.get(call.from_user.id, {})
    if not d.get("items"):
        ans(call, "❌ Добавьте хотя бы одну позицию.", True); return
    now  = datetime.now(); ans(call)
    conn = get_db()
    conn.execute(
        "INSERT INTO orders (counterparty_id,created_by,created_at,desired_date,status) VALUES (?,?,?,?,'new')",
        (d["cp_id"],user["id"],now,d.get("desired_date"))
    )
    oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]:
        conn.execute("INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)",
                     (oid,it["nom_id"],it["qty"]))
    conn.commit(); conn.close()
    cancel_state(call.from_user.id)
    bot.send_message(call.from_user.id,
        f"✅ Заказ №*{onum(oid)}* создан!\n👥 {d['cp_name']}\nПозиций: {len(d['items'])}",
        parse_mode="Markdown")
    lines = [f"📦 *Новый заказ №{onum(oid)}*\n👥 {d['cp_name']}\n⏰ {d.get('desired_date') or '—'}\n"]
    for it in d["items"]: lines.append(f"• {it['name']} — {it['qty']:,.1f} {it['unit']}")
    notify_role("manager", "\n".join(lines))

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id,"").startswith("ship:"))
def shipment_qty_input(message):
    state = user_states.get(message.from_user.id,""); parts = state.split(":")
    oid = int(parts[1]); idx = int(parts[2])
    d = user_data.get(message.from_user.id,{}); items = d.get("items",[])
    try:
        qty = float(message.text.strip().replace(",",".")); assert qty>=0
    except:
        bot.send_message(message.from_user.id, "Введите число (0 чтобы пропустить)."); return
    it = items[idx]; rem = it["quantity"] - it["shipped_qty"]
    qty = min(qty, rem)
    d["ship_qtys"].append({"order_item_id":it["id"],"qty":qty,"name":it["name"],"unit":it["unit"]})
    next_idx = idx+1
    if next_idx < len(items):
        user_states[message.from_user.id] = f"ship:{oid}:{next_idx}"
        nit = items[next_idx]; nr = nit["quantity"]-nit["shipped_qty"]
        bot.send_message(message.from_user.id,
            f"Позиция {next_idx+1}/{len(items)}: *{nit['name']}*\nОстаток: {nr:,.1f} {nit['unit']}\n\nВведите количество:",
            parse_mode="Markdown")
    else:
        # Сохраняем отгрузку
        user = get_user(message.from_user.id); now = datetime.now()
        conn = get_db()
        conn.execute("INSERT INTO shipments (order_id,created_by,created_at,status) VALUES (?,?,?,'pending')",
                     (oid,user["id"],now))
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for sq in d["ship_qtys"]:
            if sq["qty"]>0:
                conn.execute("INSERT INTO shipment_items (shipment_id,order_item_id,quantity) VALUES (?,?,?)",
                             (sid,sq["order_item_id"],sq["qty"]))
        conn.execute("UPDATE orders SET status='shipping' WHERE id=?", (oid,))
        conn.commit(); conn.close()
        cancel_state(message.from_user.id)
        lines = [f"✅ Отгрузка №{sid} по заказу №{onum(oid)} создана!\n\n*Позиции:*"]
        for sq in d["ship_qtys"]:
            if sq["qty"]>0: lines.append(f"• {sq['name']}: {sq['qty']:,.1f} {sq['unit']}")
        lines.append("\n⏳ Ожидает подтверждения Начальника Цеха.")
        bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")
        notify_role("manager", "\n".join(lines[:-1]) + f"\n\nНажмите *✅ Подтвердить отгрузку* в разделе Производство")

@bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "prod:qty")
def prod_qty_input(message):
    try:
        qty = float(message.text.strip().replace(",",".")); assert qty>=0
    except:
        bot.send_message(message.from_user.id, "Введите число."); return
    user = get_user(message.from_user.id)
    d = user_data[message.from_user.id]; nom = d.pop("current_nom"); today = d["date"]; now = datetime.now()
    conn = get_db()
    ex = conn.execute("SELECT * FROM daily_production WHERE date=? AND nomenclature_id=? AND recorded_by=?",
                      (today,nom["id"],user["id"])).fetchone()
    if ex: conn.execute("UPDATE daily_production SET quantity=quantity+?,recorded_at=? WHERE id=?", (qty,now,ex["id"]))
    else:  conn.execute("INSERT INTO daily_production (date,nomenclature_id,quantity,recorded_by,recorded_at) VALUES (?,?,?,?,?)",
                        (today,nom["id"],qty,user["id"],now))
    # Обновляем список
    updated = conn.execute(
        "SELECT dp.*,n.name,n.unit FROM daily_production dp JOIN nomenclature n ON n.id=dp.nomenclature_id WHERE dp.date=? AND dp.recorded_by=?",
        (today,user["id"])
    ).fetchall(); conn.commit(); conn.close()
    d["items"] = [{"name":e["name"],"unit":e["unit"],"qty":e["quantity"]} for e in updated]
    user_states[message.from_user.id] = "prod"
    bot.send_message(message.from_user.id, f"✅ {nom['name']}: {qty:,.1f} {nom['unit']}")
    _show_prod_picker(message.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data == "pd:done")
def prod_done(call):
    user = get_user(call.from_user.id)
    d    = user_data.get(call.from_user.id,{}); items = d.get("items",[])
    ans(call)
    cancel_state(call.from_user.id)
    if not items: bot.send_message(call.from_user.id, "Ничего не введено."); return
    today = d.get("date","")
    lines = [f"✅ *Производство за {today}:*\n"]
    for it in items: lines.append(f"• {it['name']}: {it['qty']:,.1f} {it['unit']}")
    bot.send_message(call.from_user.id, "\n".join(lines), parse_mode="Markdown")
    notify_supervisors("\n".join(lines))

# ─── Добавление контрагента и номенклатуры ────────────────────────────────────

@bot.message_handler(commands=["cp_add"])
def cp_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    raw = message.text.replace("/cp_add","",1).strip()
    parts = [p.strip() for p in raw.split("|")]
    if not parts[0]:
        bot.send_message(message.from_user.id,
            "📌 `/cp_add Название | тел | email | адрес | примечание`", parse_mode="Markdown"); return
    name=parts[0]; phone=parts[1] if len(parts)>1 else ""; email=parts[2] if len(parts)>2 else ""
    address=parts[3] if len(parts)>3 else ""; notes=parts[4] if len(parts)>4 else ""
    conn = get_db()
    conn.execute("INSERT INTO counterparties (name,phone,email,address,notes) VALUES (?,?,?,?,?)",
                 (name,phone,email,address,notes)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ Контрагент добавлен: *{name}*", parse_mode="Markdown")

@bot.message_handler(commands=["nom_add"])
def nom_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    raw = message.text.replace("/nom_add","",1).strip()
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 2:
        bot.send_message(message.from_user.id, "📌 `/nom_add Название | ед | примечание`", parse_mode="Markdown"); return
    name=parts[0]; unit=parts[1]; notes=parts[2] if len(parts)>2 else ""
    conn = get_db()
    conn.execute("INSERT INTO nomenclature (name,unit,notes) VALUES (?,?,?)", (name,unit,notes)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ Добавлено: *{name}* ({unit})", parse_mode="Markdown")

# ─── Заявки — вспомогательная функция ────────────────────────────────────────

def _start_request(tid, rtype):
    prompts = {"absence":"📅 Укажи дату и причину отсутствия:","breakdown":"🔧 Опиши неисправность:","mts":"📦 Опиши что нужно:"}
    user_states[tid] = f"rq:{rtype}"
    bot.send_message(tid, prompts[rtype]+"\n\n_/cancel для отмены_", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# НАПОМИНАНИЯ (фоновый поток)
# ═══════════════════════════════════════════════════════════════════════════════

def reminder_loop():
    while True:
        time.sleep(1800)
        try:
            conn = get_db(); now = datetime.now()
            rows = conn.execute(
                "SELECT t.*,u.name,u.telegram_id FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active'"
            ).fetchall()
            for rec in rows:
                dt = datetime.fromisoformat(rec["check_in"])
                count = rec["reminder_count"]
                last  = datetime.fromisoformat(rec["last_reminder"]) if rec["last_reminder"] else None
                elapsed = (now-dt).total_seconds()
                if count==0 and elapsed>=3600:
                    bot.send_message(rec["telegram_id"],
                        "⏰ *Напоминание:* ты не отметил уход.\nНажми '🚪 Ушёл с работы'", parse_mode="Markdown")
                    conn.execute("UPDATE time_records SET reminder_count=1,last_reminder=? WHERE id=?", (now,rec["id"]))
                    conn.commit()
                elif count==1 and last and (now-last).total_seconds()>=3600:
                    bot.send_message(rec["telegram_id"],
                        "⏰ *Второе напоминание:* отметь уход!", parse_mode="Markdown")
                    conn.execute("UPDATE time_records SET reminder_count=2,last_reminder=? WHERE id=?", (now,rec["id"]))
                    conn.commit()
                elif count>=2 and last and (now-last).total_seconds()>=3600:
                    conn.execute("UPDATE time_records SET status='no_checkout',check_out=? WHERE id=?", (now,rec["id"]))
                    conn.commit()
                    bot.send_message(rec["telegram_id"], "⚠️ Смена закрыта автоматически. Руководство уведомлено.")
                    notify_supervisors(f"⚠️ *{rec['name']}* не отметил уход.\nСмена с {dt.strftime('%H:%M')}.")
            conn.close()
        except Exception as e:
            print(f"[reminder error] {e}")

threading.Thread(target=reminder_loop, daemon=True).start()

print("Bot started!")
bot.infinity_polling()
