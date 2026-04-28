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
    "new":         "🆕 Новый",
    "accepted":    "✅ Принят",
    "in_progress": "🔧 В работе",
    "ready":       "🏁 Готов",
    "shipping":    "📦 На отгрузке",
    "shipped":     "✅ Отгружен",
}

MONTHS_RU = {
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",
    5:"Май",6:"Июнь",7:"Июль",8:"Август",
    9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}

INITIAL_NOMENCLATURE = [
    ("НОМ-001", "Гарпун Вид 1 (уз) белый",    "м",  "Намотка 200м"),
    ("НОМ-002", "Гарпун Вид 1 (уз) чёрный",   "м",  "Намотка 200м"),
    ("НОМ-003", "Гарпун Вид 2 (шир) белый",   "м",  "Намотка 200м"),
    ("НОМ-004", "Гарпун Вид 2 (шир) чёрный",  "м",  "Намотка 200м"),
    ("НОМ-005", "Вставка Т «Элит»",           "м",  "Первичное сырьё, намотка 50/150м"),
    ("НОМ-006", "Вставка Т чёрная",           "м",  "Намотка 50/150м"),
    ("НОМ-007", "Вставка Т",                  "м",  "Намотка 50/150м"),
    ("НОМ-008", "Вставка Уголок белая",       "м",  "Намотка 50/100м"),
    ("НОМ-009", "Вставка Уголок чёрная",      "м",  "Намотка 50/100м"),
    ("НОМ-010", "Багет ПВХ стеновой 150 г/м", "м",  "Вид 1, для пистолета, 2м/2.5м"),
    ("НОМ-011", "Багет ПВХ стеновой 140 г/м", "м",  "Вид 2, 2м/2.5м"),
    ("НОМ-012", "Платформа унив. 60-110",     "шт", "Серая, 50шт/короб, 150шт/мешок"),
    ("НОМ-013", "Платформа 90",               "шт", "Серая, 50шт/короб, 150шт/мешок"),
]

INITIAL_USERS = [
    (915402089, "Katran 150", "superadmin", 0),
]

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
            daily_rate REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            notes TEXT,
            initial_stock REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS stock_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nomenclature_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            type TEXT NOT NULL,
            comment TEXT,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (nomenclature_id) REFERENCES nomenclature(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS counterparties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            phone TEXT, email TEXT, address TEXT, notes TEXT,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE NOT NULL,
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
            number TEXT UNIQUE NOT NULL,
            order_id INTEGER NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            ship_date TEXT,
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

    if conn.execute("SELECT COUNT(*) FROM nomenclature").fetchone()[0] == 0:
        conn.executemany(
            "INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)",
            INITIAL_NOMENCLATURE
        )

    for tid, name, role, rate in INITIAL_USERS:
        ex = conn.execute("SELECT id FROM users WHERE telegram_id=?", (tid,)).fetchone()
        if ex:
            conn.execute("UPDATE users SET role=? WHERE telegram_id=?", (role,tid))
        else:
            conn.execute("INSERT INTO users (telegram_id,name,role,daily_rate) VALUES (?,?,?,?)",
                         (tid,name,role,rate))

    conn.commit()
    _migrate(conn)
    conn.close()
    print("✅ БД инициализирована")

def _migrate(conn):
    def cols(t):
        try: return [r[1] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()]
        except: return []
    changes = [
        ("users",        "daily_rate REAL DEFAULT 0"),
        ("users",        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("requests",     "status TEXT DEFAULT 'new'"),
        ("nomenclature", "code TEXT"),
        ("nomenclature", "initial_stock REAL DEFAULT 0"),
        ("nomenclature", "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("counterparties","code TEXT"),
        ("counterparties","created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("orders",       "number TEXT"),
        ("shipments",    "number TEXT"),
        ("shipments",    "ship_date TEXT"),
    ]
    for table, col_def in changes:
        col_name = col_def.split()[0]
        if col_name not in cols(table):
            try: conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
            except: pass
    conn.commit()

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
    rows = conn.execute("SELECT telegram_id FROM users WHERE role IN ('manager','admin','superadmin')").fetchall()
    conn.close()
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
    nom   = conn.execute("SELECT initial_stock FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
    init  = nom["initial_stock"] if nom else 0
    prod  = conn.execute("SELECT COALESCE(SUM(quantity),0) FROM daily_production WHERE nomenclature_id=?", (nom_id,)).fetchone()[0]
    adj   = conn.execute("SELECT COALESCE(SUM(CASE WHEN type='add' THEN quantity ELSE -quantity END),0) FROM stock_adjustments WHERE nomenclature_id=?", (nom_id,)).fetchone()[0]
    ship  = conn.execute(
        "SELECT COALESCE(SUM(si.quantity),0) FROM shipment_items si "
        "JOIN shipments s ON s.id=si.shipment_id "
        "JOIN order_items oi ON oi.id=si.order_item_id "
        "WHERE oi.nomenclature_id=? AND s.status='confirmed'", (nom_id,)
    ).fetchone()[0]
    conn.close()
    return init + prod + adj - ship

def next_order_number():
    conn = get_db()
    year = datetime.now().year
    last = conn.execute(
        "SELECT number FROM orders WHERE number LIKE ? ORDER BY id DESC LIMIT 1",
        (f"{year}-%",)
    ).fetchone()
    conn.close()
    if last:
        num = int(last["number"].split("-")[1]) + 1
    else:
        num = 1
    return f"{year}-{num:03d}"

def next_shipment_number():
    conn = get_db()
    year = datetime.now().year
    last = conn.execute(
        "SELECT number FROM shipments WHERE number LIKE ? ORDER BY id DESC LIMIT 1",
        (f"ОТГ-{year}-%",)
    ).fetchone()
    conn.close()
    if last:
        num = int(last["number"].split("-")[2]) + 1
    else:
        num = 1
    return f"ОТГ-{year}-{num:03d}"

def fmt_dt(dt_str):
    if not dt_str: return "—"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%d.%m.%Y %H:%M")
    except:
        return dt_str

def cancel_state(tid):
    user_states.pop(tid, None)
    user_data.pop(tid, None)

# ═══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════════════════════

def ik(*rows):
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

def staff_menu_kb(role):
    rows = [
        [("📊 Отчёт за неделю", "rp:week"), ("📊 Отчёт за месяц", "rp:month")],
        [("👷 Кто сейчас на смене", "shift:now")],
    ]
    if role in ("admin","superadmin"):
        rows.append([("👥 Список сотрудников", "usr:list")])
    rows.append([("❓ Справка", "help")])
    return ik(*rows)

def prod_menu_kb(role):
    rows = [
        [("📦 Заказы", "ord:list")],
        [("📊 Производство за день", "pd:start"), ("🏭 Склад", "wh")],
    ]
    if role in ("admin","superadmin"):
        rows.append([("➕ Новый заказ", "ord:new"), ("👥 Контрагенты", "cp:list")])
        rows.append([("📋 Номенклатура", "nm:list"), ("⚙️ Инвентаризация", "inv:menu")])
        rows.append([("✅ Подтвердить отгрузку", "cs:list")])
    else:
        rows.append([("✅ Подтвердить отгрузку", "cs:list")])
    return ik(*rows)

def req_kb():
    return ik(
        [("📅 Не выйду на работу", "rq:abs")],
        [("🔧 Неисправность оборудования", "rq:brk")],
        [("📦 Заявка на МТС", "rq:mts")],
        [("❓ Справка", "help")],
    )

# ═══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ
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
    if user: send_menu(message.from_user.id, user["role"], user["name"])
    else: bot.send_message(message.from_user.id, "Отменено.")

@bot.message_handler(commands=["setup"])
def cmd_setup(message):
    conn = get_db()
    if conn.execute("SELECT id FROM users WHERE role='superadmin'").fetchone():
        conn.close(); bot.send_message(message.from_user.id, "⛔ Главный Админ уже зарегистрирован."); return
    first = message.from_user.first_name or ""; last = message.from_user.last_name or ""
    name  = f"{first} {last}".strip() or f"SuperAdmin_{message.from_user.id}"
    if conn.execute("SELECT id FROM users WHERE telegram_id=?", (message.from_user.id,)).fetchone():
        conn.execute("UPDATE users SET role='superadmin',name=? WHERE telegram_id=?", (name,message.from_user.id))
    else:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,'superadmin')", (message.from_user.id,name))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ Вы — *Главный Админ*!\nИмя: *{name}*\nID: `{message.from_user.id}`",
        parse_mode="Markdown")
    send_menu(message.from_user.id, "superadmin", name)

@bot.message_handler(commands=["add"])
def cmd_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin","superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 4:
        hint = "`worker`|`manager`|`admin`" if user["role"]=="superadmin" else "`worker`|`manager`"
        bot.send_message(message.from_user.id,
            f"📌 `/add [id] [имя фамилия] [роль]`\nРоли: {hint}", parse_mode="Markdown"); return
    tid_str=parts[1]; role=parts[-1]; name=" ".join(parts[2:-1])
    allowed = SUPER_MANAGED if user["role"]=="superadmin" else ADMIN_MANAGED
    if role not in allowed:
        bot.send_message(message.from_user.id,
            f"❌ Роли: {', '.join(f'`{r}`' for r in allowed)}", parse_mode="Markdown"); return
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
        bot.send_message(message.from_user.id, "⚠️ Пользователь уже есть.")
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
    except: bot.send_message(message.from_user.id, "❌ ID числом."); return
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
        bot.send_message(message.from_user.id, "📌 `/setrate [id] [сумма]`", parse_mode="Markdown"); return
    try: tid=int(parts[1]); rate=float(parts[2])
    except: bot.send_message(message.from_user.id, "❌ Неверный формат."); return
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not target or target["role"]!="worker":
        bot.send_message(message.from_user.id, "❌ Рабочий не найден."); conn.close(); return
    conn.execute("UPDATE users SET daily_rate=? WHERE telegram_id=?", (rate,tid)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"✅ *{target['name']}*\n💰 {rate:,.0f} ₽/день | {rate/8:,.2f} ₽/ч", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# НИЖНИЕ КНОПКИ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "✅ Пришёл на работу")
def check_in(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db()
    active = conn.execute("SELECT * FROM time_records WHERE user_id=? AND status='active'", (user["id"],)).fetchone()
    if active:
        bot.send_message(message.from_user.id, f"⚠️ Уже на работе с {fmt_dt(active['check_in'])}."); conn.close(); return
    now = datetime.now()
    conn.execute("INSERT INTO time_records (user_id,check_in) VALUES (?,?)", (user["id"],now)); conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ Приход: *{now.strftime('%H:%M %d.%m.%Y')}*", parse_mode="Markdown")
    notify_supervisors(f"✅ *{user['name']}* пришёл в {now.strftime('%H:%M %d.%m.%Y')}")

@bot.message_handler(func=lambda m: m.text == "🚪 Ушёл с работы")
def check_out(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker": return
    conn = get_db()
    active = conn.execute("SELECT * FROM time_records WHERE user_id=? AND status='active'", (user["id"],)).fetchone()
    if not active: bot.send_message(message.from_user.id, "⚠️ Не отмечен как на работе!"); conn.close(); return
    now = datetime.now(); hrs = (now-datetime.fromisoformat(active["check_in"])).total_seconds()/3600
    conn.execute("UPDATE time_records SET check_out=?,status='closed' WHERE id=?", (now,active["id"])); conn.commit(); conn.close()
    bot.send_message(message.from_user.id,
        f"👋 Уход: *{now.strftime('%H:%M %d.%m.%Y')}*\n⏱ Отработано: *{hrs:.1f} ч.*", parse_mode="Markdown")
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
    ms = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0); md,mh = calc(ms)
    rate = user["daily_rate"] or 0; hourly = rate/8 if rate>0 else 0
    active = conn.execute("SELECT check_in FROM time_records WHERE user_id=? AND status='active'", (user["id"],)).fetchone(); conn.close()
    lines = [f"📈 *{user['name']}*\n"]
    if active:
        dt = datetime.fromisoformat(active["check_in"]); hrs = (now-dt).total_seconds()/3600
        lines.append(f"🟢 На смене с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)\n")
    wn = now.isocalendar()[1]; mn = MONTHS_RU[now.month]
    lines.append(f"📅 *Неделя №{wn}:* {wd} дн. | {wh:.1f} ч.")
    if rate>0: lines.append(f"💰 {wh*hourly:,.0f} ₽")
    lines.append(f"\n📆 *{mn} {now.year}:* {md} дн. | {mh:.1f} ч.")
    if rate>0: lines.append(f"💰 {mh*hourly:,.0f} ₽")
    if rate>0: lines.append(f"\n⏱ Ставка: {rate:,.0f} ₽/день")
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
# ИНЛАЙН КОЛБЭКИ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    user = get_user(call.from_user.id)
    if not user: ans(call, "⛔ Не зарегистрированы.", True); return
    cd = call.data

    if cd == "rp:week":   ans(call); edit(call, _gen_report(7)); return
    if cd == "rp:month":  ans(call); edit(call, _gen_report(0)); return
    if cd == "shift:now": ans(call); edit(call, _who_on_shift()); return
    if cd == "usr:list":  ans(call); edit(call, _staff_list(user["role"])); return
    if cd == "help":      ans(call); edit(call, _help_text(user["role"])); return
    if cd == "wh":        ans(call); edit(call, _warehouse_text()); return
    if cd == "nm:list":   ans(call); edit(call, _nom_list_text(), _nom_registry_kb(user["role"])); return
    if cd == "cp:list":   ans(call); edit(call, _cp_list_text(), _cp_registry_kb()); return
    if cd == "ord:list":  ans(call); t,k = _orders_list_view(user["role"]); edit(call, t, k); return
    if cd == "cs:list":   ans(call); t,k = _pending_shipments_view(); edit(call, t, k); return

    # Заявки
    if cd == "rq:abs": ans(call); _start_request(call.from_user.id, "absence"); return
    if cd == "rq:brk": ans(call); _start_request(call.from_user.id, "breakdown"); return
    if cd == "rq:mts": ans(call); _start_request(call.from_user.id, "mts"); return

    # Просмотр заказа
    if cd.startswith("ord:v:"):
        oid = int(cd.split(":")[2]); ans(call)
        t,k = _order_detail_view(oid, user["role"]); edit(call, t, k); return

    # Смена статуса заказа
    if cd.startswith("ord:s:"):
        parts = cd.split(":"); oid=int(parts[2]); new_st=parts[3]; ans(call)
        _change_order_status(call.from_user.id, oid, new_st, user)
        t,k = _order_detail_view(oid, user["role"]); edit(call, t, k); return

    # Отгрузка
    if cd.startswith("ord:ship:"):
        oid = int(cd.split(":")[2]); ans(call); _start_shipment(call.from_user.id, oid); return

    # Новый заказ
    if cd == "ord:new": ans(call); _start_new_order(call.from_user.id); return

    # Выбор контрагента в заказе
    if cd.startswith("cp:sel:"):
        cp_id = int(cd.split(":")[2]); ans(call); _order_cp_selected(call.from_user.id, cp_id); return

    # Выбор позиции в заказе
    if cd.startswith("ni:"):
        nom_id = int(cd.split(":")[1]); ans(call); _order_item_selected(call.from_user.id, nom_id); return

    # Сохранить заказ
    if cd == "ord:save": _order_save(call, user); return

    # Производство за день
    if cd == "pd:start": ans(call); _start_production(call.from_user.id); return
    if cd.startswith("pd:n:"): nom_id=int(cd.split(":")[2]); ans(call); _prod_item_selected(call.from_user.id, nom_id); return
    if cd == "pd:done": _prod_done(call); return

    # Подтверждение отгрузки
    if cd.startswith("cs:v:"):
        sid=int(cd.split(":")[2]); ans(call); t,k=_shipment_detail_view(sid); edit(call,t,k); return
    if cd.startswith("cs:ok:"):
        sid=int(cd.split(":")[2]); ans(call); _confirm_shipment(call.from_user.id, sid, user)
        edit(call, "✅ Отгрузка подтверждена!"); return

    # Номенклатура
    if cd == "nm:add":  ans(call); _start_nom_add(call.from_user.id); return
    if cd.startswith("nm:v:"): nom_id=int(cd.split(":")[2]); ans(call); t,k=_nom_detail(nom_id,user["role"]); edit(call,t,k); return
    if cd.startswith("nm:edit:"): parts=cd.split(":"); nom_id=int(parts[2]); field=parts[3]; ans(call); _start_nom_edit(call.from_user.id,nom_id,field); return

    # Контрагенты
    if cd == "cp:add": ans(call); _start_cp_add(call.from_user.id); return
    if cd.startswith("cp:v:"): cp_id=int(cd.split(":")[2]); ans(call); t,k=_cp_detail(cp_id,user["role"]); edit(call,t,k); return
    if cd.startswith("cp:edit:"): parts=cd.split(":"); cp_id=int(parts[2]); field=parts[3]; ans(call); _start_cp_edit(call.from_user.id,cp_id,field); return

    # Инвентаризация
    if cd == "inv:menu": ans(call); t,k=_inv_menu(); bot.send_message(call.from_user.id,t,reply_markup=k,parse_mode="Markdown"); return
    if cd == "inv:init": ans(call); _start_inv_init(call.from_user.id); return
    if cd == "inv:adj":  ans(call); _start_inv_adj(call.from_user.id); return
    if cd.startswith("inv:n:"): nom_id=int(cd.split(":")[2]); ans(call); _inv_nom_selected(call.from_user.id,nom_id); return
    if cd.startswith("inv:adj:n:"): nom_id=int(cd.split(":")[3]); ans(call); _inv_adj_nom_selected(call.from_user.id,nom_id); return
    if cd.startswith("inv:adj:add:") or cd.startswith("inv:adj:sub:") or cd.startswith("inv:adj:set:"):
        parts=cd.split(":"); action=parts[2]; nom_id=int(parts[3])
        user_data[call.from_user.id]["adj_type"]=action; user_data[call.from_user.id]["nom_id"]=nom_id
        labels={"add":"прибавить","sub":"убрать","set":"установить точное значение"}
        user_states[call.from_user.id]="inv:adj:qty"; ans(call)
        bot.send_message(call.from_user.id,
            f"Введите количество ({labels[action]}):\n\n_/cancel для отмены_", parse_mode="Markdown"); return

    ans(call)

# ═══════════════════════════════════════════════════════════════════════════════
# КОНТЕНТ — ОТЧЁТЫ И СПИСКИ
# ═══════════════════════════════════════════════════════════════════════════════

def _gen_report(days):
    conn = get_db(); now = datetime.now()
    if days == 7:
        since = now-timedelta(days=7); wn = now.isocalendar()[1]
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
        if rate>0: lines.append(f"   💰 {th*hourly:,.0f} ₽")
        if no_mrk: lines.append(f"   ⚠️ Не отметил уход: {len(no_mrk)} раз(а)")
        lines.append("")
    conn.close(); return "\n".join(lines)

def _who_on_shift():
    conn = get_db()
    rows = conn.execute("SELECT u.name,t.check_in FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active' ORDER BY t.check_in").fetchall()
    conn.close()
    if not rows: return "👷 Сейчас никого нет на смене."
    lines = ["👷 *Сейчас на смене:*\n"]
    for r in rows:
        dt = datetime.fromisoformat(r["check_in"]); hrs = (datetime.now()-dt).total_seconds()/3600
        lines.append(f"• *{r['name']}* — с {dt.strftime('%H:%M %d.%m')} ({hrs:.1f} ч.)")
    return "\n".join(lines)

def _staff_list(role):
    conn = get_db()
    if role=="superadmin": users = conn.execute("SELECT * FROM users ORDER BY role,name").fetchall()
    else: users = conn.execute("SELECT * FROM users WHERE role IN ('worker','manager') ORDER BY role,name").fetchall()
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
        "worker": "📖 *Рабочий*\n\n✅/🚪 Приход и уход\n📈 Статистика и заработок\n📋 Заявки",
        "manager": "📖 *Начальник Цеха*\n\n*👥 Сотрудники:* отчёты, кто на смене\n*📦 Производство:*\n• Заказы — статусы\n• Производство за день\n• Склад\n• Подтвердить отгрузку",
        "admin": "📖 *Администратор*\n\n*/add /remove /setrate*\n*👥 Производство:*\n• Заказы, новый заказ\n• Контрагенты, Номенклатура\n• Склад, Инвентаризация",
        "superadmin": "📖 *Главный Админ*\n\nПолный доступ.\n*/add [id] [имя] worker|manager|admin*\n*/remove [id]*\n*/setrate [id] [сумма]*",
    }
    return texts.get(role, "")

def _nom_list_text():
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    lines = ["📋 *Номенклатура:*\n"]
    for it in items:
        stock = get_stock(it["id"]); icon = "✅" if stock>0 else "⚠️"
        lines.append(f"{icon} *{it['code']}* | {it['name']} ({it['unit']}) | {stock:,.1f}")
    return "\n".join(lines)

def _nom_registry_kb(role):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    rows = []
    if role in ("admin","superadmin"):
        rows.append([("➕ Добавить номенклатуру", "nm:add")])
    for it in items:
        rows.append([(f"{it['code']} — {it['name']}", f"nm:v:{it['id']}")])
    return ik(*rows)

def _nom_detail(nom_id, role):
    conn = get_db()
    it = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not it: return "Не найдено.", None
    stock = get_stock(nom_id)
    lines = [
        f"📋 *{it['name']}*\n",
        f"🔑 Код: `{it['code'] or '—'}`",
        f"📦 Единица: {it['unit']}",
        f"📝 Примечание: {it['notes'] or '—'}",
        f"🏭 Остаток на складе: *{stock:,.1f} {it['unit']}*",
        f"📅 Добавлена: {fmt_dt(it['created_at'])}",
    ]
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([(f"✏️ Изменить название", f"nm:edit:{nom_id}:name")])
        kb_rows.append([(f"✏️ Изменить единицу", f"nm:edit:{nom_id}:unit"), (f"✏️ Примечание", f"nm:edit:{nom_id}:notes")])
    kb_rows.append([("🔙 К номенклатуре", "nm:list")])
    return "\n".join(lines), ik(*kb_rows)

def _cp_list_text():
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps: return "👥 Контрагентов нет."
    lines = ["👥 *Контрагенты:*\n"]
    for cp in cps:
        lines.append(f"• *{cp['code'] or '—'}* | *{cp['name']}*")
    return "\n".join(lines)

def _cp_registry_kb():
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    rows = [[("➕ Добавить контрагента", "cp:add")]]
    for cp in cps:
        rows.append([(f"{cp['code'] or '—'} — {cp['name']}", f"cp:v:{cp['id']}")])
    return ik(*rows)

def _cp_detail(cp_id, role):
    conn = get_db()
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: return "Не найдено.", None
    lines = [
        f"👥 *{cp['name']}*\n",
        f"🔑 Код: `{cp['code'] or '—'}`",
        f"📞 Телефон: {cp['phone'] or '—'}",
        f"📧 Email: {cp['email'] or '—'}",
        f"📍 Адрес: {cp['address'] or '—'}",
        f"📝 Примечание: {cp['notes'] or '—'}",
        f"📅 Добавлен: {fmt_dt(cp['created_at'])}",
    ]
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([("✏️ Название", f"cp:edit:{cp_id}:name"), ("✏️ Код", f"cp:edit:{cp_id}:code")])
        kb_rows.append([("✏️ Телефон", f"cp:edit:{cp_id}:phone"), ("✏️ Email", f"cp:edit:{cp_id}:email")])
        kb_rows.append([("✏️ Адрес", f"cp:edit:{cp_id}:address"), ("✏️ Примечание", f"cp:edit:{cp_id}:notes")])
    kb_rows.append([("🔙 К контрагентам", "cp:list")])
    return "\n".join(lines), ik(*kb_rows)

def _warehouse_text():
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    now = datetime.now()
    lines = [f"🏭 *Склад на {now.strftime('%d.%m.%Y %H:%M')}:*\n"]
    for it in items:
        s = get_stock(it["id"]); icon = "✅" if s>0 else ("⚠️" if s==0 else "🔴")
        lines.append(f"{icon} *{it['name']}*: {s:,.1f} {it['unit']}")
    return "\n".join(lines)

def _inv_menu():
    text = "⚙️ *Инвентаризация и остатки*\n\nВыберите действие:"
    kb = ik(
        [("📥 Ввести начальные остатки", "inv:init")],
        [("🔧 Корректировка (инвентаризация)", "inv:adj")],
        [("🔙 Назад", "mn:prod")],
    )
    return text, kb

# ═══════════════════════════════════════════════════════════════════════════════
# ДИАЛОГИ — ДОБАВЛЕНИЕ КОНТРАГЕНТА
# ═══════════════════════════════════════════════════════════════════════════════

def _start_cp_add(tid):
    user_states[tid] = "cp:add:code"
    user_data[tid]   = {}
    bot.send_message(tid, "👥 *Новый контрагент*\n\nВведите код контрагента (например КА-001):\n\n_/cancel для отмены_", parse_mode="Markdown")

def _start_cp_edit(tid, cp_id, field):
    field_names = {"name":"название","code":"код","phone":"телефон","email":"email","address":"адрес","notes":"примечание"}
    user_states[tid] = f"cp:edit:{cp_id}:{field}"
    bot.send_message(tid, f"✏️ Введите новое {field_names.get(field,field)}:\n\n_/cancel для отмены_", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# ДИАЛОГИ — ДОБАВЛЕНИЕ НОМЕНКЛАТУРЫ
# ═══════════════════════════════════════════════════════════════════════════════

def _start_nom_add(tid):
    user_states[tid] = "nm:add:code"
    user_data[tid]   = {}
    bot.send_message(tid, "📋 *Новая позиция номенклатуры*\n\nВведите код (например НОМ-014):\n\n_/cancel для отмены_", parse_mode="Markdown")

def _start_nom_edit(tid, nom_id, field):
    field_names = {"name":"название","unit":"единицу измерения","notes":"примечание","code":"код"}
    user_states[tid] = f"nm:edit:{nom_id}:{field}"
    bot.send_message(tid, f"✏️ Введите новое {field_names.get(field,field)}:\n\n_/cancel для отмены_", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# ДИАЛОГИ — ИНВЕНТАРИЗАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

def _start_inv_init(tid):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    rows = [[("📋 "+(it["code"] or f"НОМ-{it['id']:03d}")+" — "+it["name"], f"inv:n:{it['id']}")] for it in items]
    kb = ik(*rows)
    user_states[tid] = "inv:init"
    bot.send_message(tid, "📥 *Начальные остатки*\n\nВыберите позицию:", reply_markup=kb, parse_mode="Markdown")

def _inv_nom_selected(tid, nom_id):
    conn = get_db()
    nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    cur = get_stock(nom_id)
    user_data[tid] = {"nom_id": nom_id, "nom_name": nom["name"], "unit": nom["unit"]}
    user_states[tid] = "inv:init:qty"
    bot.send_message(tid,
        f"📥 *{nom['name']}*\nТекущий остаток: {cur:,.1f} {nom['unit']}\n\nВведите начальный остаток:",
        parse_mode="Markdown")

def _start_inv_adj(tid):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    rows = [[("📋 "+(it["code"] or f"НОМ-{it['id']:03d}")+" — "+it["name"], f"inv:adj:n:{it['id']}")] for it in items]
    kb = ik(*rows)
    user_states[tid] = "inv:adj"
    bot.send_message(tid, "🔧 *Корректировка остатков*\n\nВыберите позицию:", reply_markup=kb, parse_mode="Markdown")

def _inv_adj_nom_selected(tid, nom_id):
    conn = get_db()
    nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    cur = get_stock(nom_id)
    user_data[tid] = {"nom_id": nom_id, "nom_name": nom["name"], "unit": nom["unit"]}
    user_states[tid] = "inv:adj:type"
    kb = ik(
        [(f"➕ Прибавить к {cur:,.1f} {nom['unit']}", f"inv:adj:add:{nom_id}")],
        [(f"➖ Убрать из {cur:,.1f} {nom['unit']}", f"inv:adj:sub:{nom_id}")],
        [(f"🎯 Установить точное значение", f"inv:adj:set:{nom_id}")],
    )
    bot.send_message(tid,
        f"🔧 *{nom['name']}*\nТекущий остаток: *{cur:,.1f} {nom['unit']}*\n\nВыберите тип корректировки:",
        reply_markup=kb, parse_mode="Markdown")



# ═══════════════════════════════════════════════════════════════════════════════
# ЗАКАЗЫ
# ═══════════════════════════════════════════════════════════════════════════════

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
            "ORDER BY o.created_at DESC LIMIT 30"
        ).fetchall()
    conn.close()
    if not orders: return "📦 Заказов нет.", ik([("🔙 Назад","mn:prod")])
    lines = ["📦 *Реестр заказов:*\n"]
    kb_rows = []
    for o in orders:
        st = ORDER_STATUS_LABELS.get(o["status"],o["status"])
        date_info = f" | до {o['desired_date']}" if o["desired_date"] else ""
        lines.append(f"• *{o['number']}* — {o['cp_name'] or '—'} | {st}{date_info}")
        kb_rows.append([(f"{o['number']} | {o['cp_name'] or '—'} | {st}", f"ord:v:{o['id']}")])
    kb_rows.append([("🔙 Назад","mn:prod")])
    return "\n".join(lines), ik(*kb_rows)

def _order_detail_view(oid, role):
    conn = get_db()
    o  = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o: conn.close(); return "Заказ не найден.", None
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (o["counterparty_id"],)).fetchone()
    cr = conn.execute("SELECT name FROM users WHERE id=?", (o["created_by"],)).fetchone()
    items = conn.execute(
        "SELECT oi.*,n.name,n.unit,n.code FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE oi.order_id=?",
        (oid,)
    ).fetchall(); conn.close()
    status = ORDER_STATUS_LABELS.get(o["status"],o["status"])
    lines = [
        f"📦 *Заказ {o['number']}*",
        f"👥 Контрагент: *{cp['name'] if cp else '—'}* ({cp['code'] if cp else '—'})",
        f"📅 Создан: {fmt_dt(o['created_at'])}",
        f"⏰ Желаемая готовность: {o['desired_date'] or '—'}",
        f"👤 Создал: {cr['name'] if cr else '—'}",
        f"📊 Статус: {status}\n",
        "*Позиции:*"
    ]
    for it in items:
        rem = it["quantity"]-it["shipped_qty"]; stock = get_stock(it["nomenclature_id"])
        to_make = max(0, rem-stock); icon = "✅" if stock>=rem else "⚠️"
        lines.append(
            f"• *{it['code']}* {it['name']}\n"
            f"  Заказано: {it['quantity']:,.1f} | Отгружено: {it['shipped_qty']:,.1f} {it['unit']}\n"
            f"  {icon} Склад: {stock:,.1f} | 🔧 Произвести: {to_make:,.1f}"
        )
    kb_rows = []
    if role == "manager":
        BTN = {"new":("✅ Принять",f"ord:s:{oid}:accepted"),"accepted":("🔧 В работу",f"ord:s:{oid}:in_progress"),"in_progress":("🏁 Готово",f"ord:s:{oid}:ready")}
        if o["status"] in BTN: kb_rows.append([BTN[o["status"]]])
    if role in ("admin","superadmin") and o["status"] in ("ready","in_progress","accepted"):
        kb_rows.append([("📦 Оформить отгрузку", f"ord:ship:{oid}")])
    kb_rows.append([("🔙 К заказам","ord:list")])
    return "\n".join(lines), ik(*kb_rows)

def _change_order_status(tid, oid, new_status, user):
    conn = get_db()
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status,oid))
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.commit(); conn.close()
    if creator:
        try: bot.send_message(creator["telegram_id"],
            f"📦 Заказ *{order['number']}* → {ORDER_STATUS_LABELS.get(new_status,new_status)}", parse_mode="Markdown")
        except: pass

def _start_new_order(tid):
    conn = get_db()
    cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps:
        bot.send_message(tid, "❌ Нет контрагентов.\nДобавьте через раздел Контрагенты."); return
    user_states[tid] = "ord:cp"; user_data[tid] = {"items":[]}
    rows = [[(f"{cp['code'] or '—'} — {cp['name']}", f"cp:sel:{cp['id']}")] for cp in cps]
    kb = ik(*rows)
    bot.send_message(tid, "📦 *Новый заказ*\n\nВыберите контрагента:", reply_markup=kb, parse_mode="Markdown")

def _order_cp_selected(tid, cp_id):
    conn = get_db(); cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: return
    user_data[tid]["cp_id"] = cp_id; user_data[tid]["cp_name"] = cp["name"]
    user_states[tid] = "ord:date"
    bot.send_message(tid,
        f"✅ Контрагент: *{cp['name']}*\n\nВведите желаемую дату готовности (ДД.ММ.ГГГГ)\nили *—* чтобы пропустить:",
        parse_mode="Markdown")

def _order_item_selected(tid, nom_id):
    conn = get_db(); nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"] = {"id":nom_id,"name":nom["name"],"unit":nom["unit"],"code":(nom["code"] or "—")}
    user_states[tid] = "ord:qty"
    bot.send_message(tid, f"Введите количество для *{nom['name']}* ({nom['unit']}):", parse_mode="Markdown")

def _show_item_picker(tid):
    d = user_data.get(tid,{}); added = d.get("items",[])
    conn = get_db(); items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    text = "📦 *Добавьте позиции:*\n"
    if added:
        text += "\n*Добавлено:*\n" + "\n".join(f"• {(it['code'] or '—')} {it['name']} — {it['qty']:,.1f} {it['unit']}" for it in added) + "\n"
    rows = []
    for i in range(0, len(items), 2):
        row = [((items[i]["code"] or "—")+" "+items[i]["name"][:20], f"ni:{items[i]['id']}")]
        if i+1 < len(items): row.append(((items[i+1]["code"] or "—")+" "+items[i+1]["name"][:20], f"ni:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("✅ Сохранить заказ","ord:save")])
    bot.send_message(tid, text, reply_markup=ik(*rows), parse_mode="Markdown")

def _order_save(call, user):
    d = user_data.get(call.from_user.id,{})
    if not d.get("items"): ans(call,"❌ Добавьте позицию.",True); return
    now = datetime.now(); ans(call)
    number = next_order_number()
    conn = get_db()
    conn.execute(
        "INSERT INTO orders (number,counterparty_id,created_by,created_at,desired_date,status) VALUES (?,?,?,?,?,'new')",
        (number,d["cp_id"],user["id"],now,d.get("desired_date"))
    )
    oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]:
        conn.execute("INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)", (oid,it["nom_id"],it["qty"]))
    conn.commit(); conn.close()
    cancel_state(call.from_user.id)
    bot.send_message(call.from_user.id,
        f"✅ Заказ *{number}* создан!\n👥 {d['cp_name']}\n📅 {now.strftime('%d.%m.%Y %H:%M')}\nПозиций: {len(d['items'])}",
        parse_mode="Markdown")
    lines = [f"📦 *Новый заказ {number}*\n👥 {d['cp_name']}\n⏰ {d.get('desired_date') or '—'}\n📅 {now.strftime('%d.%m.%Y %H:%M')}\n"]
    for it in d["items"]: lines.append(f"• {(it['code'] or '—')} {it['name']} — {it['qty']:,.1f} {it['unit']}")
    notify_role("manager", "\n".join(lines))

# ═══════════════════════════════════════════════════════════════════════════════
# ОТГРУЗКА
# ═══════════════════════════════════════════════════════════════════════════════

def _start_shipment(tid, oid):
    conn = get_db()
    items = conn.execute(
        "SELECT oi.*,n.name,n.unit,n.code FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id "
        "WHERE oi.order_id=? AND oi.quantity>oi.shipped_qty", (oid,)
    ).fetchall(); conn.close()
    if not items: bot.send_message(tid, "✅ Все позиции уже отгружены."); return
    user_states[tid] = f"ship:{oid}:0"
    user_data[tid]   = {"order_id":oid,"items":[dict(it) for it in items],"ship_qtys":[]}
    it = items[0]; rem = it["quantity"]-it["shipped_qty"]
    user_states[tid] = f"ship:{oid}:date"
    bot.send_message(tid,
        f"📦 *Отгрузка по заказу {_get_order_number(oid)}*\n\nВведите дату отгрузки (ДД.ММ.ГГГГ)\nили *—* для сегодня:",
        parse_mode="Markdown")

def _get_order_number(oid):
    conn = get_db(); o = conn.execute("SELECT number FROM orders WHERE id=?", (oid,)).fetchone(); conn.close()
    return o["number"] if o else str(oid)

def _pending_shipments_view():
    conn = get_db()
    ships = conn.execute(
        "SELECT s.*,o.number as onum FROM shipments s JOIN orders o ON o.id=s.order_id WHERE s.status='pending'"
    ).fetchall(); conn.close()
    if not ships: return "📦 Нет отгрузок для подтверждения.", ik([("🔙 Назад","mn:prod")])
    lines = ["📦 *Ожидают подтверждения:*\n"]
    rows  = []
    for s in ships:
        lines.append(f"• *{s['number']}* по заказу {s['onum']}")
        rows.append([(f"{s['number']} | Заказ {s['onum']}", f"cs:v:{s['id']}")])
    rows.append([("🔙 Назад","mn:prod")])
    return "\n".join(lines), ik(*rows)

def _shipment_detail_view(sid):
    conn = get_db()
    ship = conn.execute("SELECT * FROM shipments WHERE id=?", (sid,)).fetchone()
    if not ship: conn.close(); return "Не найдено.", None
    o = conn.execute("SELECT number FROM orders WHERE id=?", (ship["order_id"],)).fetchone()
    sitems = conn.execute(
        "SELECT si.*,n.name,n.unit,n.code FROM shipment_items si "
        "JOIN order_items oi ON oi.id=si.order_item_id "
        "JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE si.shipment_id=?", (sid,)
    ).fetchall(); conn.close()
    lines = [
        f"📦 *{ship['number']}*",
        f"📋 Заказ: {o['number'] if o else '—'}",
        f"📅 Создана: {fmt_dt(ship['created_at'])}",
        f"🚚 Дата отгрузки: {ship['ship_date'] or '—'}\n",
        "*Позиции:*"
    ]
    for si in sitems:
        lines.append(f"• *{si['code']}* {si['name']}: *{si['quantity']:,.1f} {si['unit']}*")
    return "\n".join(lines), ik(
        [(f"✅ Подтвердить {ship['number']}", f"cs:ok:{sid}")],
        [("🔙 Назад","cs:list")],
    )

def _confirm_shipment(tid, sid, user):
    now = datetime.now(); conn = get_db()
    sitems = conn.execute(
        "SELECT si.*,oi.nomenclature_id,oi.order_id FROM shipment_items si "
        "JOIN order_items oi ON oi.id=si.order_item_id WHERE si.shipment_id=?", (sid,)
    ).fetchall()
    for si in sitems:
        conn.execute("UPDATE order_items SET shipped_qty=shipped_qty+? WHERE id=?", (si["quantity"],si["order_item_id"]))
    conn.execute("UPDATE shipments SET status='confirmed',confirmed_by=?,confirmed_at=? WHERE id=?", (user["id"],now,sid))
    ship = conn.execute("SELECT * FROM shipments WHERE id=?", (sid,)).fetchone()
    oid  = ship["order_id"]
    rem  = conn.execute("SELECT SUM(quantity-shipped_qty) FROM order_items WHERE order_id=?", (oid,)).fetchone()[0] or 0
    new_status = "shipped" if rem<=0 else "ready"
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status,oid))
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.commit(); conn.close()
    result = "полностью ✅" if new_status=="shipped" else "частично ⚠️"
    if creator:
        try: bot.send_message(creator["telegram_id"],
            f"📦 *{ship['number']}* подтверждена!\nЗаказ {order['number']} отгружен {result}", parse_mode="Markdown")
        except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# ПРОИЗВОДСТВО ЗА ДЕНЬ
# ═══════════════════════════════════════════════════════════════════════════════

def _start_production(tid):
    today = datetime.now().strftime("%d.%m.%Y")
    user_states[tid] = "prod"; user_data[tid] = {"date":today,"items":[]}
    _show_prod_picker(tid)

def _show_prod_picker(tid):
    d = user_data.get(tid,{}); today = d.get("date","")
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    added = d.get("items",[])
    text = f"📊 *Производство за {today}*\n"
    if added:
        text += "\n*Введено:*\n" + "\n".join(f"• {it['name']}: {it['qty']:,.1f} {it['unit']}" for it in added) + "\n"
    text += "\nВыберите позицию:"
    rows = []
    for i in range(0, len(items), 2):
        row = [((items[i]["code"] or "—")+" "+items[i]["name"][:18], f"pd:n:{items[i]['id']}")]
        if i+1 < len(items): row.append(((items[i+1]["code"] or "—")+" "+items[i+1]["name"][:18], f"pd:n:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("✅ Сохранить","pd:done")])
    bot.send_message(tid, text, reply_markup=ik(*rows), parse_mode="Markdown")

def _prod_item_selected(tid, nom_id):
    conn = get_db(); nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"] = {"id":nom_id,"name":nom["name"],"unit":nom["unit"]}
    user_states[tid] = "prod:qty"
    bot.send_message(tid, f"Введите количество *{nom['name']}* ({nom['unit']}):", parse_mode="Markdown")

def _prod_done(call):
    user = get_user(call.from_user.id); d = user_data.get(call.from_user.id,{})
    items = d.get("items",[]); ans(call)
    cancel_state(call.from_user.id)
    if not items: bot.send_message(call.from_user.id, "Ничего не введено."); return
    today = d.get("date","")
    lines = [f"✅ *Производство за {today}:*\n"]
    for it in items: lines.append(f"• {it['name']}: {it['qty']:,.1f} {it['unit']}")
    bot.send_message(call.from_user.id, "\n".join(lines), parse_mode="Markdown")
    notify_supervisors("\n".join(lines))

# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ ТЕКСТОВОГО ВВОДА
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.from_user.id in user_states)
def universal_text_handler(message):
    tid   = message.from_user.id
    state = user_states.get(tid,"")
    text  = message.text.strip()
    user  = get_user(tid)
    if not user: return

    # ── Заявки ──────────────────────────────────────────────────────────────
    if state.startswith("rq:"):
        rtype = state.replace("rq:","")
        labels = {"absence":"📅 Не выйдет на работу","breakdown":"🔧 Неисправность","mts":"📦 МТС"}
        now = datetime.now(); cancel_state(tid)
        conn = get_db()
        conn.execute("INSERT INTO requests (user_id,type,text,created_at) VALUES (?,?,?,?)", (user["id"],rtype,text,now))
        conn.commit(); conn.close()
        bot.send_message(tid, "✅ Заявка отправлена!")
        notify_supervisors(f"📋 *{labels.get(rtype,rtype)}*\n👤 *{user['name']}*\n🕐 {now.strftime('%d.%m.%Y %H:%M')}\n\n📝 {text}")
        return

    # ── Дата заказа ─────────────────────────────────────────────────────────
    if state == "ord:date":
        user_data[tid]["desired_date"] = None if text in ("-","пропустить") else text
        user_states[tid] = "ord:items"
        _show_item_picker(tid); return

    # ── Количество в заказе ─────────────────────────────────────────────────
    if state == "ord:qty":
        try: qty=float(text.replace(",",".")); assert qty>0
        except: bot.send_message(tid,"Введите число больше нуля."); return
        d=user_data[tid]; nom=d.pop("current_nom")
        d["items"].append({"nom_id":nom["id"],"name":nom["name"],"unit":nom["unit"],"code":(nom["code"] or "—"),"qty":qty})
        user_states[tid]="ord:items"; bot.send_message(tid,f"✅ {nom['name']} — {qty:,.1f} {nom['unit']}")
        _show_item_picker(tid); return

    # ── Дата отгрузки ───────────────────────────────────────────────────────
    if state.startswith("ship:") and state.endswith(":date"):
        oid = int(state.split(":")[1])
        ship_date = datetime.now().strftime("%d.%m.%Y") if text=="-" else text
        user_data[tid]["ship_date"] = ship_date
        items = user_data[tid]["items"]
        user_states[tid] = f"ship:{oid}:0"
        it = items[0]; rem = it["quantity"]-it["shipped_qty"]
        bot.send_message(tid,
            f"Позиция 1/{len(items)}: *{(it['code'] or '—')} {it['name']}*\nОстаток: {rem:,.1f} {it['unit']}\n\nВведите количество:",
            parse_mode="Markdown"); return

    # ── Количество в отгрузке ───────────────────────────────────────────────
    if state.startswith("ship:") and not state.endswith(":date"):
        parts=state.split(":"); oid=int(parts[1]); idx=int(parts[2])
        d=user_data.get(tid,{}); items=d.get("items",[])
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число (0 чтобы пропустить)."); return
        it=items[idx]; rem=it["quantity"]-it["shipped_qty"]; qty=min(qty,rem)
        d["ship_qtys"].append({"order_item_id":it["id"],"qty":qty,"name":it["name"],"unit":it["unit"],"code":it["code"]})
        next_idx=idx+1
        if next_idx<len(items):
            user_states[tid]=f"ship:{oid}:{next_idx}"
            nit=items[next_idx]; nr=nit["quantity"]-nit["shipped_qty"]
            bot.send_message(tid,
                f"Позиция {next_idx+1}/{len(items)}: *{nit['code']} {nit['name']}*\nОстаток: {nr:,.1f} {nit['unit']}\n\nВведите количество:",
                parse_mode="Markdown")
        else:
            now=datetime.now(); number=next_shipment_number()
            conn=get_db()
            conn.execute("INSERT INTO shipments (number,order_id,created_by,created_at,ship_date,status) VALUES (?,?,?,?,?,'pending')",
                         (number,oid,user["id"],now,d.get("ship_date")))
            sid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            for sq in d["ship_qtys"]:
                if sq["qty"]>0:
                    conn.execute("INSERT INTO shipment_items (shipment_id,order_item_id,quantity) VALUES (?,?,?)", (sid,sq["order_item_id"],sq["qty"]))
            conn.execute("UPDATE orders SET status='shipping' WHERE id=?", (oid,))
            conn.commit(); conn.close()
            cancel_state(tid)
            lines=[f"✅ *{number}* создана!\n📅 {d.get('ship_date','')}\n\n*Позиции:*"]
            for sq in d["ship_qtys"]:
                if sq["qty"]>0: lines.append(f"• {(sq['code'] or '—')} {sq['name']}: {sq['qty']:,.1f} {sq['unit']}")
            lines.append("\n⏳ Ожидает подтверждения Начальника Цеха.")
            bot.send_message(tid, "\n".join(lines), parse_mode="Markdown")
            notify_role("manager", "\n".join(lines[:-1]))
        return

    # ── Производство: количество ────────────────────────────────────────────
    if state == "prod:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; nom=d.pop("current_nom"); today=d["date"]; now=datetime.now()
        conn=get_db()
        ex=conn.execute("SELECT * FROM daily_production WHERE date=? AND nomenclature_id=? AND recorded_by=?",
                        (today,nom["id"],user["id"])).fetchone()
        if ex: conn.execute("UPDATE daily_production SET quantity=quantity+?,recorded_at=? WHERE id=?", (qty,now,ex["id"]))
        else:  conn.execute("INSERT INTO daily_production (date,nomenclature_id,quantity,recorded_by,recorded_at) VALUES (?,?,?,?,?)",
                            (today,nom["id"],qty,user["id"],now))
        updated=conn.execute(
            "SELECT dp.*,n.name,n.unit FROM daily_production dp JOIN nomenclature n ON n.id=dp.nomenclature_id WHERE dp.date=? AND dp.recorded_by=?",
            (today,user["id"])
        ).fetchall(); conn.commit(); conn.close()
        d["items"]=[{"name":e["name"],"unit":e["unit"],"qty":e["quantity"]} for e in updated]
        user_states[tid]="prod"
        bot.send_message(tid, f"✅ {nom['name']}: {qty:,.1f} {nom['unit']}")
        _show_prod_picker(tid); return

    # ── Инвентаризация: начальный остаток ───────────────────────────────────
    if state == "inv:init:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]
        conn=get_db()
        conn.execute("UPDATE nomenclature SET initial_stock=? WHERE id=?", (qty,d["nom_id"]))
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid, f"✅ Начальный остаток *{d['nom_name']}*: {qty:,.1f} {d['unit']}", parse_mode="Markdown"); return

    # ── Инвентаризация: корректировка ───────────────────────────────────────
    if state == "inv:adj:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; adj_type=d["adj_type"]; nom_id=d["nom_id"]
        conn=get_db()
        nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
        cur=get_stock(nom_id)
        if adj_type=="set":
            # Установить точное значение — добавляем разницу
            diff=qty-cur
            atype="add" if diff>=0 else "sub"
            aval=abs(diff)
        elif adj_type=="add":
            atype="add"; aval=qty
        else:
            atype="sub"; aval=qty
        conn.execute("INSERT INTO stock_adjustments (nomenclature_id,quantity,type,comment,created_by) VALUES (?,?,?,?,?)",
                     (nom_id,aval,atype,"Инвентаризация",user["id"]))
        conn.commit(); conn.close()
        new_stock=get_stock(nom_id)
        cancel_state(tid)
        bot.send_message(tid,
            f"✅ Остаток *{nom['name']}* скорректирован:\nБыло: {cur:,.1f} → Стало: *{new_stock:,.1f} {nom['unit']}*",
            parse_mode="Markdown"); return

    # ── Добавление номенклатуры ─────────────────────────────────────────────
    if state == "nm:add:code":
        user_data[tid]["code"]=text; user_states[tid]="nm:add:name"
        bot.send_message(tid, "Введите название:"); return
    if state == "nm:add:name":
        user_data[tid]["name"]=text; user_states[tid]="nm:add:unit"
        bot.send_message(tid, "Введите единицу измерения (м, шт, кг...):"); return
    if state == "nm:add:unit":
        user_data[tid]["unit"]=text; user_states[tid]="nm:add:notes"
        bot.send_message(tid, "Введите примечание (или *—* чтобы пропустить):", parse_mode="Markdown"); return
    if state == "nm:add:notes":
        d=user_data[tid]; notes=None if text=="-" else text
        conn=get_db()
        try:
            conn.execute("INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)", (d["code"],d["name"],d["unit"],notes))
            conn.commit()
            bot.send_message(tid, f"✅ Добавлено: *{d['name']}* ({d['code']})", parse_mode="Markdown")
        except sqlite3.IntegrityError:
            bot.send_message(tid, f"❌ Код *{d['code']}* уже существует.", parse_mode="Markdown")
        finally: conn.close()
        cancel_state(tid); return

    # ── Редактирование номенклатуры ─────────────────────────────────────────
    if state.startswith("nm:edit:"):
        parts=state.split(":"); nom_id=int(parts[2]); field=parts[3]
        conn=get_db()
        conn.execute(f"UPDATE nomenclature SET {field}=? WHERE id=?", (text,nom_id))
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid, f"✅ Обновлено!"); return

    # ── Добавление контрагента ──────────────────────────────────────────────
    if state == "cp:add:code":
        user_data[tid]["code"]=text; user_states[tid]="cp:add:name"
        bot.send_message(tid, "Введите название контрагента:"); return
    if state == "cp:add:name":
        user_data[tid]["name"]=text; user_states[tid]="cp:add:phone"
        bot.send_message(tid, "Введите телефон (или *—* чтобы пропустить):", parse_mode="Markdown"); return
    if state == "cp:add:phone":
        user_data[tid]["phone"]=None if text=="-" else text; user_states[tid]="cp:add:email"
        bot.send_message(tid, "Введите email (или *—* чтобы пропустить):", parse_mode="Markdown"); return
    if state == "cp:add:email":
        user_data[tid]["email"]=None if text=="-" else text; user_states[tid]="cp:add:address"
        bot.send_message(tid, "Введите адрес отгрузки (или *—* чтобы пропустить):", parse_mode="Markdown"); return
    if state == "cp:add:address":
        user_data[tid]["address"]=None if text=="-" else text; user_states[tid]="cp:add:notes"
        bot.send_message(tid, "Введите примечание (или *—* чтобы пропустить):", parse_mode="Markdown"); return
    if state == "cp:add:notes":
        d=user_data[tid]; notes=None if text=="-" else text
        conn=get_db()
        try:
            conn.execute("INSERT INTO counterparties (code,name,phone,email,address,notes) VALUES (?,?,?,?,?,?)",
                         (d["code"],d["name"],d.get("phone"),d.get("email"),d.get("address"),notes))
            conn.commit()
            bot.send_message(tid, f"✅ Контрагент добавлен: *{d['name']}*", parse_mode="Markdown")
        except sqlite3.IntegrityError:
            bot.send_message(tid, f"❌ Код *{d['code']}* уже существует.", parse_mode="Markdown")
        finally: conn.close()
        cancel_state(tid); return

    # ── Редактирование контрагента ──────────────────────────────────────────
    if state.startswith("cp:edit:"):
        parts=state.split(":"); cp_id=int(parts[2]); field=parts[3]
        conn=get_db()
        conn.execute(f"UPDATE counterparties SET {field}=? WHERE id=?", (text,cp_id))
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid, f"✅ Обновлено!"); return

# ═══════════════════════════════════════════════════════════════════════════════
# ЗАЯВКИ — ВСПОМОГАТЕЛЬНАЯ
# ═══════════════════════════════════════════════════════════════════════════════

def _start_request(tid, rtype):
    prompts={"absence":"📅 Укажи дату и причину:","breakdown":"🔧 Опиши неисправность:","mts":"📦 Опиши что нужно:"}
    user_states[tid]=f"rq:{rtype}"
    bot.send_message(tid, prompts[rtype]+"\n\n_/cancel для отмены_", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# НАПОМИНАНИЯ
# ═══════════════════════════════════════════════════════════════════════════════

def reminder_loop():
    while True:
        time.sleep(1800)
        try:
            conn=get_db(); now=datetime.now()
            rows=conn.execute("SELECT t.*,u.name,u.telegram_id FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active'").fetchall()
            for rec in rows:
                dt=datetime.fromisoformat(rec["check_in"]); count=rec["reminder_count"]
                last=datetime.fromisoformat(rec["last_reminder"]) if rec["last_reminder"] else None
                elapsed=(now-dt).total_seconds()
                if count==0 and elapsed>=3600:
                    bot.send_message(rec["telegram_id"],"⏰ *Напоминание:* ты не отметил уход.\nНажми '🚪 Ушёл с работы'",parse_mode="Markdown")
                    conn.execute("UPDATE time_records SET reminder_count=1,last_reminder=? WHERE id=?", (now,rec["id"])); conn.commit()
                elif count==1 and last and (now-last).total_seconds()>=3600:
                    bot.send_message(rec["telegram_id"],"⏰ *Второе напоминание:* отметь уход!",parse_mode="Markdown")
                    conn.execute("UPDATE time_records SET reminder_count=2,last_reminder=? WHERE id=?", (now,rec["id"])); conn.commit()
                elif count>=2 and last and (now-last).total_seconds()>=3600:
                    conn.execute("UPDATE time_records SET status='no_checkout',check_out=? WHERE id=?", (now,rec["id"])); conn.commit()
                    bot.send_message(rec["telegram_id"],"⚠️ Смена закрыта автоматически.")
                    notify_supervisors(f"⚠️ *{rec['name']}* не отметил уход. Смена с {dt.strftime('%H:%M %d.%m')}.")
            conn.close()
        except Exception as e: print(f"[reminder] {e}")

threading.Thread(target=reminder_loop, daemon=True).start()

print("Bot started!")
bot.infinity_polling()
