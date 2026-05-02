import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from telebot import TeleBot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = os.environ["BOT_TOKEN"]
bot = TeleBot(BOT_TOKEN)
DB_PATH = "/app/data/timetrack.db"

ROLE_LABELS = {
    "worker": "👷 Рабочий", "manager": "👔 Начальник Цеха",
    "admin": "⚙️ Администратор", "superadmin": "👑 Главный Админ",
}
SUPERVISOR_ROLES = ("manager", "admin", "superadmin")
ADMIN_MANAGED = ("worker", "manager")
SUPER_MANAGED = ("worker", "manager", "admin")
FINANCE_ROLES = ("admin", "superadmin")

ORDER_STATUS_LABELS = {
    "new": "🆕 Новый", "accepted": "✅ Принят",
    "in_progress": "🔧 В работе", "ready": "🏁 Готов",
    "shipping": "📦 На отгрузке", "shipped": "✅ Отгружен",
}

MONTHS_RU = {
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",
    5:"Май",6:"Июнь",7:"Июль",8:"Август",
    9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}

FIXED_EXPENSE_TYPES = [
    ("rent",    "🏠 Аренда"),
    ("utility", "💡 Коммунальные"),
    ("waste",   "🗑 Мусор"),
    ("salary",  "👷 Зарплата (фонд)"),
    ("other",   "📝 Прочие постоянные"),
]

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

INITIAL_USERS = [(915402089, "Katran 150", "superadmin", 0)]

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
            name TEXT NOT NULL, role TEXT NOT NULL,
            daily_rate REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS time_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL, check_in TIMESTAMP NOT NULL,
            check_out TIMESTAMP, status TEXT DEFAULT "active",
            reminder_count INTEGER DEFAULT 0, last_reminder TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL, type TEXT NOT NULL,
            text TEXT NOT NULL, created_at TIMESTAMP NOT NULL,
            status TEXT DEFAULT "new"
        );
        CREATE TABLE IF NOT EXISTS nomenclature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE, name TEXT NOT NULL, unit TEXT NOT NULL,
            notes TEXT, initial_stock REAL DEFAULT 0,
            cost_price REAL DEFAULT 0, sale_price REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nomenclature_id INTEGER NOT NULL,
            price_type TEXT NOT NULL,
            price REAL NOT NULL,
            changed_by INTEGER NOT NULL,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS stock_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nomenclature_id INTEGER NOT NULL, quantity REAL NOT NULL,
            type TEXT NOT NULL, comment TEXT, created_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS counterparties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE, name TEXT NOT NULL,
            phone TEXT, email TEXT, address TEXT, notes TEXT,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE NOT NULL,
            counterparty_id INTEGER, created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL, desired_date TEXT,
            status TEXT DEFAULT "new", order_type TEXT DEFAULT "production",
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS order_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL, nomenclature_id INTEGER NOT NULL,
            quantity REAL NOT NULL, shipped_qty REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE NOT NULL,
            order_id INTEGER, created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL, ship_date TEXT,
            confirmed_by INTEGER, confirmed_at TIMESTAMP,
            status TEXT DEFAULT "pending",
            shipment_type TEXT DEFAULT "order",
            notes TEXT, counterparty_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS shipment_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shipment_id INTEGER NOT NULL,
            order_item_id INTEGER,
            nomenclature_id INTEGER,
            quantity REAL NOT NULL,
            sale_price REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS daily_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL, nomenclature_id INTEGER NOT NULL,
            quantity REAL NOT NULL, recorded_by INTEGER NOT NULL,
            recorded_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fixed_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL, amount REAL NOT NULL,
            description TEXT, changed_by INTEGER NOT NULL,
            effective_from TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS variable_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL, amount REAL NOT NULL,
            description TEXT, expense_date TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS salary_bonuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            month TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    ''')

    if conn.execute("SELECT COUNT(*) FROM nomenclature").fetchone()[0] == 0:
        for i, (name, unit, notes) in enumerate(INITIAL_NOMENCLATURE, 1):
            code = f"НОМ-{i:03d}"
            conn.execute("INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)", (code,name,unit,notes))

    for tid, name, role, rate in INITIAL_USERS:
        ex = conn.execute("SELECT id FROM users WHERE telegram_id=?", (tid,)).fetchone()
        if ex: conn.execute("UPDATE users SET role=? WHERE telegram_id=?", (role,tid))
        else:  conn.execute("INSERT INTO users (telegram_id,name,role,daily_rate) VALUES (?,?,?,?)", (tid,name,role,rate))

    conn.commit()
    _migrate(conn)
    conn.close()
    print("✅ БД инициализирована")

def _migrate(conn):
    def cols(t):
        try: return [r[1] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()]
        except: return []
    migs = [
        ("users","daily_rate REAL DEFAULT 0"),
        ("nomenclature","cost_price REAL DEFAULT 0"),
        ("nomenclature","sale_price REAL DEFAULT 0"),
        ("nomenclature","initial_stock REAL DEFAULT 0"),
        ("orders","order_type TEXT DEFAULT 'production'"),
        ("shipments","shipment_type TEXT DEFAULT 'order'"),
        ("shipments","notes TEXT"),
        ("shipments","counterparty_id INTEGER"),
        ("shipment_items","order_item_id INTEGER"),
        ("shipment_items","nomenclature_id INTEGER"),
        ("shipment_items","sale_price REAL DEFAULT 0"),
        ("orders","notes TEXT"),
        ("orders","order_type TEXT DEFAULT 'production'"),
        ("salary_bonuses","user_id INTEGER"),
    ]
    for table, col_def in migs:
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

def notify_supervisors(text, **kwargs):
    if not is_work_time(): return
    conn = get_db()
    rows = conn.execute("SELECT telegram_id FROM users WHERE role IN ('manager','admin','superadmin')").fetchall()
    conn.close()
    for r in rows:
        try: bot.send_message(r["telegram_id"], text, parse_mode="Markdown", **kwargs)
        except: pass

def notify_roles(roles, text, **kwargs):
    if not is_work_time(): return
    conn = get_db()
    ph = ",".join("?"*len(roles))
    rows = conn.execute(f"SELECT telegram_id FROM users WHERE role IN ({ph})", roles).fetchall()
    conn.close()
    for r in rows:
        try: bot.send_message(r["telegram_id"], text, parse_mode="Markdown", **kwargs)
        except: pass

def get_stock(nom_id):
    conn = get_db()
    nom  = conn.execute("SELECT initial_stock FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
    init = (nom["initial_stock"] or 0) if nom else 0
    prod = conn.execute("SELECT COALESCE(SUM(quantity),0) FROM daily_production WHERE nomenclature_id=?", (nom_id,)).fetchone()[0]
    adj  = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN type='add' THEN quantity ELSE -quantity END),0) FROM stock_adjustments WHERE nomenclature_id=?",
        (nom_id,)
    ).fetchone()[0]
    ship = conn.execute(
        "SELECT COALESCE(SUM(si.quantity),0) FROM shipment_items si "
        "JOIN shipments s ON s.id=si.shipment_id "
        "WHERE s.status='confirmed' AND ("
        "  si.nomenclature_id=? OR "
        "  EXISTS(SELECT 1 FROM order_items oi WHERE oi.id=si.order_item_id AND oi.nomenclature_id=?))",
        (nom_id, nom_id)
    ).fetchone()[0]
    conn.close()
    return init + prod + adj - ship

def next_nom_code():
    conn = get_db()
    last = conn.execute("SELECT code FROM nomenclature WHERE code LIKE 'НОМ-%' ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    if last:
        try: num = int(last["code"].split("-")[1]) + 1
        except: num = 1
    else: num = 1
    return f"НОМ-{num:03d}"

def next_cp_code():
    conn = get_db()
    last = conn.execute("SELECT code FROM counterparties WHERE code LIKE 'БОТ-%' ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    if last:
        try: num = int(last["code"].split("-")[1]) + 1
        except: num = 1
    else: num = 1
    return f"БОТ-{num:03d}"

def next_order_number():
    conn = get_db(); year = datetime.now().year
    last = conn.execute("SELECT number FROM orders WHERE number LIKE ? ORDER BY id DESC LIMIT 1", (f"{year}-%",)).fetchone()
    conn.close()
    num = int(last["number"].split("-")[1]) + 1 if last else 1
    return f"{year}-{num:03d}"

def next_shipment_number():
    conn = get_db(); year = datetime.now().year
    last = conn.execute("SELECT number FROM shipments WHERE number LIKE ? ORDER BY id DESC LIMIT 1", (f"ОТГ-{year}-%",)).fetchone()
    conn.close()
    num = int(last["number"].split("-")[2]) + 1 if last else 1
    return f"ОТГ-{year}-{num:03d}"

def fmt_dt(dt_str):
    if not dt_str: return "—"
    try: return datetime.fromisoformat(dt_str).strftime("%d.%m.%Y %H:%M")
    except: return str(dt_str)

def c(val): return val or "—"

def cancel_state(tid):
    user_states.pop(tid, None); user_data.pop(tid, None)

def esc(s):
    """Экранирует спецсимволы Markdown"""
    if not s: return ""
    for ch in ["*","_","`","[","]","(",")"]:
        s = str(s).replace(ch, "\\" + ch)
    return s

def is_work_time():
    """Проверяет: сейчас рабочее время 8:00-20:00"""
    h = datetime.now().hour
    return 8 <= h < 20

def safe_notify(tid, text, **kwargs):
    """Отправляет уведомление только в рабочее время"""
    if is_work_time():
        try: bot.send_message(tid, text, **kwargs)
        except: pass

def get_current_month_range():
    now = datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start.strftime("%d.%m.%Y"), now.strftime("%d.%m.%Y"), MONTHS_RU[now.month], now.year

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

# Меню: Сотрудники
def staff_menu_kb(role):
    rows = [
        [("📊 Отчёт по сотрудникам", "rp:staff")],
        [("👷 Кто сейчас на смене", "shift:now")],
        [("👥 Список сотрудников", "usr:list")],
    ]
    if role in ("admin","superadmin"):
        rows.append([("➕ Добавить сотрудника","usr:add"), ("❌ Удалить сотрудника","usr:del:list")])
    rows.append([("❓ Справка","help")])
    return ik(*rows)

# Меню: Производство (верхний уровень)
def prod_top_kb():
    return ik(
        [("💼 Продажи","mn:sales")],
        [("🏭 Склад","mn:warehouse")],
        [("⚙️ Производство","mn:production")],
        [("📊 Отчёты","mn:reports")],
        [("💰 Финансы","mn:finance")],
    )

# Меню: Продажи
def sales_menu_kb(role):
    rows = [
        [("📦 Реестр заказов","ord:list")],
        [("🚚 Реестр отгрузок","ship:list")],
    ]
    if role in ("admin","superadmin"):
        rows.append([("➕ Заказ на производство","ord:new")])
        rows.append([("📤 Прямая отгрузка","ds:start")])
        rows.append([("📋 Номенклатура","nm:list"), ("👥 Контрагенты","cp:list")])
    rows.append([("🔙 Назад","mn:prod_top")])
    return ik(*rows)

# Меню: Склад
def warehouse_menu_kb():
    return ik(
        [("📊 Текущие остатки","wh")],
        [("⚙️ Инвентаризация","inv:menu")],
        [("🔙 Назад","mn:prod_top")],
    )

# Меню: Производство
def production_menu_kb():
    return ik(
        [("📊 Производство за день","pd:start")],
        [("✅ Подтвердить отгрузку","cs:list")],
        [("🔙 Назад","mn:prod_top")],
    )

# Меню: Отчёты
def reports_menu_kb():
    return ik(
        [("📊 Отчёт по сотрудникам","rp:staff")],
        [("📈 Производство по дням","rp:prod_days")],
        [("💰 Реестр зарплат за год","rp:salary_year")],
        [("🔙 Назад","mn:prod_top")],
    )

# Меню: Финансы
def finance_menu_kb(role):
    rows = [
        [("📋 Реестр расходов","exp:list")],
        [("📊 Финансовый отчёт","fin:report")],
        [("💰 Зарплата за месяц","sal:report")],
    ]
    if role in FINANCE_ROLES:
        rows.append([("💸 Постоянные расходы","exp:fixed:list")])
        rows.append([("➕ Переменный расход","exp:var:add")])
        rows.append([("➕ Доплата сотруднику","sal:bonus:start")])
    rows.append([("🔙 Назад","mn:prod_top")])
    return ik(*rows)

# ═══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def cmd_start(message):
    cancel_state(message.from_user.id)
    user = get_user(message.from_user.id)
    if user: send_menu(message.from_user.id, user["role"], user["name"])
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
    name  = f"{first} {last}".strip() or f"Admin_{message.from_user.id}"
    if conn.execute("SELECT id FROM users WHERE telegram_id=?", (message.from_user.id,)).fetchone():
        conn.execute("UPDATE users SET role='superadmin',name=? WHERE telegram_id=?", (name,message.from_user.id))
    else:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,'superadmin')", (message.from_user.id,name))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"✅ Вы — *Главный Админ*!\nИмя: *{name}*", parse_mode="Markdown")
    send_menu(message.from_user.id, "superadmin", name)

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
    if not target or target["role"] != "worker":
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
    if active: bot.send_message(message.from_user.id, f"⚠️ Уже на работе с {fmt_dt(active['check_in'])}."); conn.close(); return
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
    kb = ik(
        [("📅 Не выйду на работу","rq:abs")],
        [("🔧 Неисправность оборудования","rq:brk")],
        [("📦 Заявка на МТС","rq:mts")],
        [("❓ Справка","help")],
    )
    bot.send_message(message.from_user.id, "📋 *Заявки и справка:*", reply_markup=kb, parse_mode="Markdown")

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
    bot.send_message(message.from_user.id, "📦 *Производство и продажи*",
                     reply_markup=prod_top_kb(), parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# ИНЛАЙН КОЛБЭКИ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    user = get_user(call.from_user.id)
    if not user: ans(call,"⛔ Не зарегистрированы.",True); return
    cd = call.data

    # Навигация
    if cd == "mn:back":
        ans(call); cancel_state(call.from_user.id)
        send_menu(call.from_user.id, user["role"], user["name"]); return
    if cd == "mn:prod_top": ans(call); edit(call,"📦 *Производство и продажи*",prod_top_kb()); return
    if cd == "mn:sales":    ans(call); edit(call,"💼 *Продажи*",sales_menu_kb(user["role"])); return
    if cd == "mn:warehouse":ans(call); edit(call,"🏭 *Склад*",warehouse_menu_kb()); return
    if cd == "mn:production":ans(call); edit(call,"⚙️ *Производство*",production_menu_kb()); return
    if cd == "mn:reports":  ans(call); edit(call,"📊 *Отчёты*",reports_menu_kb()); return
    if cd == "mn:finance":  ans(call); edit(call,"💰 *Финансы*",finance_menu_kb(user["role"])); return

    # Отчёты
    if cd == "rp:staff":        ans(call); edit(call,_gen_staff_report()); return
    if cd == "rp:prod_days":    ans(call); edit(call,_prod_days_report()); return
    if cd == "rp:salary_year":  ans(call); edit(call,_salary_year_report()); return
    if cd == "sal:report":      ans(call); edit(call,_salary_month_report()); return
    if cd == "sal:bonus:start": ans(call); _start_salary_bonus(call.from_user.id); return
    if cd.startswith("sal:user:"):
        uid=int(cd.split(":")[2]); ans(call); _sal_user_selected(call.from_user.id,uid); return
    if cd == "shift:now":    ans(call); edit(call,_who_on_shift()); return

    # Сотрудники
    if cd == "usr:list": ans(call); edit(call,_staff_list(user["role"])); return
    if cd == "usr:add":  ans(call); _start_usr_add(call.from_user.id); return
    if cd == "usr:del:list": ans(call); t,k=_usr_del_list(user["role"]); edit(call,t,k); return
    if cd.startswith("usr:del:"):
        tid_del=int(cd.split(":")[2]); ans(call)
        _delete_user(call.from_user.id,tid_del,user); t,k=_usr_del_list(user["role"]); edit(call,t,k); return
    if cd.startswith("usr:role:"):
        role_sel=cd.split(":")[2]; d=user_data.get(call.from_user.id,{})
        new_tid=d.get("new_tid"); name=d.get("name",""); ans(call)
        if not new_tid or not name: cancel_state(call.from_user.id); return
        conn=get_db()
        try:
            conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (new_tid,name,role_sel))
            conn.commit()
            bot.send_message(call.from_user.id,f"✅ *{name}* — {ROLE_LABELS[role_sel]}", parse_mode="Markdown")
            try: bot.send_message(new_tid,f"✅ Вы зарегистрированы как *{name}*.\nНажмите /start", parse_mode="Markdown")
            except: pass
        except sqlite3.IntegrityError:
            bot.send_message(call.from_user.id,"⚠️ Пользователь уже есть в системе.")
        finally: conn.close()
        cancel_state(call.from_user.id)
        bot.send_message(call.from_user.id,"Вернуться:", reply_markup=ik([("👥 К списку сотрудников","usr:list")],[("🏠 Главное меню","mn:back")]))
        return

    # Справка
    if cd == "help": ans(call); edit(call,_help_text(user["role"])); return

    # Заявки
    if cd == "rq:abs": ans(call); _start_request(call.from_user.id,"absence"); return
    if cd == "rq:brk": ans(call); _start_request(call.from_user.id,"breakdown"); return
    if cd == "rq:mts": ans(call); _start_request(call.from_user.id,"mts"); return

    # Склад
    if cd == "wh": ans(call); edit(call,_warehouse_text()); return

    # Номенклатура
    if cd == "nm:list": ans(call); edit(call,_nom_list_text(),_nom_registry_kb(user["role"])); return
    if cd == "nm:add":  ans(call); _start_nom_add(call.from_user.id); return
    if cd.startswith("nm:v:"):
        nom_id=int(cd.split(":")[2]); ans(call); t,k=_nom_detail(nom_id,user["role"]); edit(call,t,k); return
    if cd.startswith("nm:edit:"):
        parts=cd.split(":"); nom_id=int(parts[2]); field=parts[3]; ans(call)
        _start_nom_edit(call.from_user.id,nom_id,field); return
    if cd.startswith("nm:price:"):
        parts=cd.split(":"); nom_id=int(parts[2]); ptype=parts[3]; ans(call)
        _start_price_change(call.from_user.id,nom_id,ptype); return
    if cd.startswith("nm:ph:"):
        nom_id=int(cd.split(":")[2]); ans(call); edit(call,_price_history(nom_id)); return

    # Контрагенты
    if cd == "cp:list": ans(call); edit(call,_cp_list_text(),_cp_registry_kb()); return
    if cd.startswith("cp:del:") and "confirm" not in cd:
        cp_id=int(cd.split(":")[2]); ans(call)
        conn=get_db(); cp=conn.execute("SELECT name FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
        name=cp["name"] if cp else cp_id
        edit(call,f"🗑 Удалить контрагента *{esc(name)}*?",
             ik([(f"✅ Да, удалить",f"cp:del:confirm:{cp_id}"),(f"❌ Отмена",f"cp:v:{cp_id}")])); return
    if cd.startswith("cp:del:confirm:"):
        cp_id=int(cd.split(":")[3]); ans(call)
        conn=get_db(); conn.execute("UPDATE counterparties SET active=0 WHERE id=?", (cp_id,)); conn.commit(); conn.close()
        edit(call,"✅ Контрагент удалён.",ik([("🔙 К контрагентам","cp:list")])); return
    if cd == "cp:add":  ans(call); _start_cp_add(call.from_user.id); return
    if cd.startswith("cp:v:"):
        cp_id=int(cd.split(":")[2]); ans(call); t,k=_cp_detail(cp_id,user["role"]); edit(call,t,k); return
    if cd.startswith("cp:edit:"):
        parts=cd.split(":"); cp_id=int(parts[2]); field=parts[3]; ans(call)
        _start_cp_edit(call.from_user.id,cp_id,field); return

    # Инвентаризация
    if cd == "inv:menu": ans(call); t,k=_inv_menu(); edit(call,t,k); return
    if cd == "inv:init": ans(call); _start_inv_init(call.from_user.id); return
    if cd == "inv:adj":  ans(call); _start_inv_adj(call.from_user.id); return
    if cd.startswith("inv:n:"):
        nom_id=int(cd.split(":")[2]); ans(call); _inv_nom_selected(call.from_user.id,nom_id); return
    if cd.startswith("inv:adj:n:"):
        nom_id=int(cd.split(":")[3]); ans(call); _inv_adj_nom_selected(call.from_user.id,nom_id); return
    if cd.startswith("inv:adj:add:") or cd.startswith("inv:adj:sub:") or cd.startswith("inv:adj:set:"):
        parts=cd.split(":"); action=parts[2]; nom_id=int(parts[3])
        user_data[call.from_user.id]["adj_type"]=action; user_data[call.from_user.id]["nom_id"]=nom_id
        user_states[call.from_user.id]="inv:adj:qty"; ans(call)
        labels={"add":"прибавить","sub":"убрать","set":"установить точное значение"}
        bot.send_message(call.from_user.id,f"Введите количество ({labels[action]}):\n_/cancel для отмены_",parse_mode="Markdown"); return

    # Заказы
    if cd == "ord:list": ans(call); t,k=_orders_list_view(user["role"]); edit(call,t,k); return
    if cd == "ord:new":  ans(call); _start_new_order(call.from_user.id); return
    if cd.startswith("ord:v:"):
        oid=int(cd.split(":")[2]); ans(call); t,k=_order_detail_view(oid,user["role"]); edit(call,t,k); return
    if cd.startswith("ord:edit:"):
        parts=cd.split(":"); oid=int(parts[2]); field=parts[3]; ans(call)
        if user["role"] not in ("admin","superadmin"): ans(call,"⛔ Нет доступа.",True); return
        labels={"desired_date":"желаемую дату готовности (ДД.ММ.ГГГГ)","notes":"примечание к заказу"}
        user_states[call.from_user.id]=f"ord:edit:{oid}:{field}"
        bot.send_message(call.from_user.id,f"✏️ Введите {labels.get(field,field)}:\n_/cancel для отмены_",parse_mode="Markdown"); return
    if cd.startswith("ord:del:") and "confirm" not in cd:
        oid=int(cd.split(":")[2]); ans(call)
        if user["role"] not in ("admin","superadmin"): ans(call,"⛔ Нет доступа.",True); return
        conn=get_db(); o=conn.execute("SELECT number FROM orders WHERE id=?", (oid,)).fetchone(); conn.close()
        num=o["number"] if o else oid
        edit(call,f"🗑 Удалить заказ *{num}*?\n\nЭто действие нельзя отменить!",
             ik([(f"✅ Да, удалить",f"ord:del:confirm:{oid}"),(f"❌ Отмена",f"ord:v:{oid}")])); return
    if cd.startswith("ord:del:confirm:"):
        oid=int(cd.split(":")[3]); ans(call)
        if user["role"] not in ("admin","superadmin"): ans(call,"⛔ Нет доступа.",True); return
        conn=get_db()
        conn.execute("DELETE FROM order_items WHERE order_id=?", (oid,))
        conn.execute("DELETE FROM orders WHERE id=?", (oid,))
        conn.commit(); conn.close()
        edit(call,"✅ Заказ удалён.",ik([("🔙 К заказам","ord:list")])); return
    if cd.startswith("ord:s:"):
        parts=cd.split(":"); oid=int(parts[2]); new_st=parts[3]; ans(call)
        _change_order_status(call.from_user.id,oid,new_st,user); t,k=_order_detail_view(oid,user["role"]); edit(call,t,k); return
    if cd.startswith("ord:ship:"):
        oid=int(cd.split(":")[2]); ans(call); _start_shipment(call.from_user.id,oid); return
    if cd == "ord:save": _order_save(call,user); return
    if cd == "ord:note": ans(call); user_states[call.from_user.id]="ord:note"; user_data[call.from_user.id]["last_note_call"]=call; bot.send_message(call.from_user.id,"📝 Введите примечание:\n_/cancel для отмены_",parse_mode="Markdown"); return
    if cd.startswith("ord:comment:"):
        oid=int(cd.split(":")[2]); ans(call)
        user_states[call.from_user.id]=f"ord:addcomment:{oid}"
        bot.send_message(call.from_user.id,"💬 Введите комментарий к заказу:\n_/cancel для отмены_",parse_mode="Markdown"); return
    if cd.startswith("ord:waybill:"):
        sid=int(cd.split(":")[2]); ans(call)
        _generate_waybill_choice(call.from_user.id, sid); return
    if cd.startswith("ord:wb:xlsx:"):
        sid=int(cd.split(":")[2]); ans(call)
        _generate_waybill(call.from_user.id, sid, "xlsx"); return
    if cd.startswith("ord:wb:pdf:"):
        sid=int(cd.split(":")[2]); ans(call)
        _generate_waybill(call.from_user.id, sid, "pdf"); return
    if cd.startswith("cp:sel:"):
        cp_id=int(cd.split(":")[2]); ans(call); _order_cp_selected(call.from_user.id,cp_id); return
    if cd.startswith("ni:"):
        nom_id=int(cd.split(":")[1]); ans(call); _order_item_selected(call.from_user.id,nom_id,call); return

    # Реестр отгрузок
    if cd == "ship:list": ans(call); t,k=_shipments_list_view(); edit(call,t,k); return
    if cd.startswith("ship:v:"):
        sid=int(cd.split(":")[2]); ans(call); t,k=_shipment_view(sid,user["role"]); edit(call,t,k); return
    if cd.startswith("ship:edit:"):
        parts=cd.split(":"); sid=int(parts[2]); field=parts[3]; ans(call)
        if user["role"] not in ("admin","superadmin"): ans(call,"⛔ Нет доступа.",True); return
        labels={"ship_date":"дату отгрузки (ДД.ММ.ГГГГ)","notes":"примечание"}
        user_states[call.from_user.id]=f"ship:edit:{sid}:{field}"
        bot.send_message(call.from_user.id,f"✏️ Введите новое {labels.get(field,field)}:\n_/cancel для отмены_",parse_mode="Markdown"); return
    if cd.startswith("ship:del:"):
        sid=int(cd.split(":")[2]); ans(call)
        if user["role"] not in ("admin","superadmin"): ans(call,"⛔ Нет доступа.",True); return
        edit(call,f"🗑 Удалить отгрузку №{sid}?\n\nЭто действие нельзя отменить!",
             ik([(f"✅ Да, удалить",f"ship:del:confirm:{sid}"),(f"❌ Отмена","ship:list")])); return
    if cd.startswith("ship:del:confirm:"):
        sid=int(cd.split(":")[3]); ans(call)
        conn=get_db()
        conn.execute("DELETE FROM shipment_items WHERE shipment_id=?", (sid,))
        conn.execute("DELETE FROM shipments WHERE id=?", (sid,))
        conn.commit(); conn.close()
        edit(call,"✅ Отгрузка удалена.",ik([("🔙 К отгрузкам","ship:list")])); return

    # Прямая отгрузка
    if cd == "ds:start": ans(call); _start_direct_shipment(call.from_user.id); return
    if cd.startswith("ds:cp:"):
        cp_id=int(cd.split(":")[2]); ans(call); _ds_cp_selected(call.from_user.id,cp_id); return
    if cd.startswith("ds:ni:"):
        nom_id=int(cd.split(":")[2]); ans(call); _ds_item_selected(call.from_user.id,nom_id); return
    if cd == "ds:save":  _ds_save(call,user); return
    if cd == "ds:note":  ans(call); user_states[call.from_user.id]="ds:note"; bot.send_message(call.from_user.id,"Введите примечание:"); return
    if cd.startswith("ds:partial:"):
        ans(call); _ds_partial_action(call.from_user.id,user); return
    if cd.startswith("ds:wait:"):
        ans(call); cancel_state(call.from_user.id); bot.send_message(call.from_user.id,"⏳ Отгрузка отложена."); return
    if cd.startswith("ds:cancel_ship:"):
        ans(call); cancel_state(call.from_user.id); bot.send_message(call.from_user.id,"❌ Отгрузка отменена."); return

    # Подтверждение отгрузки
    if cd == "cs:list": ans(call); t,k=_pending_shipments_view(); edit(call,t,k); return
    if cd.startswith("cs:v:"):
        sid=int(cd.split(":")[2]); ans(call); t,k=_shipment_detail_view(sid); edit(call,t,k); return
    if cd.startswith("cs:ok:"):
        sid=int(cd.split(":")[2]); ans(call); _confirm_shipment(call.from_user.id,sid,user); edit(call,"✅ Отгрузка подтверждена!"); return

    # Производство за день
    if cd == "pd:start": ans(call); _start_production(call.from_user.id); return
    if cd.startswith("pd:n:"):
        nom_id=int(cd.split(":")[2]); ans(call); _prod_item_selected(call.from_user.id,nom_id); return
    if cd == "pd:done": _prod_done(call); return

    # Финансы — Постоянные расходы
    if cd == "exp:fixed:list": ans(call); t,k=_fixed_expenses_view(user["role"]); edit(call,t,k); return
    if cd.startswith("exp:fixed:edit:"):
        etype=cd.split(":")[3]; ans(call); _start_fixed_expense_edit(call.from_user.id,etype); return
    if cd.startswith("exp:fixed:hist:"):
        etype=cd.split(":")[3]; ans(call); edit(call,_fixed_expense_history(etype)); return

    # Финансы — Переменные расходы
    if cd == "exp:var:add": ans(call); _start_var_expense(call.from_user.id); return
    if cd.startswith("exp:var:cat:"):
        cat=cd.split(":")[3]; ans(call); _var_exp_cat_selected(call.from_user.id,cat); return

    # Финансы — Реестр и отчёт
    if cd == "exp:list":    ans(call); edit(call,_expenses_registry(),_expenses_registry_kb()); return
    if cd == "fin:report":  ans(call); edit(call,_financial_report()); return

    # Цена продажи при отгрузке
    if cd.startswith("sp:"):
        parts=cd.split(":"); nom_id=int(parts[1]); ans(call)
        _start_sale_price_input(call.from_user.id,nom_id); return

    ans(call)

# ═══════════════════════════════════════════════════════════════════════════════
# КОНТЕНТ
# ═══════════════════════════════════════════════════════════════════════════════

def _gen_staff_report():
    now = datetime.now(); wn = now.isocalendar()[1]
    conn = get_db()
    since_week  = now - timedelta(days=7)
    since_month = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    workers = conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall()
    if not workers: conn.close(); return "📊 Нет рабочих."
    lines = [f"📊 *Отчёт по сотрудникам*\n_{MONTHS_RU[now.month]} {now.year}_\n"]
    for w in workers:
        def calc(since):
            recs = conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=?", (w["id"],since)).fetchall()
            closed = [r for r in recs if r["status"]=="closed"]
            no_mrk = [r for r in recs if r["status"]=="no_checkout"]
            th = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in closed)
            dw = len({datetime.fromisoformat(r["check_in"]).date() for r in closed})
            ov = sum(max(0,(datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600-8) for r in closed)
            return dw, th, ov, len(no_mrk)
        wd,wh,wo,wn2 = calc(since_week); md,mh,mo,mn2 = calc(since_month)
        rate = w["daily_rate"] or 0; hourly = rate/8 if rate>0 else 0
        lines.append(f"👷 *{w['name']}*")
        lines.append(f"   Неделя: {wd} дн. | {wh:.1f} ч." + (f" (перераб.: {wo:.1f})" if wo>0 else ""))
        lines.append(f"   Месяц:  {md} дн. | {mh:.1f} ч." + (f" (перераб.: {mo:.1f})" if mo>0 else ""))
        if rate>0: lines.append(f"   💰 Месяц: {mh*hourly:,.0f} ₽")
        if mn2: lines.append(f"   ⚠️ Не отметил уход: {mn2} раз(а)")
        lines.append("")
    conn.close(); return "\n".join(lines)

def _prod_days_report():
    conn = get_db(); now = datetime.now()
    month_start = now.replace(day=1).strftime("%d.%m.%Y")
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall()
    lines = [f"📈 *Производство — {MONTHS_RU[now.month]} {now.year}*\n"]
    for it in items:
        rows = conn.execute(
            "SELECT date, SUM(quantity) as qty FROM daily_production "
            "WHERE nomenclature_id=? AND date>=? GROUP BY date ORDER BY date",
            (it["id"], month_start)
        ).fetchall()
        if not rows: continue
        total = sum(r["qty"] for r in rows)
        lines.append(f"📦 *{c(it['code'])} {it['name']}* ({it['unit']})")
        for r in rows: lines.append(f"   {r['date']}: {r['qty']:,.1f}")
        lines.append(f"   *Итого: {total:,.1f} {it['unit']}*\n")
    conn.close()
    return "\n".join(lines) if len(lines)>1 else f"📈 Данных за {MONTHS_RU[now.month]} нет."

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

def _usr_del_list(role):
    conn = get_db()
    allowed = SUPER_MANAGED if role=="superadmin" else ADMIN_MANAGED
    ph = ",".join("?"*len(allowed))
    users = conn.execute(f"SELECT * FROM users WHERE role IN ({ph}) ORDER BY name", allowed).fetchall()
    conn.close()
    if not users: return "Нет сотрудников для удаления.", ik([("🔙 Назад","usr:list")])
    lines = ["❌ *Выберите сотрудника:*\n"]
    rows = []
    for u in users:
        lines.append(f"• {ROLE_LABELS.get(u['role'],u['role'])}: *{u['name']}*")
        rows.append([(f"❌ {u['name']}", f"usr:del:{u['telegram_id']}")])
    rows.append([("🔙 Назад","usr:list")])
    return "\n".join(lines), ik(*rows)

def _delete_user(admin_tid, target_tid, admin_user):
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (target_tid,)).fetchone()
    if not target: conn.close(); bot.send_message(admin_tid,"❌ Не найден."); return
    allowed = SUPER_MANAGED if admin_user["role"]=="superadmin" else ADMIN_MANAGED
    if target["role"] not in allowed:
        conn.close(); bot.send_message(admin_tid,f"❌ Нельзя удалить {ROLE_LABELS.get(target['role'],'')}."); return
    conn.execute("DELETE FROM users WHERE telegram_id=?", (target_tid,)); conn.commit(); conn.close()
    bot.send_message(admin_tid, f"✅ *{target['name']}* удалён.", parse_mode="Markdown")

def _start_usr_add(tid):
    user_states[tid]="usr:add:id"; user_data[tid]={}
    bot.send_message(tid,"➕ *Добавить сотрудника*\n\nВведите Telegram ID:\n_/cancel для отмены_", parse_mode="Markdown")

def _help_text(role):
    texts = {
        "worker":     "📖 *Рабочий*\n\n✅/🚪 Приход и уход\n📈 Статистика\n📋 Заявки",
        "manager":    "📖 *Начальник Цеха*\n\n👥 Сотрудники\n📦 Производство — заказы, отгрузки, склад, производство за день",
        "admin":      "📖 *Администратор*\n\n*/setrate [id] [сумма]*\n💼 Продажи — заказы, отгрузки\n💰 Финансы — расходы",
        "superadmin": "📖 *Главный Админ*\n\nПолный доступ.\n*/setrate [id] [сумма]*",
    }
    return texts.get(role,"")

def _nom_list_text():
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    lines = ["📋 *Номенклатура:*\n"]
    for it in items:
        stock = get_stock(it["id"]); icon = "✅" if stock>0 else "⚠️"
        lines.append(f"{icon} *{c(it['code'])}* | {it['name']} ({it['unit']}) | 🏭{stock:,.1f}")
    return "\n".join(lines)

def _nom_registry_kb(role):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    rows = []
    if role in ("admin","superadmin"): rows.append([("➕ Добавить","nm:add")])
    for it in items: rows.append([(f"{c(it['code'])} — {it['name']}", f"nm:v:{it['id']}")])
    rows.append([("🔙 Назад","mn:sales")])
    return ik(*rows)

def _nom_detail(nom_id, role):
    conn = get_db(); it = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not it: return "Не найдено.", None
    stock = get_stock(nom_id)
    lines = [
        f"📋 *{esc(it['name'])}*\n",
        f"🔑 Код: `{c(it['code'])}`",
        f"📦 Ед.: {it['unit']}",
        f"📝 Примечание: {it['notes'] or '—'}",
        f"🏭 Остаток: *{stock:,.1f} {it['unit']}*",
        f"💰 Себестоимость: *{(it['cost_price'] or 0):,.2f} ₽*",
        f"💵 Прайс: *{(it['sale_price'] or 0):,.2f} ₽*",
        f"📅 Добавлена: {fmt_dt(it['created_at'])}",
    ]
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([("✏️ Название",f"nm:edit:{nom_id}:name"),("✏️ Примечание",f"nm:edit:{nom_id}:notes")])
        kb_rows.append([("💰 Себестоимость",f"nm:price:{nom_id}:cost"),("💵 Прайс",f"nm:price:{nom_id}:sale")])
        kb_rows.append([("📜 История цен",f"nm:ph:{nom_id}"),("🗑 Удалить",f"nm:del:{nom_id}")])
    kb_rows.append([("🔙 Назад","nm:list")])
    return "\n".join(lines), ik(*kb_rows)

def _price_history(nom_id):
    conn = get_db()
    nom = conn.execute("SELECT name FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
    rows = conn.execute(
        "SELECT * FROM price_history WHERE nomenclature_id=? ORDER BY changed_at DESC LIMIT 20",
        (nom_id,)
    ).fetchall(); conn.close()
    if not rows: return "📜 История цен пуста."
    labels = {"cost":"Себестоимость","sale":"Прайс"}
    lines = [f"📜 *История цен — {nom['name'] if nom else '—'}*\n"]
    for r in rows:
        lines.append(f"• {labels.get(r['price_type'],r['price_type'])}: *{r['price']:,.2f} ₽* — {fmt_dt(r['changed_at'])}")
    return "\n".join(lines)

def _start_price_change(tid, nom_id, ptype):
    labels = {"cost":"себестоимость","sale":"прайсовую цену"}
    user_states[tid] = f"nm:price:{nom_id}:{ptype}"
    bot.send_message(tid,f"Введите новую {labels.get(ptype,'цену')} (₽):\n_/cancel для отмены_", parse_mode="Markdown")

def _cp_list_text():
    conn = get_db(); cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps: return "👥 Контрагентов нет."
    lines = ["👥 *Контрагенты:*\n"]
    for cp in cps: lines.append(f"• *{c(cp['code'])}* | *{cp['name']}*")
    return "\n".join(lines)

def _cp_registry_kb():
    conn = get_db(); cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    rows = [[("➕ Добавить контрагента","cp:add")]]
    for cp in cps: rows.append([(f"{c(cp['code'])} — {cp['name']}", f"cp:v:{cp['id']}")])
    rows.append([("🔙 Назад","mn:sales")])
    return ik(*rows)

def _cp_detail(cp_id, role):
    conn = get_db(); cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: return "Не найдено.", None
    lines = [
        f"👥 *{esc(cp['name'])}*\n",
        f"🔑 Код: `{c(cp['code'])}`",
        f"📞 {cp['phone'] or '—'}", f"📧 {cp['email'] or '—'}",
        f"📍 {cp['address'] or '—'}", f"📝 {cp['notes'] or '—'}",
        f"📅 {fmt_dt(cp['created_at'])}"
    ]
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([("✏️ Название",f"cp:edit:{cp_id}:name"),("✏️ Телефон",f"cp:edit:{cp_id}:phone")])
        kb_rows.append([("✏️ Email",f"cp:edit:{cp_id}:email"),("✏️ Адрес",f"cp:edit:{cp_id}:address")])
        kb_rows.append([("✏️ Примечание",f"cp:edit:{cp_id}:notes"),("🗑 Удалить",f"cp:del:{cp_id}")])
    kb_rows.append([("🔙 Назад","cp:list")])
    return "\n".join(lines), ik(*kb_rows)

def _warehouse_text():
    conn = get_db(); items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    lines = [f"🏭 *Текущие остатки на {datetime.now().strftime('%d.%m.%Y %H:%M')}:*\n"]
    for it in items:
        s = get_stock(it["id"]); icon = "✅" if s>0 else ("⚠️" if s==0 else "🔴")
        lines.append(f"{icon} *{it['name']}*: {s:,.1f} {it['unit']}")
    return "\n".join(lines)

def _inv_menu():
    return "⚙️ *Инвентаризация*", ik(
        [("📥 Начальные остатки","inv:init")],
        [("🔧 Корректировка","inv:adj")],
        [("🔙 Назад","mn:warehouse")],
    )

# ─── Финансы ──────────────────────────────────────────────────────────────────

def _fixed_expenses_view(role):
    conn = get_db()
    lines = ["💸 *Постоянные расходы:*\n"]
    rows_kb = []
    for etype, elabel in FIXED_EXPENSE_TYPES:
        last = conn.execute(
            "SELECT amount,effective_from FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 1",
            (etype,)
        ).fetchone()
        amount = last["amount"] if last else 0
        eff    = last["effective_from"] if last else "—"
        lines.append(f"{elabel}: *{amount:,.2f} ₽* (с {eff})")
        if role in FINANCE_ROLES:
            rows_kb.append([(f"✏️ {elabel} | {amount:,.2f} ₽", f"exp:fixed:edit:{etype}")])
    conn.close()
    rows_kb.append([("🔙 Назад","mn:finance")])
    return "\n".join(lines), ik(*rows_kb)

def _fixed_expense_history(etype):
    conn = get_db()
    label = dict(FIXED_EXPENSE_TYPES).get(etype, etype)
    rows = conn.execute(
        "SELECT * FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 15", (etype,)
    ).fetchall(); conn.close()
    if not rows: return f"📜 История *{label}* пуста."
    lines = [f"📜 *История: {label}*\n"]
    for r in rows:
        lines.append(f"• *{r['amount']:,.2f} ₽* с {r['effective_from']} (внесено {fmt_dt(r['created_at'])})")
        if r["description"]: lines.append(f"  📝 {r['description']}")
    return "\n".join(lines)

def _start_fixed_expense_edit(tid, etype):
    label = dict(FIXED_EXPENSE_TYPES).get(etype, etype)
    user_states[tid] = f"exp:fixed:{etype}:amount"
    user_data[tid]   = {"etype": etype, "elabel": label}
    bot.send_message(tid, f"💸 *{label}*\n\nВведите сумму (₽):\n_/cancel для отмены_", parse_mode="Markdown")

def _start_var_expense(tid):
    rows = [
        [("🛒 Материалы","exp:var:cat:Материалы"),("🚗 Транспорт","exp:var:cat:Транспорт")],
        [("🔧 Ремонт и обслуживание","exp:var:cat:Ремонт")],
        [("📦 Расходники","exp:var:cat:Расходники"),("🖨 Офис","exp:var:cat:Офис")],
        [("📝 Прочее","exp:var:cat:Прочее")],
        [("🔙 Назад","mn:finance")],
    ]
    user_states[tid]="exp:var:cat"; user_data[tid]={}
    bot.send_message(tid,"➕ *Переменный расход*\n\nВыберите категорию:", reply_markup=ik(*rows), parse_mode="Markdown")

def _var_exp_cat_selected(tid, cat):
    user_data[tid]["category"]=cat
    user_states[tid]="exp:var:amount"
    bot.send_message(tid,f"Категория: *{cat}*\n\nВведите сумму (₽):", parse_mode="Markdown")

def _expenses_registry():
    now = datetime.now()
    start, end, mname, year = get_current_month_range()
    conn = get_db()

    lines = [f"📋 *Реестр расходов — {mname} {year}*\n"]

    # Постоянные
    lines.append("*💸 Постоянные:*")
    total_fixed = 0
    for etype, elabel in FIXED_EXPENSE_TYPES:
        last = conn.execute(
            "SELECT amount FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 1", (etype,)
        ).fetchone()
        if last and last["amount"]>0:
            lines.append(f"  {elabel}: {last['amount']:,.2f} ₽")
            total_fixed += last["amount"]
    lines.append(f"  *Итого: {total_fixed:,.2f} ₽*\n")

    # Переменные
    lines.append("*💸 Переменные:*")
    var_rows = conn.execute(
        "SELECT * FROM variable_expenses WHERE expense_date>=? AND expense_date<=? ORDER BY expense_date DESC",
        (start, end)
    ).fetchall()
    total_var = 0
    for r in var_rows:
        lines.append(f"  {r['expense_date']} | {r['category']}: {r['amount']:,.2f} ₽")
        if r["description"]: lines.append(f"    📝 {r['description']}")
        total_var += r["amount"]
    lines.append(f"  *Итого: {total_var:,.2f} ₽*\n")

    total = total_fixed + total_var
    lines.append(f"💰 *Всего расходов: {total:,.2f} ₽*")

    conn.close()
    return "\n".join(lines)

def _expenses_registry_kb():
    return ik([("🔙 Назад","mn:finance")])

def _financial_report():
    now = datetime.now()
    start, end, mname, year = get_current_month_range()
    conn = get_db()

    # Отгрузки за месяц (подтверждённые)
    ships = conn.execute(
        "SELECT s.id FROM shipments s WHERE s.status='confirmed' AND s.created_at>=? AND s.created_at<=?",
        (now.replace(day=1,hour=0,minute=0,second=0,microsecond=0), now)
    ).fetchall()

    total_revenue = 0.0
    total_cost    = 0.0
    ship_lines    = []

    for s in ships:
        sitems = conn.execute(
            "SELECT si.quantity, si.sale_price, "
            "COALESCE(n.name,n2.name) as nom_name, "
            "COALESCE(n.cost_price,n2.cost_price,0) as cost_price "
            "FROM shipment_items si "
            "LEFT JOIN order_items oi ON oi.id=si.order_item_id "
            "LEFT JOIN nomenclature n ON n.id=oi.nomenclature_id "
            "LEFT JOIN nomenclature n2 ON n2.id=si.nomenclature_id "
            "WHERE si.shipment_id=?", (s["id"],)
        ).fetchall()
        for si in sitems:
            rev  = (si["sale_price"] or 0) * si["quantity"]
            cost = (si["cost_price"] or 0) * si["quantity"]
            total_revenue += rev
            total_cost    += cost

    # Расходы
    total_fixed = 0.0
    for etype, _ in FIXED_EXPENSE_TYPES:
        last = conn.execute("SELECT amount FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 1", (etype,)).fetchone()
        if last: total_fixed += last["amount"] or 0

    total_var = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM variable_expenses WHERE expense_date>=? AND expense_date<=?",
        (start, end)
    ).fetchone()[0]

    conn.close()

    total_expenses = total_fixed + total_var
    profit = total_revenue - total_cost - total_expenses

    lines = [
        f"📊 *Финансовый отчёт — {mname} {year}*\n",
        f"📦 *Сумма отгрузок:* {total_revenue:,.2f} ₽",
        f"📉 *Себестоимость отгруженного:* {total_cost:,.2f} ₽",
        f"💸 *Расходы:*",
        f"   Постоянные: {total_fixed:,.2f} ₽",
        f"   Переменные: {total_var:,.2f} ₽",
        f"   Итого: {total_expenses:,.2f} ₽",
        f"\n{'💰' if profit>=0 else '📉'} *Прибыль: {profit:,.2f} ₽*",
        f"_(Отгрузки − Себестоимость − Расходы)_",
    ]
    return "\n".join(lines)

# ─── Зарплата ────────────────────────────────────────────────────────────────

def _get_salary_auto(user_id, month_start, month_end):
    """Автозарплата: часы × ставка за период"""
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    rate = (user["daily_rate"] or 0) if user else 0
    hourly = rate / 8 if rate > 0 else 0
    recs = conn.execute(
        "SELECT * FROM time_records WHERE user_id=? AND check_in>=? AND check_in<=? AND status='closed'",
        (user_id, month_start, month_end)
    ).fetchall()
    hours = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in recs)
    days  = len({datetime.fromisoformat(r["check_in"]).date() for r in recs})
    conn.close()
    return days, hours, hours * hourly

def _get_salary_bonuses(user_id, month_str):
    """Ручные доплаты за месяц"""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM salary_bonuses WHERE user_id=? AND month=? ORDER BY created_at",
        (user_id, month_str)
    ).fetchall()
    conn.close()
    return rows

def _salary_month_report():
    """Зарплата за текущий месяц"""
    now = datetime.now()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end   = now
    month_str   = now.strftime("%Y-%m")
    mname       = MONTHS_RU[now.month]

    conn = get_db()
    workers = conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall()
    conn.close()

    if not workers: return f"💰 *Зарплата — {mname} {now.year}*\n\nНет рабочих."

    lines = [f"💰 *Зарплата — {mname} {now.year}*\n"]
    total_fot = 0.0

    for w in workers:
        days, hours, auto = _get_salary_auto(w["id"], month_start, month_end)
        bonuses = _get_salary_bonuses(w["id"], month_str)
        bonus_sum = sum(b["amount"] for b in bonuses)
        total = auto + bonus_sum
        total_fot += total

        lines.append(f"👷 *{w['name']}*")
        lines.append(f"   Дней: {days} | Часов: {hours:.1f}")
        lines.append(f"   Авто: {auto:,.2f} ₽")
        if bonuses:
            for b in bonuses:
                lines.append(f"   ➕ {b['description'] or 'Доплата'}: {b['amount']:,.2f} ₽")
        lines.append(f"   *Итого: {total:,.2f} ₽*\n")

    lines.append(f"💼 *ФОТ за {mname}: {total_fot:,.2f} ₽*")
    return "\n".join(lines)

def _salary_year_report():
    """Реестр зарплат по месяцам за текущий год"""
    now = datetime.now(); year = now.year
    conn = get_db()
    workers = conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall()
    conn.close()

    if not workers: return f"📋 *Реестр зарплат {year}*\n\nНет рабочих."

    lines = [f"📋 *Реестр зарплат — {year} год*\n"]

    for w in workers:
        lines.append(f"👷 *{w['name']}*")
        year_total = 0.0
        for m in range(1, now.month + 1):
            month_start = datetime(year, m, 1)
            if m < now.month:
                import calendar
                last_day = calendar.monthrange(year, m)[1]
                month_end = datetime(year, m, last_day, 23, 59, 59)
            else:
                month_end = now
            month_str = f"{year}-{m:02d}"
            days, hours, auto = _get_salary_auto(w["id"], month_start, month_end)
            bonuses = _get_salary_bonuses(w["id"], month_str)
            bonus_sum = sum(b["amount"] for b in bonuses)
            total = auto + bonus_sum
            year_total += total
            mname = MONTHS_RU[m]
            lines.append(f"   {mname}: {days}дн. {hours:.1f}ч. → *{total:,.2f} ₽*")
        lines.append(f"   *Итого за год: {year_total:,.2f} ₽*\n")

    return "\n".join(lines)

def _start_salary_bonus(tid):
    """Начало диалога добавления доплаты"""
    conn = get_db()
    workers = conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall()
    conn.close()
    if not workers: bot.send_message(tid,"❌ Нет рабочих."); return
    rows = [[(f"👷 {w['name']}", f"sal:user:{w['id']}")] for w in workers]
    rows.append([("🔙 Назад","mn:finance")])
    user_states[tid]="sal:bonus:user"
    bot.send_message(tid,"💰 *Доплата сотруднику*\n\nВыберите сотрудника:", reply_markup=ik(*rows), parse_mode="Markdown")

def _sal_user_selected(tid, uid):
    conn = get_db()
    worker = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    conn.close()
    if not worker: return
    user_data[tid] = {"uid": uid, "uname": worker["name"]}
    user_states[tid] = "sal:bonus:amount"
    bot.send_message(tid, f"👷 *{worker['name']}*\n\nВведите сумму доплаты (₽):\n_/cancel для отмены_", parse_mode="Markdown")

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
            "ORDER BY o.created_at DESC LIMIT 30"
        ).fetchall()
    conn.close()
    if not orders: return "📦 Заказов нет.", ik([("🔙 Назад","mn:sales")])
    lines = ["📦 *Реестр заказов:*\n"]; kb_rows = []
    for o in orders:
        st = ORDER_STATUS_LABELS.get(o["status"],o["status"])
        lines.append(f"• *{o['number']}* — {o['cp_name'] or '—'} | {st}")
        kb_rows.append([(f"{o['number']} | {o['cp_name'] or '—'} | {st}", f"ord:v:{o['id']}")])
    kb_rows.append([("🔙 Назад","mn:sales")])
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
        f"👥 {esc(cp['name']) if cp else '—'} ({c(cp['code']) if cp else '—'})",
        f"📅 Создан: {fmt_dt(o['created_at'])}",
        f"⏰ Готовность: {o['desired_date'] or '—'}",
        f"👤 {cr['name'] if cr else '—'}",
        f"📊 {status}\n", "*Позиции:*"
    ]
    for it in items:
        rem = it["quantity"]-it["shipped_qty"]; stock = get_stock(it["nomenclature_id"])
        to_make = max(0,rem-stock); icon = "✅" if stock>=rem else "⚠️"
        lines.append(f"• *{c(it['code'])}* {it['name']}\n"
                     f"  Заказано: {it['quantity']:,.1f} | Отгружено: {it['shipped_qty']:,.1f} {it['unit']}\n"
                     f"  {icon} Склад: {stock:,.1f} | 🔧 Произвести: {to_make:,.1f}")
    kb_rows = []
    if role == "manager":
        BTN = {"new":("✅ Принять",f"ord:s:{oid}:accepted"),"accepted":("🔧 В работу",f"ord:s:{oid}:in_progress"),"in_progress":("🏁 Готово",f"ord:s:{oid}:ready")}
        if o["status"] in BTN: kb_rows.append([BTN[o["status"]]])
    if role in ("admin","superadmin"):
        if o["status"] in ("ready","in_progress","accepted"):
            kb_rows.append([("📦 Оформить отгрузку",f"ord:ship:{oid}")])
        kb_rows.append([("✏️ Дата готовности",f"ord:edit:{oid}:desired_date"),("✏️ Примечание",f"ord:edit:{oid}:notes")])
        kb_rows.append([("🗑 Удалить заказ",f"ord:del:{oid}")])
    kb_rows.append([("🔙 К заказам","ord:list")])
    return "\n".join(lines), ik(*kb_rows)

def _change_order_status(tid, oid, new_status, user):
    conn = get_db()
    conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status,oid))
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
    conn.commit(); conn.close()
    label = ORDER_STATUS_LABELS.get(new_status, new_status)
    msg = f"📦 Заказ *{order['number']}* → {label}\n👔 {user['name']}"
    # Уведомляем создателя заказа
    if creator:
        try: bot.send_message(creator["telegram_id"], msg, parse_mode="Markdown")
        except: pass
    # Уведомляем админов при принятии и завершении
    if new_status in ("accepted", "ready"):
        notify_roles(("admin","superadmin"), msg)

def _start_new_order(tid):
    conn = get_db(); cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps: bot.send_message(tid,"❌ Нет контрагентов. Добавьте в разделе Контрагенты."); return
    user_states[tid]="ord:cp"; user_data[tid]={"items":[]}
    rows = [[(f"{c(cp['code'])} — {cp['name']}", f"cp:sel:{cp['id']}")] for cp in cps]
    bot.send_message(tid,"➕ *Заказ на производство*\n\nВыберите контрагента:", reply_markup=ik(*rows), parse_mode="Markdown")

def _order_cp_selected(tid, cp_id):
    conn = get_db(); cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: return
    user_data[tid]["cp_id"]=cp_id; user_data[tid]["cp_name"]=cp["name"]
    user_states[tid]="ord:date"
    bot.send_message(tid,f"✅ Контрагент: *{esc(cp['name'])}*\n\nВведите дату готовности (ДД.ММ.ГГГГ)\nили *—* пропустить:", parse_mode="Markdown")

def _order_item_selected(tid, nom_id, call=None):
    conn = get_db(); nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"]={"id":nom_id,"name":nom["name"],"unit":nom["unit"],"code":c(nom["code"])}
    user_data[tid]["last_call"]=call
    user_states[tid]="ord:qty"
    bot.send_message(tid,f"Введите количество *{nom['name']}* ({nom['unit']}):", parse_mode="Markdown")

def _show_item_picker(tid, call=None):
    d=user_data.get(tid,{}); added=d.get("items",[])
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    text = "📦 *Добавьте позиции в заказ:*\n"
    if added:
        text+="\n*Добавлено:*\n"+"\n".join(f"• {c(it['code'])} {it['name']} — {it['qty']:,.1f} {it['unit']}" for it in added)+"\n"
    rows=[]
    for i in range(0,len(items),2):
        row=[(f"{c(items[i]['code'])} {items[i]['name'][:18]}", f"ni:{items[i]['id']}")]
        if i+1<len(items): row.append((f"{c(items[i+1]['code'])} {items[i+1]['name'][:18]}", f"ni:{items[i+1]['id']}"))
        rows.append(row)
    note = d.get("notes","")
    if note: text += f"\n📝 Примечание: {note}"
    rows.append([("📝 Добавить примечание","ord:note")])
    rows.append([("✅ Сохранить заказ","ord:save")])
    kb = ik(*rows)
    # Редактируем существующее сообщение если возможно, иначе отправляем новое
    if call:
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                                  reply_markup=kb, parse_mode="Markdown")
            return
        except: pass
    # Удаляем предыдущее сообщение пикера если оно сохранено
    prev_msg_id = d.get("picker_msg_id")
    if prev_msg_id:
        try: bot.delete_message(tid, prev_msg_id)
        except: pass
    msg = bot.send_message(tid, text, reply_markup=kb, parse_mode="Markdown")
    user_data[tid]["picker_msg_id"] = msg.message_id

def _order_save(call, user):
    d=user_data.get(call.from_user.id,{})
    if not d.get("items"): ans(call,"❌ Добавьте позицию.",True); return
    now=datetime.now(); ans(call); number=next_order_number()
    conn=get_db()
    conn.execute("INSERT INTO orders (number,counterparty_id,created_by,created_at,desired_date,status,order_type,notes) VALUES (?,?,?,?,?,'new','production',?)",
                 (number,d["cp_id"],user["id"],now,d.get("desired_date"),d.get("notes")))
    oid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]:
        conn.execute("INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)", (oid,it["nom_id"],it["qty"]))
    conn.commit(); conn.close()
    cancel_state(call.from_user.id)
    bot.send_message(call.from_user.id,
        f"✅ *Заказ {number}* создан!\n👥 {esc(d['cp_name'])}\n📅 {now.strftime('%d.%m.%Y %H:%M')}", parse_mode="Markdown")
    lines=[f"➕ *Новый заказ на производство {number}*\n👥 {esc(d['cp_name'])}\n⏰ {d.get('desired_date') or '—'}\n📅 {now.strftime('%d.%m.%Y %H:%M')}\n"]
    for it in d["items"]: lines.append(f"• {c(it['code'])} {esc(it['name'])} — {it['qty']:,.1f} {it['unit']}")
    notify_roles(("manager","admin","superadmin"), "\n".join(lines))

# ─── Реестр отгрузок ──────────────────────────────────────────────────────────

def _shipments_list_view():
    conn = get_db()
    ships = conn.execute(
        "SELECT s.*,o.number as onum,c.name as cp_name FROM shipments s "
        "LEFT JOIN orders o ON o.id=s.order_id "
        "LEFT JOIN counterparties c ON c.id=s.counterparty_id "
        "ORDER BY s.created_at DESC LIMIT 30"
    ).fetchall(); conn.close()
    if not ships: return "🚚 Отгрузок нет.", ik([("🔙 Назад","mn:sales")])
    lines=["🚚 *Реестр отгрузок:*\n"]; kb_rows=[]
    for s in ships:
        st = "✅" if s["status"]=="confirmed" else "⏳"
        cp = s["cp_name"] or "—"
        lines.append(f"• *{s['number']}* | {cp} | {st}")
        kb_rows.append([(f"{s['number']} | {cp} | {st}", f"ship:v:{s['id']}")])
    kb_rows.append([("🔙 Назад","mn:sales")])
    return "\n".join(lines), ik(*kb_rows)

def _shipment_view(sid, role):
    conn=get_db()
    s=conn.execute(
        "SELECT s.*,o.number as onum,c.name as cp_name FROM shipments s "
        "LEFT JOIN orders o ON o.id=s.order_id "
        "LEFT JOIN counterparties c ON c.id=s.counterparty_id WHERE s.id=?", (sid,)
    ).fetchone()
    if not s: conn.close(); return "Не найдено.", None
    sitems=conn.execute(
        "SELECT si.quantity, si.sale_price, "
        "COALESCE(n.name,n2.name) as nom_name, COALESCE(n.unit,n2.unit) as nom_unit, "
        "COALESCE(n.code,n2.code) as nom_code "
        "FROM shipment_items si "
        "LEFT JOIN order_items oi ON oi.id=si.order_item_id "
        "LEFT JOIN nomenclature n ON n.id=oi.nomenclature_id "
        "LEFT JOIN nomenclature n2 ON n2.id=si.nomenclature_id "
        "WHERE si.shipment_id=?", (sid,)
    ).fetchall(); conn.close()
    stype="Прямая" if s["shipment_type"]=="direct" else f"Заказ {s['onum'] or '—'}"
    lines=[
        f"🚚 *{s['number']}*",
        f"📋 {stype} | 👥 {s['cp_name'] or '—'}",
        f"📅 {fmt_dt(s['created_at'])} | 🚚 {s['ship_date'] or '—'}",
        f"📝 {s['notes'] or '—'}",
        f"📊 {'✅ Подтверждена' if s['status']=='confirmed' else '⏳ Ожидает'}\n",
        "*Позиции:*"
    ]
    total_rev = 0
    for si in sitems:
        rev = (si["sale_price"] or 0)*si["quantity"]; total_rev += rev
        lines.append(f"• *{c(si['nom_code'])}* {si['nom_name']}: {si['quantity']:,.1f} {si['nom_unit']} × {si['sale_price'] or 0:,.2f} ₽ = {rev:,.2f} ₽")
    lines.append(f"\n💰 *Итого: {total_rev:,.2f} ₽*")
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([("✏️ Дата отгрузки",f"ship:edit:{sid}:ship_date"),("✏️ Примечание",f"ship:edit:{sid}:notes")])
        kb_rows.append([("🗑 Удалить отгрузку",f"ship:del:{sid}")])
    kb_rows.append([("🔙 Назад","ship:list")])
    return "\n".join(lines), ik(*kb_rows)

# ─── Прямая отгрузка ──────────────────────────────────────────────────────────

def _start_direct_shipment(tid):
    conn=get_db(); cps=conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps: bot.send_message(tid,"❌ Нет контрагентов."); return
    user_states[tid]="ds:cp"; user_data[tid]={"items":[]}
    rows=[[(f"{c(cp['code'])} — {cp['name']}", f"ds:cp:{cp['id']}")] for cp in cps]
    rows.append([("🔙 Назад","mn:sales")])
    bot.send_message(tid,"📤 *Прямая отгрузка*\n\nВыберите контрагента:", reply_markup=ik(*rows), parse_mode="Markdown")

def _ds_cp_selected(tid, cp_id):
    conn=get_db(); cp=conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: return
    user_data[tid]["cp_id"]=cp_id; user_data[tid]["cp_name"]=cp["name"]
    user_states[tid]="ds:date"
    bot.send_message(tid,f"✅ {esc(cp['name'])}\n\nВведите дату отгрузки (ДД.ММ.ГГГГ)\nили *—* для сегодня:", parse_mode="Markdown")

def _ds_item_selected(tid, nom_id):
    conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"]={"id":nom_id,"name":nom["name"],"unit":nom["unit"],"code":c(nom["code"]),"sale_price":nom["sale_price"] or 0}
    user_states[tid]="ds:qty"
    stock=get_stock(nom_id)
    bot.send_message(tid,f"*{nom['name']}* ({nom['unit']})\n_Склад: {stock:,.1f}_\n\nВведите количество:", parse_mode="Markdown")

def _show_ds_picker(tid):
    d=user_data.get(tid,{}); added=d.get("items",[])
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    text="📤 *Прямая отгрузка — позиции:*\n"
    if added: text+="\n*Добавлено:*\n"+"\n".join(f"• {it['code']} {it['name']} — {it['qty']:,.1f} {it['unit']} × {it['price']:,.2f}₽" for it in added)+"\n"
    rows=[]
    for i in range(0,len(items),2):
        row=[(f"{c(items[i]['code'])} {items[i]['name'][:18]}", f"ds:ni:{items[i]['id']}")]
        if i+1<len(items): row.append((f"{c(items[i+1]['code'])} {items[i+1]['name'][:18]}", f"ds:ni:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("➕ Примечание","ds:note"),("✅ Оформить","ds:save")])
    bot.send_message(tid, text, reply_markup=ik(*rows), parse_mode="Markdown")

def _ds_save(call, user):
    d=user_data.get(call.from_user.id,{})
    if not d.get("items"): ans(call,"❌ Добавьте позицию.",True); return
    ans(call)
    shortages=[]
    for it in d["items"]:
        stock=get_stock(it["nom_id"])
        if stock < it["qty"]:
            shortages.append({"nom_id":it["nom_id"],"name":it["name"],"unit":it["unit"],"code":it["code"],"need":it["qty"],"stock":stock,"short":it["qty"]-stock})
    if shortages:
        lines=["⚠️ *Не хватает товара:*\n"]
        for sh in shortages:
            lines.append(f"• *{sh['code']} {sh['name']}*\n  Нужно: {sh['need']:,.1f} | Склад: {sh['stock']:,.1f} | Не хватает: {sh['short']:,.1f} {sh['unit']}")
        user_data[call.from_user.id]["shortages"]=shortages
        kb=ik(
            [("📦 Отгрузить что есть + запустить производство","ds:partial:yes")],
            [("⏳ Подождать — сначала произвести","ds:wait:yes")],
            [("❌ Отменить","ds:cancel_ship:yes")],
        )
        bot.send_message(call.from_user.id,"\n".join(lines),reply_markup=kb,parse_mode="Markdown"); return
    _create_direct_shipment(call.from_user.id,d,user)

def _ds_partial_action(tid, user):
    d=user_data.get(tid,{}); shortages=d.get("shortages",[])
    _create_direct_shipment(tid,d,user,partial=True)
    now=datetime.now(); number=next_order_number()
    conn=get_db()
    conn.execute("INSERT INTO orders (number,counterparty_id,created_by,created_at,status,order_type) VALUES (?,?,?,?,'new','production')",
                 (number,d.get("cp_id"),user["id"],now))
    oid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for sh in shortages:
        conn.execute("INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)", (oid,sh["nom_id"],sh["short"]))
    conn.commit(); conn.close()
    bot.send_message(tid,f"✅ Отгрузка создана.\n➕ Автозаказ *{number}* на недостающее количество.", parse_mode="Markdown")
    lines=[f"➕ *Автозаказ {number}* (нехватка при отгрузке)\n"]
    for sh in shortages: lines.append(f"• {sh['code']} {sh['name']}: {sh['short']:,.1f} {sh['unit']}")
    notify_roles(("manager","admin","superadmin"),"\n".join(lines))
    cancel_state(tid)

def _create_direct_shipment(tid, d, user, partial=False):
    now=datetime.now(); number=next_shipment_number()
    conn=get_db()
    conn.execute("INSERT INTO shipments (number,created_by,created_at,ship_date,status,shipment_type,notes,counterparty_id) VALUES (?,?,?,?,'pending','direct',?,?)",
                 (number,user["id"],now,d.get("ship_date"),d.get("notes"),d.get("cp_id")))
    sid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]:
        qty=min(it["qty"],get_stock(it["nom_id"])) if partial else it["qty"]
        if qty>0:
            conn.execute("INSERT INTO shipment_items (shipment_id,nomenclature_id,quantity,sale_price) VALUES (?,?,?,?)",
                         (sid,it["nom_id"],qty,it.get("price",0)))
    conn.commit(); conn.close()
    cancel_state(tid)
    bot.send_message(tid,f"✅ Отгрузка *{number}* создана!\n⏳ Ожидает подтверждения.", parse_mode="Markdown", reply_markup=ik([("💼 Продажи","mn:sales"),("🏠 Главное меню","mn:back")]))
    notify_roles(("manager",),f"📤 *Прямая отгрузка {number}*\n👥 {d.get('cp_name','—')}\nПодтвердите в разделе Производство")

# ─── Отгрузка по заказу ───────────────────────────────────────────────────────

def _start_shipment(tid, oid):
    conn=get_db()
    items=conn.execute(
        "SELECT oi.*,n.name,n.unit,n.code,n.sale_price FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id "
        "WHERE oi.order_id=? AND oi.quantity>oi.shipped_qty", (oid,)
    ).fetchall(); conn.close()
    if not items: bot.send_message(tid,"✅ Все позиции уже отгружены."); return
    user_states[tid]=f"ship:{oid}:date"
    user_data[tid]={"order_id":oid,"items":[dict(it) for it in items],"ship_qtys":[]}
    bot.send_message(tid,"📦 *Отгрузка по заказу*\n\nВведите дату (ДД.ММ.ГГГГ)\nили *—* для сегодня:", parse_mode="Markdown")

def _pending_shipments_view():
    conn=get_db()
    ships=conn.execute("SELECT s.*,o.number as onum FROM shipments s LEFT JOIN orders o ON o.id=s.order_id WHERE s.status='pending'").fetchall()
    conn.close()
    if not ships: return "📦 Нет отгрузок для подтверждения.", ik([("🔙 Назад","mn:production")])
    lines=["📦 *Ожидают подтверждения:*\n"]; rows=[]
    for s in ships:
        stype="Прямая" if s["shipment_type"]=="direct" else f"Заказ {s['onum'] or '—'}"
        lines.append(f"• *{s['number']}* | {stype}")
        rows.append([(f"{s['number']} | {stype}", f"cs:v:{s['id']}")])
    rows.append([("🔙 Назад","mn:production")])
    return "\n".join(lines), ik(*rows)

def _shipment_detail_view(sid):
    conn=get_db()
    ship=conn.execute("SELECT s.*,o.number as onum FROM shipments s LEFT JOIN orders o ON o.id=s.order_id WHERE s.id=?", (sid,)).fetchone()
    if not ship: conn.close(); return "Не найдено.", None
    sitems=conn.execute(
        "SELECT si.quantity,si.sale_price,COALESCE(n.name,n2.name) as nom_name,COALESCE(n.unit,n2.unit) as nom_unit,COALESCE(n.code,n2.code) as nom_code "
        "FROM shipment_items si "
        "LEFT JOIN order_items oi ON oi.id=si.order_item_id "
        "LEFT JOIN nomenclature n ON n.id=oi.nomenclature_id "
        "LEFT JOIN nomenclature n2 ON n2.id=si.nomenclature_id "
        "WHERE si.shipment_id=?", (sid,)
    ).fetchall(); conn.close()
    lines=[f"📦 *{ship['number']}* | {ship['onum'] or 'Прямая'}\n📅 {fmt_dt(ship['created_at'])}\n\n*Позиции:*"]
    for si in sitems:
        lines.append(f"• *{c(si['nom_code'])}* {si['nom_name']}: *{si['quantity']:,.1f} {si['nom_unit']}* × {si['sale_price'] or 0:,.2f} ₽")
    return "\n".join(lines), ik([(f"✅ Подтвердить {ship['number']}",f"cs:ok:{sid}")],[("🔙 Назад","cs:list")])

def _confirm_shipment(tid, sid, user):
    now=datetime.now(); conn=get_db()
    ship=conn.execute("SELECT * FROM shipments WHERE id=?", (sid,)).fetchone()
    sitems=conn.execute("SELECT si.*,oi.nomenclature_id,oi.order_id FROM shipment_items si LEFT JOIN order_items oi ON oi.id=si.order_item_id WHERE si.shipment_id=?", (sid,)).fetchall()
    for si in sitems:
        if si["order_item_id"]:
            conn.execute("UPDATE order_items SET shipped_qty=shipped_qty+? WHERE id=?", (si["quantity"],si["order_item_id"]))
    conn.execute("UPDATE shipments SET status='confirmed',confirmed_by=?,confirmed_at=? WHERE id=?", (user["id"],now,sid))
    if ship["order_id"]:
        rem=conn.execute("SELECT SUM(quantity-shipped_qty) FROM order_items WHERE order_id=?", (ship["order_id"],)).fetchone()[0] or 0
        new_status="shipped" if rem<=0 else "ready"
        conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status,ship["order_id"]))
        order=conn.execute("SELECT * FROM orders WHERE id=?", (ship["order_id"],)).fetchone()
        creator=conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
        conn.commit(); conn.close()
        if creator:
            try: bot.send_message(creator["telegram_id"],
                f"📦 Отгрузка *{ship['number']}* подтверждена!\nЗаказ {order['number']} {'✅ Отгружен' if new_status=='shipped' else '⚠️ Частично'}", parse_mode="Markdown")
            except: pass
    else:
        conn.commit(); conn.close()

# ─── Производство за день ─────────────────────────────────────────────────────

def _start_production(tid):
    today=datetime.now().strftime("%d.%m.%Y")
    user_states[tid]="prod"; user_data[tid]={"date":today,"items":[]}
    _show_prod_picker(tid)

def _show_prod_picker(tid):
    d=user_data.get(tid,{}); today=d.get("date","")
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    added=d.get("items",[])
    text=f"📊 *Производство за {today}*\n"
    if added: text+="\n*Введено:*\n"+"\n".join(f"• {it['name']}: {it['qty']:,.1f} {it['unit']}" for it in added)+"\n"
    text+="\nВыберите позицию:"
    rows=[]
    for i in range(0,len(items),2):
        row=[(f"{c(items[i]['code'])} {items[i]['name'][:18]}", f"pd:n:{items[i]['id']}")]
        if i+1<len(items): row.append((f"{c(items[i+1]['code'])} {items[i+1]['name'][:18]}", f"pd:n:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("✅ Сохранить","pd:done")])
    bot.send_message(tid, text, reply_markup=ik(*rows), parse_mode="Markdown")

def _prod_item_selected(tid, nom_id):
    conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    user_data[tid]["current_nom"]={"id":nom_id,"name":nom["name"],"unit":nom["unit"]}
    user_states[tid]="prod:qty"
    bot.send_message(tid,f"Введите количество *{nom['name']}* ({nom['unit']}):", parse_mode="Markdown")

def _prod_done(call):
    user=get_user(call.from_user.id); d=user_data.get(call.from_user.id,{})
    items=d.get("items",[]); ans(call); cancel_state(call.from_user.id)
    if not items: bot.send_message(call.from_user.id,"Ничего не введено."); return
    today=d.get("date","")
    lines=[f"✅ *Производство за {today}:*\n"]
    for it in items: lines.append(f"• {it['name']}: {it['qty']:,.1f} {it['unit']}")
    bot.send_message(call.from_user.id,"\n".join(lines),parse_mode="Markdown")
    notify_supervisors("\n".join(lines))

# ─── Инвентаризация ───────────────────────────────────────────────────────────

def _start_inv_init(tid):
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    rows=[[( f"📋 {c(it['code'])} — {it['name']}", f"inv:n:{it['id']}")] for it in items]
    rows.append([("🔙 Назад","inv:menu")])
    user_states[tid]="inv:init"
    bot.send_message(tid,"📥 *Начальные остатки*\n\nВыберите позицию:", reply_markup=ik(*rows), parse_mode="Markdown")

def _inv_nom_selected(tid, nom_id):
    conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    cur=get_stock(nom_id); user_data[tid]={"nom_id":nom_id,"nom_name":nom["name"],"unit":nom["unit"]}
    user_states[tid]="inv:init:qty"
    bot.send_message(tid,f"📥 *{nom['name']}*\nТекущий: {cur:,.1f} {nom['unit']}\n\nВведите начальный остаток:", parse_mode="Markdown")

def _start_inv_adj(tid):
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    rows=[[(f"📋 {c(it['code'])} — {it['name']}", f"inv:adj:n:{it['id']}")] for it in items]
    rows.append([("🔙 Назад","inv:menu")])
    user_states[tid]="inv:adj"
    bot.send_message(tid,"🔧 *Корректировка*\n\nВыберите позицию:", reply_markup=ik(*rows), parse_mode="Markdown")

def _inv_adj_nom_selected(tid, nom_id):
    conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    cur=get_stock(nom_id); user_data[tid]={"nom_id":nom_id,"nom_name":nom["name"],"unit":nom["unit"]}
    user_states[tid]="inv:adj:type"
    kb=ik(
        [(f"➕ Прибавить",f"inv:adj:add:{nom_id}")],
        [(f"➖ Убрать",f"inv:adj:sub:{nom_id}")],
        [(f"🎯 Установить точное значение",f"inv:adj:set:{nom_id}")],
        [("🔙 Назад","inv:adj")],
    )
    bot.send_message(tid,f"🔧 *{nom['name']}*\nТекущий: *{cur:,.1f} {nom['unit']}*\n\nТип корректировки:", reply_markup=kb, parse_mode="Markdown")

# ─── Добавление: номенклатура и контрагенты ───────────────────────────────────

def _start_nom_add(tid):
    user_states[tid]="nm:add:name"; user_data[tid]={}
    bot.send_message(tid,"📋 *Новая позиция*\n\nВведите название:\n_/cancel для отмены_", parse_mode="Markdown")

def _start_nom_edit(tid, nom_id, field):
    names={"name":"название","unit":"единицу","notes":"примечание","code":"код"}
    user_states[tid]=f"nm:edit:{nom_id}:{field}"
    bot.send_message(tid,f"✏️ Введите новое {names.get(field,field)}:\n_/cancel для отмены_", parse_mode="Markdown")

def _start_cp_add(tid):
    user_states[tid]="cp:add:name"; user_data[tid]={}
    bot.send_message(tid,"👥 *Новый контрагент*\n\nВведите название:\n_/cancel для отмены_", parse_mode="Markdown")

def _start_cp_edit(tid, cp_id, field):
    names={"name":"название","code":"код","phone":"телефон","email":"email","address":"адрес","notes":"примечание"}
    user_states[tid]=f"cp:edit:{cp_id}:{field}"
    bot.send_message(tid,f"✏️ Введите новое {names.get(field,field)}:\n_/cancel для отмены_", parse_mode="Markdown")

def _start_request(tid, rtype):
    prompts={"absence":"📅 Укажи дату и причину:","breakdown":"🔧 Опиши неисправность:","mts":"📦 Опиши что нужно:"}
    user_states[tid]=f"rq:{rtype}"
    bot.send_message(tid,prompts[rtype]+"\n\n_/cancel для отмены_", parse_mode="Markdown")

def _start_sale_price_input(tid, nom_id):
    conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not nom: return
    current_price = nom["sale_price"] or 0
    user_data[tid]["current_price_nom_id"]=nom_id
    user_data[tid]["current_price_name"]=nom["name"]
    user_states[tid]="ds:price"
    bot.send_message(tid,
        f"💵 *{nom['name']}*\nПрайс: {current_price:,.2f} ₽\n\nВведите цену продажи (или *—* оставить прайс):",
        parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ТЕКСТА
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.from_user.id in user_states)
def universal_text(message):
    tid=message.from_user.id; state=user_states.get(tid,""); text=message.text.strip(); user=get_user(tid)
    if not user: return

    # Заявки
    if state.startswith("rq:"):
        rtype=state.replace("rq:","")
        labels={"absence":"📅 Не выйдет","breakdown":"🔧 Неисправность","mts":"📦 МТС"}
        now=datetime.now(); cancel_state(tid)
        conn=get_db(); conn.execute("INSERT INTO requests (user_id,type,text,created_at) VALUES (?,?,?,?)",(user["id"],rtype,text,now)); conn.commit(); conn.close()
        bot.send_message(tid,"✅ Заявка отправлена!")
        notify_supervisors(f"📋 *{labels.get(rtype,rtype)}*\n👤 *{user['name']}*\n🕐 {now.strftime('%d.%m.%Y %H:%M')}\n\n📝 {text}"); return

    # Добавление сотрудника
    if state=="usr:add:id":
        try: new_tid=int(text)
        except: bot.send_message(tid,"❌ Введите числовой ID."); return
        user_data[tid]["new_tid"]=new_tid; user_states[tid]="usr:add:name"
        bot.send_message(tid,"Введите имя и фамилию:"); return
    if state=="usr:add:name":
        user_data[tid]["name"]=text; user_states[tid]="usr:add:role"
        allowed=SUPER_MANAGED if user["role"]=="superadmin" else ADMIN_MANAGED
        rows=[[(ROLE_LABELS[r],f"usr:role:{r}")] for r in allowed]
        bot.send_message(tid,"Выберите роль:", reply_markup=ik(*rows)); return

    # Дата заказа
    if state=="ord:note":
        user_data[tid]["notes"]=text
        user_states[tid]="ord:items"
        call_back=user_data[tid].pop("last_note_call",None)
        bot.send_message(tid,"✅ Примечание добавлено!")
        _show_item_picker(tid); return

    if state=="ord:date":
        user_data[tid]["desired_date"]=None if text in ("-","пропустить") else text
        user_states[tid]="ord:items"; _show_item_picker(tid); return

    # Количество в заказе
    if state=="ord:qty":
        try: qty=float(text.replace(",",".")); assert qty>0
        except: bot.send_message(tid,"Введите число больше нуля."); return
        d=user_data[tid]; nom=d.pop("current_nom"); last_call=d.pop("last_call",None)
        d["items"].append({"nom_id":nom["id"],"name":nom["name"],"unit":nom["unit"],"code":nom["code"],"qty":qty})
        user_states[tid]="ord:items"
        bot.send_message(tid,f"✅ {nom['name']} — {qty:,.1f} {nom['unit']}")
        _show_item_picker(tid); return

    # Прямая отгрузка — дата
    if state=="ds:date":
        user_data[tid]["ship_date"]=datetime.now().strftime("%d.%m.%Y") if text=="-" else text
        user_states[tid]="ds:items"; _show_ds_picker(tid); return

    # Прямая отгрузка — количество
    if state=="ds:qty":
        try: qty=float(text.replace(",",".")); assert qty>0
        except: bot.send_message(tid,"Введите число больше нуля."); return
        d=user_data[tid]; nom=d.pop("current_nom")
        # Сначала спрашиваем цену
        default_price = nom.get("sale_price",0)
        user_data[tid]["pending_item"]={"nom_id":nom["id"],"name":nom["name"],"unit":nom["unit"],"code":nom["code"],"qty":qty,"price":default_price}
        user_states[tid]="ds:item_price"
        bot.send_message(tid,
            f"✅ {nom['name']} — {qty:,.1f} {nom['unit']}\n\n💵 Цена продажи (прайс: {default_price:,.2f} ₽)\nВведите цену или *—* оставить прайс:",
            parse_mode="Markdown"); return

    # Прямая отгрузка — цена позиции
    if state=="ds:item_price":
        d=user_data[tid]; item=d.pop("pending_item")
        if text != "-":
            try: item["price"]=float(text.replace(",",".")); assert item["price"]>=0
            except: bot.send_message(tid,"Введите число или *—*."); user_data[tid]["pending_item"]=item; return
        d["items"].append(item)
        bot.send_message(tid,f"✅ Добавлено: {item['name']} × {item['qty']:,.1f} × {item['price']:,.2f} ₽")
        user_states[tid]="ds:items"; _show_ds_picker(tid); return

    # Прямая отгрузка — примечание
    if state=="ds:note":
        user_data[tid]["notes"]=text; user_states[tid]="ds:items"
        bot.send_message(tid,"✅ Примечание добавлено."); _show_ds_picker(tid); return

    # Отгрузка по заказу — дата
    if state.startswith("ship:") and state.endswith(":date"):
        oid=int(state.split(":")[1])
        user_data[tid]["ship_date"]=datetime.now().strftime("%d.%m.%Y") if text=="-" else text
        items=user_data[tid]["items"]; user_states[tid]=f"ship:{oid}:0"
        it=items[0]; rem=it["quantity"]-it["shipped_qty"]
        bot.send_message(tid,f"Позиция 1/{len(items)}: *{c(it['code'])} {it['name']}*\nОстаток: {rem:,.1f} {it['unit']}\n\nВведите количество:", parse_mode="Markdown"); return

    # Отгрузка по заказу — количество
    if state.startswith("ship:") and not state.endswith(":date"):
        parts=state.split(":"); oid=int(parts[1]); idx=int(parts[2])
        d=user_data.get(tid,{}); items=d.get("items",[])
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        it=items[idx]; rem=it["quantity"]-it["shipped_qty"]; qty=min(qty,rem)
        d["ship_qtys"].append({"order_item_id":it["id"],"qty":qty,"name":it["name"],"unit":it["unit"],"code":c(it["code"]),"sale_price":it.get("sale_price",0)})
        next_idx=idx+1
        if next_idx<len(items):
            user_states[tid]=f"ship:{oid}:{next_idx}"
            nit=items[next_idx]; nr=nit["quantity"]-nit["shipped_qty"]
            bot.send_message(tid,f"Позиция {next_idx+1}/{len(items)}: *{c(nit['code'])} {nit['name']}*\nОстаток: {nr:,.1f} {nit['unit']}\n\nВведите количество:", parse_mode="Markdown")
        else:
            # Спрашиваем цену для первой позиции
            user_data[tid]["ship_price_idx"]=0; user_states[tid]=f"ship_price:{oid}"
            sq=d["ship_qtys"][0]
            bot.send_message(tid,
                f"💵 Цена продажи для *{sq['name']}*\n(прайс: {sq['sale_price']:,.2f} ₽)\nВведите цену или *—* оставить прайс:",
                parse_mode="Markdown")
        return

    # Отгрузка по заказу — цена
    if state.startswith("ship_price:"):
        oid=int(state.split(":")[1]); d=user_data.get(tid,{})
        idx=d.get("ship_price_idx",0); sq=d["ship_qtys"][idx]
        if text!="-":
            try: sq["sale_price"]=float(text.replace(",",".")); assert sq["sale_price"]>=0
            except: bot.send_message(tid,"Введите число или *—*."); return
        next_p=idx+1
        if next_p<len(d["ship_qtys"]) and d["ship_qtys"][next_p]["qty"]>0:
            d["ship_price_idx"]=next_p; user_states[tid]=f"ship_price:{oid}"
            nsq=d["ship_qtys"][next_p]
            bot.send_message(tid,f"💵 Цена для *{nsq['name']}* (прайс: {nsq['sale_price']:,.2f} ₽)\nВведите или *—*:", parse_mode="Markdown")
        else:
            # Сохраняем отгрузку
            now=datetime.now(); number=next_shipment_number()
            conn=get_db()
            conn.execute("INSERT INTO shipments (number,order_id,created_by,created_at,ship_date,status,shipment_type) VALUES (?,?,?,?,?,'pending','order')",
                         (number,oid,user["id"],now,d.get("ship_date")))
            sid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            for sq2 in d["ship_qtys"]:
                if sq2["qty"]>0:
                    conn.execute("INSERT INTO shipment_items (shipment_id,order_item_id,quantity,sale_price) VALUES (?,?,?,?)",
                                 (sid,sq2["order_item_id"],sq2["qty"],sq2.get("sale_price",0)))
            conn.execute("UPDATE orders SET status='shipping' WHERE id=?", (oid,)); conn.commit(); conn.close()
            cancel_state(tid)
            lines=[f"✅ *{number}* создана!\n\n*Позиции:*"]
            for sq2 in d["ship_qtys"]:
                if sq2["qty"]>0: lines.append(f"• {sq2['code']} {sq2['name']}: {sq2['qty']:,.1f} × {sq2['sale_price']:,.2f} ₽")
            bot.send_message(tid,"\n".join(lines),parse_mode="Markdown", reply_markup=ik([("💼 Продажи","mn:sales"),("🏠 Главное меню","mn:back")]))
            notify_roles(("manager",),"\n".join(lines)+"\n\nПодтвердите в разделе Производство")
        return

    # Производство — количество
    if state=="prod:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; nom=d.pop("current_nom"); today=d["date"]; now=datetime.now()
        conn=get_db()
        ex=conn.execute("SELECT * FROM daily_production WHERE date=? AND nomenclature_id=? AND recorded_by=?", (today,nom["id"],user["id"])).fetchone()
        if ex: conn.execute("UPDATE daily_production SET quantity=quantity+?,recorded_at=? WHERE id=?", (qty,now,ex["id"]))
        else:  conn.execute("INSERT INTO daily_production (date,nomenclature_id,quantity,recorded_by,recorded_at) VALUES (?,?,?,?,?)", (today,nom["id"],qty,user["id"],now))
        updated=conn.execute("SELECT dp.*,n.name,n.unit FROM daily_production dp JOIN nomenclature n ON n.id=dp.nomenclature_id WHERE dp.date=? AND dp.recorded_by=?", (today,user["id"])).fetchall()
        conn.commit(); conn.close()
        d["items"]=[{"name":e["name"],"unit":e["unit"],"qty":e["quantity"]} for e in updated]
        user_states[tid]="prod"; bot.send_message(tid,f"✅ {nom['name']}: {qty:,.1f} {nom['unit']}")
        _show_prod_picker(tid); return

    # Инвентаризация — начальный остаток
    if state=="inv:init:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; conn=get_db()
        conn.execute("UPDATE nomenclature SET initial_stock=? WHERE id=?", (qty,d["nom_id"])); conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid,f"✅ *{d['nom_name']}*: начальный остаток {qty:,.1f} {d['unit']}", parse_mode="Markdown", reply_markup=ik([("⚙️ Инвентаризация","inv:menu"),("🏠 Главное меню","mn:back")])); return

    # Инвентаризация — корректировка
    if state=="inv:adj:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; adj_type=d["adj_type"]; nom_id=d["nom_id"]
        conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); cur=get_stock(nom_id)
        if adj_type=="set": diff=qty-cur; atype="add" if diff>=0 else "sub"; aval=abs(diff)
        elif adj_type=="add": atype="add"; aval=qty
        else: atype="sub"; aval=qty
        conn.execute("INSERT INTO stock_adjustments (nomenclature_id,quantity,type,comment,created_by) VALUES (?,?,?,?,?)",
                     (nom_id,aval,atype,"Инвентаризация",user["id"])); conn.commit(); conn.close()
        new_stock=get_stock(nom_id); cancel_state(tid)
        bot.send_message(tid,f"✅ *{nom['name']}*\nБыло: {cur:,.1f} → Стало: *{new_stock:,.1f} {nom['unit']}*", parse_mode="Markdown", reply_markup=ik([("⚙️ Инвентаризация","inv:menu"),("🏠 Главное меню","mn:back")])); return

    # Изменение цены номенклатуры
    if state.startswith("nm:price:"):
        parts=state.split(":"); nom_id=int(parts[2]); ptype=parts[3]
        try: price=float(text.replace(",",".")); assert price>=0
        except: bot.send_message(tid,"Введите число."); return
        field="cost_price" if ptype=="cost" else "sale_price"
        conn=get_db()
        conn.execute(f"UPDATE nomenclature SET {field}=? WHERE id=?", (price,nom_id))
        conn.execute("INSERT INTO price_history (nomenclature_id,price_type,price,changed_by) VALUES (?,?,?,?)",
                     (nom_id,ptype,price,user["id"]))
        conn.commit(); conn.close()
        cancel_state(tid)
        label="Себестоимость" if ptype=="cost" else "Прайс"
        bot.send_message(tid,f"✅ {label} обновлена: *{price:,.2f} ₽*", parse_mode="Markdown", reply_markup=ik([("📋 Номенклатура","nm:list"),("🏠 Главное меню","mn:back")])); return

    # Номенклатура — добавление
    if state=="nm:add:name":
        user_data[tid]["name"]=text; user_states[tid]="nm:add:unit"
        bot.send_message(tid,"Введите единицу (м, шт, кг...):"); return
    if state=="nm:add:unit":
        user_data[tid]["unit"]=text; user_states[tid]="nm:add:notes"
        bot.send_message(tid,"Введите примечание (или *—*):"); return
    if state=="nm:add:cost":
        try: price=float(text.replace(",",".")); assert price>=0
        except: bot.send_message(tid,"Введите число."); return
        user_data[tid]["cost_price"]=price; user_states[tid]="nm:add:sale"
        bot.send_message(tid,f"Себестоимость: {price:,.2f} ₽\n\nВведите прайсовую цену (или *—* пропустить):"); return
    if state=="nm:add:sale":
        price=0 if text=="-" else float(text.replace(",","."))
        user_data[tid]["sale_price"]=price; user_states[tid]="nm:add:save"
        _save_new_nom(tid, user); return
    if state=="nm:add:notes":
        d=user_data[tid]; d["notes"]=None if text=="-" else text
        user_states[tid]="nm:add:cost"
        bot.send_message(tid,"Введите себестоимость (₽) или *—* пропустить:", parse_mode="Markdown"); return

    # Номенклатура — редактирование
    if state.startswith("nm:edit:"):
        parts=state.split(":"); nom_id=int(parts[2]); field=parts[3]
        conn=get_db(); conn.execute(f"UPDATE nomenclature SET {field}=? WHERE id=?", (text,nom_id)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"✅ Обновлено!", reply_markup=ik([("📋 Номенклатура","nm:list"),("🏠 Главное меню","mn:back")])); return

    # Контрагент — добавление (автокод)
    if state=="cp:add:name":
        user_data[tid]["name"]=text; user_states[tid]="cp:add:phone"
        bot.send_message(tid,"Телефон (или *—*):"); return
    if state=="cp:add:phone":
        user_data[tid]["phone"]=None if text=="-" else text; user_states[tid]="cp:add:email"
        bot.send_message(tid,"Email (или *—*):"); return
    if state=="cp:add:email":
        user_data[tid]["email"]=None if text=="-" else text; user_states[tid]="cp:add:address"
        bot.send_message(tid,"Адрес отгрузки (или *—*):"); return
    if state=="cp:add:address":
        user_data[tid]["address"]=None if text=="-" else text; user_states[tid]="cp:add:notes"
        bot.send_message(tid,"Примечание (или *—*):"); return
    if state=="cp:add:notes":
        d=user_data[tid]; notes=None if text=="-" else text
        code=next_cp_code()
        conn=get_db()
        try:
            conn.execute("INSERT INTO counterparties (code,name,phone,email,address,notes) VALUES (?,?,?,?,?,?)",
                         (code,d["name"],d.get("phone"),d.get("email"),d.get("address"),notes))
            conn.commit()
            bot.send_message(tid,f"✅ Контрагент добавлен!\n🔑 Код: *{code}*\n👥 {d['name']}", parse_mode="Markdown", reply_markup=ik([("👥 Контрагенты","cp:list"),("🏠 Главное меню","mn:back")]))
        except Exception as e:
            bot.send_message(tid,f"❌ Ошибка: {e}")
        finally: conn.close()
        cancel_state(tid); return

    # Заказ — редактирование полей
    if state.startswith("ord:edit:"):
        parts=state.split(":"); oid=int(parts[2]); field=parts[3]
        conn=get_db(); conn.execute(f"UPDATE orders SET {field}=? WHERE id=?", (text,oid)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"✅ Обновлено!",reply_markup=ik([(f"📦 К заказу",f"ord:v:{oid}"),(f"🏠 Главное меню","mn:back")])); return

    # Отгрузка — редактирование полей
    if state.startswith("ship:edit:"):
        parts=state.split(":"); sid=int(parts[2]); field=parts[3]
        conn=get_db(); conn.execute(f"UPDATE shipments SET {field}=? WHERE id=?", (text,sid)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"✅ Обновлено!",reply_markup=ik([(f"🚚 К отгрузке",f"ship:v:{sid}"),(f"🏠 Главное меню","mn:back")])); return

    # Контрагент — редактирование
    if state.startswith("cp:edit:"):
        parts=state.split(":"); cp_id=int(parts[2]); field=parts[3]
        conn=get_db(); conn.execute(f"UPDATE counterparties SET {field}=? WHERE id=?", (text,cp_id)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"✅ Обновлено!", reply_markup=ik([("👥 Контрагенты","cp:list"),("🏠 Главное меню","mn:back")])); return

    # Постоянные расходы — сумма
    if state.startswith("exp:fixed:") and state.endswith(":amount"):
        etype=state.split(":")[2]
        try: amount=float(text.replace(",",".")); assert amount>=0
        except: bot.send_message(tid,"Введите сумму."); return
        user_data[tid]["amount"]=amount; user_states[tid]=f"exp:fixed:{etype}:from"
        bot.send_message(tid,f"Сумма: *{amount:,.2f} ₽*\n\nС какого числа действует? (ДД.ММ.ГГГГ)\nили *—* для сегодня:", parse_mode="Markdown"); return

    if state.startswith("exp:fixed:") and state.endswith(":from"):
        etype=state.split(":")[2]
        eff_from=datetime.now().strftime("%d.%m.%Y") if text=="-" else text
        d=user_data[tid]; amount=d["amount"]
        conn=get_db()
        conn.execute("INSERT INTO fixed_expenses (type,amount,description,changed_by,effective_from) VALUES (?,?,?,?,?)",
                     (etype,amount,d.get("description",""),user["id"],eff_from))
        conn.commit(); conn.close()
        cancel_state(tid)
        label=dict(FIXED_EXPENSE_TYPES).get(etype,etype)
        bot.send_message(tid,f"✅ *{label}*: {amount:,.2f} ₽ с {eff_from}", parse_mode="Markdown", reply_markup=ik([("💸 Постоянные расходы","exp:fixed:list"),("🏠 Главное меню","mn:back")])); return

    # Переменные расходы — сумма
    if state=="exp:var:amount":
        try: amount=float(text.replace(",",".")); assert amount>=0
        except: bot.send_message(tid,"Введите сумму."); return
        user_data[tid]["amount"]=amount; user_states[tid]="exp:var:desc"
        bot.send_message(tid,"Введите описание (или *—*):"); return

    if state=="exp:var:desc":
        d=user_data[tid]; desc=None if text=="-" else text
        today=datetime.now().strftime("%d.%m.%Y")
        conn=get_db()
        conn.execute("INSERT INTO variable_expenses (category,amount,description,expense_date,created_by) VALUES (?,?,?,?,?)",
                     (d["category"],d["amount"],desc,today,user["id"]))
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid,f"✅ Расход добавлен!\n{d['category']}: *{d['amount']:,.2f} ₽*", parse_mode="Markdown", reply_markup=ik([("💰 Финансы","mn:finance"),("🏠 Главное меню","mn:back")])); return

def _save_new_nom(tid, user):
    d=user_data.get(tid,{}); code=next_nom_code()
    conn=get_db()
    try:
        conn.execute("INSERT INTO nomenclature (code,name,unit,notes,cost_price,sale_price) VALUES (?,?,?,?,?,?)",
                     (code,d["name"],d["unit"],d.get("notes"),d.get("cost_price",0),d.get("sale_price",0)))
        conn.commit()
        if d.get("cost_price",0)>0:
            nom_id=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute("INSERT INTO price_history (nomenclature_id,price_type,price,changed_by) VALUES (?,?,?,?)",
                         (nom_id,"cost",d["cost_price"],user["id"]))
        if d.get("sale_price",0)>0:
            nom_id=conn.execute("SELECT id FROM nomenclature WHERE code=?", (code,)).fetchone()[0]
            conn.execute("INSERT INTO price_history (nomenclature_id,price_type,price,changed_by) VALUES (?,?,?,?)",
                         (nom_id,"sale",d["sale_price"],user["id"]))
        conn.commit()
        bot.send_message(tid,f"✅ Добавлено!\n🔑 Код: *{code}*\n📋 {d['name']}", parse_mode="Markdown", reply_markup=ik([("📋 Номенклатура","nm:list"),("🏠 Главное меню","mn:back")]))
    except Exception as e:
        bot.send_message(tid,f"❌ Ошибка: {e}")
    finally: conn.close()
    cancel_state(tid)

# Выбор роли при добавлении сотрудника
@bot.callback_query_handler(func=lambda c: c.data.startswith("usr:role:"))
def usr_role_select(call):
    user=get_user(call.from_user.id); role=call.data.split(":")[2]
    d=user_data.get(call.from_user.id,{})
    new_tid=d.get("new_tid"); name=d.get("name",""); ans(call)
    if not new_tid or not name: cancel_state(call.from_user.id); return
    conn=get_db()
    try:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (new_tid,name,role))
        conn.commit()
        bot.send_message(call.from_user.id,f"✅ *{name}* — {ROLE_LABELS[role]}", parse_mode="Markdown")
        try: bot.send_message(new_tid,f"✅ Вы зарегистрированы как *{name}*.\nНажмите /start", parse_mode="Markdown")
        except: pass
    except sqlite3.IntegrityError:
        bot.send_message(call.from_user.id,"⚠️ Пользователь уже есть.")
    finally: conn.close()
    cancel_state(call.from_user.id)

# ═══════════════════════════════════════════════════════════════════════════════
# ФОНОВЫЕ ЗАДАЧИ
# ═══════════════════════════════════════════════════════════════════════════════

def reminder_loop():
    while True:
        time.sleep(1800)
        try:
            conn=get_db(); now=datetime.now()

            # Напоминания об уходе
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

            # Напоминания о новых заказах начальнику цеха (каждые 2 часа)
            new_orders=conn.execute(
                "SELECT o.*,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id "
                "WHERE o.status='new' AND o.created_at <= ?",
                (now - timedelta(hours=2),)
            ).fetchall()
            if new_orders:
                managers=conn.execute("SELECT telegram_id FROM users WHERE role='manager'").fetchall()
                for o in new_orders:
                    msg=f"⏰ *Напоминание о новом заказе!*\n\n📦 *{o['number']}*\n👥 {o['cp_name'] or '—'}\n📅 {fmt_dt(o['created_at'])}\n\nЗаказ ожидает принятия!"
                    for m in managers:
                        try: bot.send_message(m["telegram_id"],msg,parse_mode="Markdown")
                        except: pass

            conn.close()
        except Exception as e: print(f"[reminder] {e}")

threading.Thread(target=reminder_loop, daemon=True).start()

print("Bot started!")
bot.infinity_polling()
