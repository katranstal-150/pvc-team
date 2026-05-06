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
    "worker": "Рабочий", "manager": "Начальник Цеха",
    "admin": "Администратор", "superadmin": "Главный Админ",
}
ORDER_STATUS = {
    "new": "Новый", "accepted": "Принят",
    "in_progress": "В работе", "ready": "Готов", "shipped": "Отгружен",
}
MONTHS_RU = {
    1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
    7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"
}
FIXED_TYPES = [("rent","Аренда"),("utility","Коммунальные"),("waste","Мусор"),("salary","Зарплата фонд"),("other","Прочие")]
NOM_DATA = [
    ("НОМ-001","Гарпун Вид 1 уз белый","м","Намотка 200м"),
    ("НОМ-002","Гарпун Вид 1 уз черный","м","Намотка 200м"),
    ("НОМ-003","Гарпун Вид 2 шир белый","м","Намотка 200м"),
    ("НОМ-004","Гарпун Вид 2 шир черный","м","Намотка 200м"),
    ("НОМ-005","Вставка Т Элит","м","Первичное сырье"),
    ("НОМ-006","Вставка Т черная","м","Намотка 50/150м"),
    ("НОМ-007","Вставка Т","м","Намотка 50/150м"),
    ("НОМ-008","Вставка Уголок белая","м","Намотка 50/100м"),
    ("НОМ-009","Вставка Уголок черная","м","Намотка 50/100м"),
    ("НОМ-010","Багет ПВХ 150 гм","м","Вид 1 для пистолета"),
    ("НОМ-011","Багет ПВХ 140 гм","м","Вид 2"),
    ("НОМ-012","Платформа 60-110","шт","50шт/короб"),
    ("НОМ-013","Платформа 90","шт","50шт/короб"),
]
INITIAL_USERS = [(915402089,"Katran 150","superadmin")]
user_states = {}
user_data = {}

# ─── БД ───────────────────────────────────────────────────────────────────────

def get_db():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    conn = get_db()
    conn.executescript("""\nCREATE TABLE IF NOT EXISTS users (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\ntelegram_id INTEGER UNIQUE NOT NULL,\nname TEXT NOT NULL, role TEXT NOT NULL,\ndaily_rate REAL DEFAULT 0,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS nomenclature (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\ncode TEXT UNIQUE NOT NULL, name TEXT NOT NULL,\nunit TEXT NOT NULL, notes TEXT,\ninitial_stock REAL DEFAULT 0,\ncost_price REAL DEFAULT 0, sale_price REAL DEFAULT 0,\nactive INTEGER DEFAULT 1,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS counterparties (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\ncode TEXT UNIQUE NOT NULL, name TEXT NOT NULL,\nphone TEXT, email TEXT, address TEXT, notes TEXT,\nactive INTEGER DEFAULT 1,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS orders (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nnumber TEXT UNIQUE NOT NULL,\ncounterparty_id INTEGER NOT NULL,\ncreated_by INTEGER NOT NULL,\ncreated_at TIMESTAMP NOT NULL,\ndesired_date TEXT, status TEXT DEFAULT 'new', notes TEXT\n);\nCREATE TABLE IF NOT EXISTS order_items (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\norder_id INTEGER NOT NULL, nomenclature_id INTEGER NOT NULL,\nquantity REAL NOT NULL, shipped_qty REAL DEFAULT 0\n);\nCREATE TABLE IF NOT EXISTS order_comments (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\norder_id INTEGER NOT NULL, user_id INTEGER NOT NULL,\ntext TEXT NOT NULL,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS time_records (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nuser_id INTEGER NOT NULL, check_in TIMESTAMP NOT NULL,\ncheck_out TIMESTAMP, status TEXT DEFAULT 'active',\nreminder_count INTEGER DEFAULT 0, last_reminder TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS daily_production (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\ndate TEXT NOT NULL, nomenclature_id INTEGER NOT NULL,\nquantity REAL NOT NULL, recorded_by INTEGER NOT NULL,\nrecorded_at TIMESTAMP NOT NULL\n);\nCREATE TABLE IF NOT EXISTS stock_adjustments (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nnomenclature_id INTEGER NOT NULL, quantity REAL NOT NULL,\ntype TEXT NOT NULL, comment TEXT, created_by INTEGER NOT NULL,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS shipments (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nnumber TEXT UNIQUE NOT NULL,\norder_id INTEGER, created_by INTEGER NOT NULL,\ncreated_at TIMESTAMP NOT NULL, ship_date TEXT,\nconfirmed_by INTEGER, confirmed_at TIMESTAMP,\nstatus TEXT DEFAULT 'pending',\nshipment_type TEXT DEFAULT 'order',\nnotes TEXT, counterparty_id INTEGER\n);\nCREATE TABLE IF NOT EXISTS shipment_items (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nshipment_id INTEGER NOT NULL,\norder_item_id INTEGER, nomenclature_id INTEGER,\nquantity REAL NOT NULL, sale_price REAL DEFAULT 0\n);\nCREATE TABLE IF NOT EXISTS fixed_expenses (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\ntype TEXT NOT NULL, amount REAL NOT NULL,\ndescription TEXT, changed_by INTEGER NOT NULL,\neffective_from TEXT NOT NULL,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS variable_expenses (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\ncategory TEXT NOT NULL, amount REAL NOT NULL,\ndescription TEXT, expense_date TEXT NOT NULL,\ncreated_by INTEGER NOT NULL,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\nCREATE TABLE IF NOT EXISTS salary_bonuses (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\nuser_id INTEGER NOT NULL, amount REAL NOT NULL,\ndescription TEXT, month TEXT NOT NULL,\ncreated_by INTEGER NOT NULL,\ncreated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\n""")
    if conn.execute("SELECT COUNT(*) FROM nomenclature").fetchone()[0] == 0:
        conn.executemany("INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)", NOM_DATA)
    for tid, name, role in INITIAL_USERS:
        ex = conn.execute("SELECT id FROM users WHERE telegram_id=?", (tid,)).fetchone()
        if ex: conn.execute("UPDATE users SET role=? WHERE telegram_id=?", (role,tid))
        else: conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (tid,name,role))
    conn.commit(); conn.close()
    print("DB ready")

init_db()

# Настройка нативного бургер меню Telegram (кнопка ☰ Меню внизу слева)
try:
    from telebot.types import BotCommand, MenuButtonCommands
    bot.set_my_commands([
        BotCommand("start",          "🏠 Главное меню"),
        BotCommand("staff",          "👥 Сотрудники"),
        BotCommand("production",     "📦 Производство и заказы"),
        BotCommand("warehouse",      "🏭 Склад"),
        BotCommand("shipments",      "🚚 Отгрузки"),
        BotCommand("finance",        "💰 Финансы"),
        BotCommand("nomenclature",   "📋 Номенклатура"),
        BotCommand("counterparties", "👥 Контрагенты"),
        BotCommand("cancel",         "❌ Отменить действие"),
        BotCommand("myid",           "🔑 Мой Telegram ID"),
    ])
    bot.set_chat_menu_button(menu_button=MenuButtonCommands())
except Exception as e:
    print(f"Menu button setup: {e}")

# ─── УТИЛИТЫ ──────────────────────────────────────────────────────────────────

def get_user(tid):
    c = get_db(); u = c.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone(); c.close(); return u

def cancel_state(tid): user_states.pop(tid,None); user_data.pop(tid,None)

def fmt(s):
    if not s: return "-"
    try: return datetime.fromisoformat(s).strftime("%d.%m.%Y %H:%M")
    except: return str(s)

def is_work_time(): return 8 <= datetime.now().hour < 20

def notify(roles, text, force=False):
    if not force and not is_work_time(): return
    c = get_db()
    ph = ",".join("?"*len(roles))
    rows = c.execute(f"SELECT telegram_id FROM users WHERE role IN ({ph})", roles).fetchall()
    c.close()
    for r in rows:
        try: bot.send_message(r["telegram_id"], text)
        except: pass

def get_stock(nom_id):
    c = get_db()
    nom = c.execute("SELECT initial_stock FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
    init = (nom["initial_stock"] or 0) if nom else 0
    prod = c.execute("SELECT COALESCE(SUM(quantity),0) FROM daily_production WHERE nomenclature_id=?", (nom_id,)).fetchone()[0]
    adj = c.execute("SELECT COALESCE(SUM(CASE WHEN type='add' THEN quantity ELSE -quantity END),0) FROM stock_adjustments WHERE nomenclature_id=?", (nom_id,)).fetchone()[0]
    ship = c.execute(
        "SELECT COALESCE(SUM(si.quantity),0) FROM shipment_items si JOIN shipments s ON s.id=si.shipment_id "
        "WHERE s.status='confirmed' AND (si.nomenclature_id=? OR EXISTS(SELECT 1 FROM order_items oi WHERE oi.id=si.order_item_id AND oi.nomenclature_id=?))",
        (nom_id,nom_id)
    ).fetchone()[0]
    c.close(); return init + prod + adj - ship

def next_order_num():
    y = datetime.now().year; c = get_db()
    last = c.execute("SELECT number FROM orders WHERE number LIKE ? ORDER BY id DESC LIMIT 1", (f"{y}-%",)).fetchone()
    c.close(); num = int(last["number"].split("-")[1])+1 if last else 1; return f"{y}-{num:03d}"

def next_ship_num():
    y = datetime.now().year; c = get_db()
    last = c.execute("SELECT number FROM shipments WHERE number LIKE ? ORDER BY id DESC LIMIT 1", (f"OTG-{y}-%",)).fetchone()
    c.close(); num = int(last["number"].split("-")[2])+1 if last else 1; return f"OTG-{y}-{num:03d}"

def next_cp_code():
    c = get_db(); last = c.execute("SELECT code FROM counterparties WHERE code LIKE 'BOT-%' ORDER BY id DESC LIMIT 1").fetchone(); c.close()
    if last:
        try: num = int(last["code"].split("-")[1])+1
        except: num = 1
    else: num = 1
    return f"BOT-{num:03d}"

# ─── КЛАВИАТУРЫ ───────────────────────────────────────────────────────────────

def ik(*rows):
    kb = InlineKeyboardMarkup()
    for row in rows: kb.row(*[InlineKeyboardButton(t, callback_data=cd) for t,cd in row])
    return kb

def ans(call):
    try: bot.answer_callback_query(call.id)
    except: pass

def worker_rk():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("Пришел на работу"), KeyboardButton("Ушел с работы"))
    kb.row(KeyboardButton("Моя статистика"), KeyboardButton("Заявки"))
    return kb

def mgr_rk():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("👥 Сотрудники"), KeyboardButton("📦 Производство"))
    kb.row(KeyboardButton("🏭 Склад"), KeyboardButton("🚚 Отгрузки"))
    kb.row(KeyboardButton("💰 Финансы"), KeyboardButton("📋 Номенклатура"))
    kb.row(KeyboardButton("👥 Контрагенты"))
    return kb

def send_menu(tid, role, name):
    emoji = {"worker":"Рабочий","manager":"Начальник","admin":"Админ","superadmin":"Главный Админ"}
    if role == "worker":
        bot.send_message(tid, f"{emoji.get(role,'')} {name}", reply_markup=worker_rk())
    else:
        bot.send_message(tid, f"{emoji.get(role,'')} {name}", reply_markup=mgr_rk())

# ─── КОМАНДЫ ──────────────────────────────────────────────────────────────────

@bot.message_handler(commands=["start"])
def cmd_start(m):
    cancel_state(m.from_user.id)
    u = get_user(m.from_user.id)
    if u: send_menu(m.from_user.id, u["role"], u["name"])
    else: bot.send_message(m.from_user.id, f"Не зарегистрированы. Ваш ID: {m.from_user.id}")

@bot.message_handler(commands=["staff"])
def cmd_staff(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    rows = [
        [("📋 Список сотрудников","staff:list")],
        [("👷 Кто сейчас на смене","shift:now")],
        [("📊 Отчёт по сотрудникам","rp:staff")],
    ]
    if u["role"] in ("admin","superadmin"):
        rows.append([("➕ Добавить сотрудника","staff:add")])
        rows.append([("❌ Удалить сотрудника","staff:del:list")])
    bot.send_message(m.from_user.id, "👥 Сотрудники:", reply_markup=ik(*rows))

@bot.message_handler(commands=["production"])
def cmd_production(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    rows = [[("📦 Реестр заказов","ord:list")]]
    if u["role"] in ("admin","superadmin"): rows.append([("➕ Создать заказ","ord:new")])
    rows.append([("📊 Производство за день","pd:start")])
    rows.append([("✅ Подтвердить отгрузку","cs:list")])
    bot.send_message(m.from_user.id, "📦 Производство:", reply_markup=ik(*rows))

@bot.message_handler(commands=["warehouse"])
def cmd_warehouse(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    kb_rows=[[("📊 Текущие остатки","wh:show")]]
    if u["role"] in ("admin","superadmin"): kb_rows.append([("⚙️ Инвентаризация","inv:menu")])
    bot.send_message(m.from_user.id, "🏭 Склад:", reply_markup=ik(*kb_rows))

@bot.message_handler(commands=["shipments"])
def cmd_shipments(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    kb = ik([("📤 Прямая отгрузка","ds:start")],[("📋 Реестр отгрузок","ship:list")])
    bot.send_message(m.from_user.id, "🚚 Отгрузки:", reply_markup=kb)

@bot.message_handler(commands=["finance"])
def cmd_finance(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    kb = ik([("📋 Реестр расходов","exp:list")],[("📊 Финансовый отчет","fin:report")],
            [("💰 Зарплата за месяц","sal:report")],[("💸 Постоянные расходы","exp:fixed:list")],
            [("➕ Переменный расход","exp:var:add")],[("➕ Доплата сотруднику","sal:bonus:start")])
    bot.send_message(m.from_user.id, "💰 Финансы:", reply_markup=kb)

@bot.message_handler(commands=["nomenclature"])
def cmd_nomenclature(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    send_nomenclature(m.from_user.id, u["role"])

@bot.message_handler(commands=["counterparties"])
def cmd_counterparties(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"):
        bot.send_message(m.from_user.id,"Нет доступа."); return
    send_counterparties(m.from_user.id, u["role"])

@bot.message_handler(commands=["myid"])
def cmd_myid(m): bot.send_message(m.from_user.id, f"Ваш ID: {m.from_user.id}")

@bot.message_handler(commands=["menu"])
def cmd_menu(m):
    u = get_user(m.from_user.id)
    if not u: return
    if u["role"] == "worker":
        send_menu(m.from_user.id, u["role"], u["name"]); return
    rows = [
        [("👥 Сотрудники","burger:staff"), ("📦 Производство","burger:prod")],
        [("🏭 Склад","burger:wh"),         ("🚚 Отгрузки","burger:ship")],
        [("💰 Финансы","burger:fin"),      ("📋 Номенклатура","burger:nom")],
        [("👥 Контрагенты","burger:cp")],
    ]
    if u["role"] in ("admin","superadmin"):
        rows.append([("⚙️ Управление","burger:mgmt")])
    bot.send_message(m.from_user.id, "☰ Навигация:", reply_markup=ik(*rows))

@bot.message_handler(commands=["sotrudniki"])
def cmd_sotrudniki(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("manager","admin","superadmin"): btn_staff(m)

@bot.message_handler(commands=["proizv"])
def cmd_proizv(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("manager","admin","superadmin"): btn_prod(m)

@bot.message_handler(commands=["sklad"])
def cmd_sklad(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("manager","admin","superadmin"): btn_warehouse(m)

@bot.message_handler(commands=["otgruzki"])
def cmd_otgruzki(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("admin","superadmin"): btn_shipments(m)

@bot.message_handler(commands=["finansy"])
def cmd_finansy(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("admin","superadmin"): btn_finance(m)

@bot.message_handler(commands=["nomenklat"])
def cmd_nomenklat(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("manager","admin","superadmin"): btn_nom(m)

@bot.message_handler(commands=["kontragenty"])
def cmd_kontragenty(m):
    u = get_user(m.from_user.id)
    if u and u["role"] in ("admin","superadmin"): btn_cp(m)

@bot.message_handler(commands=["cancel"])
def cmd_cancel(m):
    cancel_state(m.from_user.id); u = get_user(m.from_user.id)
    if u: send_menu(m.from_user.id, u["role"], u["name"])
    else: bot.send_message(m.from_user.id, "Отменено.")

@bot.message_handler(commands=["add"])
def cmd_add(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"): bot.send_message(m.from_user.id,"Нет доступа."); return
    parts = m.text.split()
    if len(parts) < 4: bot.send_message(m.from_user.id,"/add [id] [имя фамилия] [роль]\nРоли: worker|manager|admin"); return
    try: tid = int(parts[1])
    except: bot.send_message(m.from_user.id,"ID числом."); return
    role = parts[-1]; name = " ".join(parts[2:-1])
    if role not in ("worker","manager","admin","superadmin"): bot.send_message(m.from_user.id,"Недопустимая роль."); return
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (tid,name,role)); conn.commit()
        bot.send_message(m.from_user.id, f"Добавлен: {name} — {ROLE_LABELS.get(role,role)}")
        try: bot.send_message(tid, f"Вы добавлены как {name}. Нажмите /start")
        except: pass
    except sqlite3.IntegrityError: bot.send_message(m.from_user.id,"Уже есть.")
    finally: conn.close()

@bot.message_handler(commands=["remove"])
def cmd_remove(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"): bot.send_message(m.from_user.id,"Нет доступа."); return
    parts = m.text.split()
    if len(parts) < 2: bot.send_message(m.from_user.id,"/remove [id]"); return
    try: tid = int(parts[1])
    except: bot.send_message(m.from_user.id,"ID числом."); return
    conn = get_db(); t = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not t: bot.send_message(m.from_user.id,"Не найден."); conn.close(); return
    conn.execute("DELETE FROM users WHERE telegram_id=?", (tid,)); conn.commit(); conn.close()
    bot.send_message(m.from_user.id, f"{t['name']} удален.")

@bot.message_handler(commands=["setrate"])
def cmd_setrate(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] != "superadmin": bot.send_message(m.from_user.id,"Только Главный Админ."); return
    parts = m.text.split()
    if len(parts) < 3: bot.send_message(m.from_user.id,"/setrate [id] [сумма/день]"); return
    try: tid=int(parts[1]); rate=float(parts[2])
    except: bot.send_message(m.from_user.id,"Неверный формат."); return
    conn = get_db(); t = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not t: bot.send_message(m.from_user.id,"Не найден."); conn.close(); return
    conn.execute("UPDATE users SET daily_rate=? WHERE telegram_id=?", (rate,tid)); conn.commit(); conn.close()
    bot.send_message(m.from_user.id, f"{t['name']}: {rate:,.0f} руб/день")

# ─── КНОПКИ РАБОЧЕГО ─────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "Пришел на работу")
def btn_checkin(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] != "worker": return
    conn = get_db()
    active = conn.execute("SELECT * FROM time_records WHERE user_id=? AND status='active'", (u["id"],)).fetchone()
    if active: bot.send_message(m.from_user.id,"Уже на работе."); conn.close(); return
    now = datetime.now()
    conn.execute("INSERT INTO time_records (user_id,check_in) VALUES (?,?)", (u["id"],now)); conn.commit(); conn.close()
    bot.send_message(m.from_user.id, "Приход: " + now.strftime("%H:%M %d.%m.%Y"))
    notify(("manager","admin","superadmin"), u["name"] + " пришел в " + now.strftime("%H:%M %d.%m.%Y"))

@bot.message_handler(func=lambda m: m.text == "Ушел с работы")
def btn_checkout(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] != "worker": return
    conn = get_db()
    active = conn.execute("SELECT * FROM time_records WHERE user_id=? AND status='active'", (u["id"],)).fetchone()
    if not active: bot.send_message(m.from_user.id,"Не отмечен как на работе."); conn.close(); return
    now = datetime.now(); hrs = (now - datetime.fromisoformat(active["check_in"])).total_seconds()/3600
    conn.execute("UPDATE time_records SET check_out=?,status='closed' WHERE id=?", (now,active["id"])); conn.commit(); conn.close()
    bot.send_message(m.from_user.id, f"Уход: {now.strftime('%H:%M %d.%m.%Y')}  Отработано: {hrs:.1f} ч.")
    notify(("manager","admin","superadmin"), f"{u['name']} ушел в {now.strftime('%H:%M')} ({hrs:.1f} ч.)")

@bot.message_handler(func=lambda m: m.text == "Моя статистика")
def btn_stats(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] != "worker": return
    now = datetime.now(); ms = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    conn = get_db()
    recs = conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=? AND status='closed'", (u["id"],ms)).fetchall()
    active = conn.execute("SELECT check_in FROM time_records WHERE user_id=? AND status='active'", (u["id"],)).fetchone(); conn.close()
    h = sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in recs)
    d = len({datetime.fromisoformat(r["check_in"]).date() for r in recs})
    rate = u["daily_rate"] or 0; earn = h*(rate/8) if rate>0 else 0
    lines = [f"Статистика {u['name']}", ""]
    if active:
        dt = datetime.fromisoformat(active["check_in"])
        lines.append(f"Сейчас на смене с {dt.strftime('%H:%M')}"); lines.append("")
    lines.append(f"{MONTHS_RU[now.month]} {now.year}: {d} дн. {h:.1f} ч.")
    if rate > 0: lines.append(f"Заработок: {earn:,.0f} руб.")
    bot.send_message(m.from_user.id, "\n".join(lines))

@bot.message_handler(func=lambda m: m.text == "Заявки")
def btn_requests(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] != "worker": return
    kb = ik([("Не выйду на работу","rq:abs")],[("Неисправность","rq:brk")],[("Заявка на МТС","rq:mts")])
    bot.send_message(m.from_user.id, "Тип заявки:", reply_markup=kb)

# ─── КНОПКИ РУКОВОДСТВА ───────────────────────────────────────────────────────

# Заказы теперь внутри раздела Производство

@bot.message_handler(func=lambda m: m.text == "📦 Производство")
def btn_prod(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"): return
    rows = []
    rows.append([("📦 Реестр заказов","ord:list")])
    if u["role"] in ("admin","superadmin"):
        rows.append([("➕ Создать заказ","ord:new")])
    rows.append([("📊 Производство за день","pd:start")])
    rows.append([("✅ Подтвердить отгрузку","cs:list")])
    bot.send_message(m.from_user.id, "📦 Производство:", reply_markup=ik(*rows))

@bot.message_handler(func=lambda m: m.text == "Склад")
def btn_warehouse(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"): return
    conn = get_db(); items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    lines = [f"Склад на {datetime.now().strftime('%d.%m.%Y %H:%M')}:", ""]
    for it in items:
        s = get_stock(it["id"]); icon = "OK" if s>0 else "!"
        lines.append(f"{icon} {it['code']} {it['name']}: {s:,.1f} {it['unit']}")
    kb_rows = []
    if u["role"] in ("admin","superadmin"): kb_rows.append([("Инвентаризация","inv:menu")])
    bot.send_message(m.from_user.id, "\n".join(lines), reply_markup=ik(*kb_rows) if kb_rows else None)

@bot.message_handler(func=lambda m: m.text == "🚚 Отгрузки")
def btn_shipments(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"): return
    kb = ik([("Прямая отгрузка","ds:start")],[("Реестр отгрузок","ship:list")])
    bot.send_message(m.from_user.id, "Отгрузки:", reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == "💰 Финансы")
def btn_finance(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"): return
    kb = ik([("Реестр расходов","exp:list")],[("Финансовый отчет","fin:report")],
            [("Зарплата за месяц","sal:report")],[("Постоянные расходы","exp:fixed:list")],
            [("Переменный расход","exp:var:add")],[("Доплата сотруднику","sal:bonus:start")])
    bot.send_message(m.from_user.id, "Финансы:", reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == "📋 Номенклатура")
def btn_nom(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"): return
    send_nomenclature(m.from_user.id, u["role"])

@bot.message_handler(func=lambda m: m.text == "👥 Контрагенты")
def btn_cp(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("admin","superadmin"): return
    send_counterparties(m.from_user.id, u["role"])

@bot.message_handler(func=lambda m: m.text == "👥 Сотрудники")
def btn_staff(m):
    u = get_user(m.from_user.id)
    if not u or u["role"] not in ("manager","admin","superadmin"): return
    rows = [
        [("📋 Список сотрудников","staff:list")],
        [("👷 Кто сейчас на смене","shift:now")],
        [("📊 Отчёт по сотрудникам","rp:staff")],
    ]
    if u["role"] in ("admin","superadmin"):
        rows.append([("➕ Добавить сотрудника","staff:add")])
        rows.append([("❌ Удалить сотрудника","staff:del:list")])
    bot.send_message(m.from_user.id, "👥 Сотрудники:", reply_markup=ik(*rows))



# ─── СПИСКИ ───────────────────────────────────────────────────────────────────

def send_orders_list(tid, role):
    conn = get_db()
    if role == "manager":
        orders = conn.execute("SELECT o.id,o.number,o.status,o.desired_date,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id WHERE o.status NOT IN ('shipped') ORDER BY o.created_at DESC").fetchall()
    else:
        orders = conn.execute("SELECT o.id,o.number,o.status,o.desired_date,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id ORDER BY o.created_at DESC LIMIT 30").fetchall()
    conn.close()
    kb_rows = []
    if role in ("admin","superadmin"): kb_rows.append([("+ Создать заказ","ord:new")])
    if not orders:
        bot.send_message(tid, "Заказов нет.", reply_markup=ik(*kb_rows) if kb_rows else None); return
    lines = ["Заказы:", ""]
    for o in orders:
        st = ORDER_STATUS.get(o["status"],o["status"]); cp = o["cp_name"] or "-"
        due = f" до {o['desired_date']}" if o["desired_date"] else ""
        lines.append(f"{o['number']} | {cp} | {st}{due}")
        kb_rows.append([(f"{o['number']} — {cp} — {st}", f"ord:v:{o['id']}")])
    bot.send_message(tid, "\n".join(lines), reply_markup=ik(*kb_rows))

def send_nomenclature(tid, role):
    conn = get_db(); items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    kb_rows = []
    if role in ("admin","superadmin"): kb_rows.append([("+ Добавить","nm:add")])
    lines = ["Номенклатура:", ""]
    for it in items:
        lines.append(f"{it['code']} {it['name']} ({it['unit']})")
        kb_rows.append([(f"{it['code']} — {it['name']}", f"nm:v:{it['id']}")])
    bot.send_message(tid, "\n".join(lines), reply_markup=ik(*kb_rows))

def send_counterparties(tid, role):
    conn = get_db(); cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    kb_rows = [[("+ Добавить контрагента","cp:add")]]
    if not cps:
        bot.send_message(tid, "Контрагентов нет.", reply_markup=ik(*kb_rows)); return
    lines = ["Контрагенты:", ""]
    for cp in cps:
        lines.append(f"{cp['code']} {cp['name']}")
        kb_rows.append([(f"{cp['code']} — {cp['name']}", f"cp:v:{cp['id']}")])
    bot.send_message(tid, "\n".join(lines), reply_markup=ik(*kb_rows))

# ─── КОЛБЭКИ ──────────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda c: True)
def handle_cb(call):
    u = get_user(call.from_user.id)
    if not u: bot.answer_callback_query(call.id,"Не зарегистрированы.",show_alert=True); return
    ans(call); cd = call.data; tid = call.from_user.id; role = u["role"]

    # Бургер меню
    if cd == "burger:staff": btn_staff_from_cb(call); return
    if cd == "burger:prod":
        rows = []
        rows.append([("📦 Реестр заказов","ord:list")])
        if role in ("admin","superadmin"): rows.append([("➕ Создать заказ","ord:new")])
        rows.append([("📊 Производство за день","pd:start")])
        rows.append([("✅ Подтвердить отгрузку","cs:list")])
        bot.send_message(tid, "📦 Производство:", reply_markup=ik(*rows)); return
    if cd == "burger:wh":
        kb_rows=[[("📊 Текущие остатки","wh:show")]]
        if role in ("admin","superadmin"): kb_rows.append([("⚙️ Инвентаризация","inv:menu")])
        bot.send_message(tid,"🏭 Склад:",reply_markup=ik(*kb_rows)); return
    if cd == "burger:ship":
        kb=ik([("📤 Прямая отгрузка","ds:start")],[("📋 Реестр отгрузок","ship:list")])
        bot.send_message(tid,"🚚 Отгрузки:",reply_markup=kb); return
    if cd == "burger:fin":
        kb=ik([("📋 Реестр расходов","exp:list")],[("📊 Финансовый отчет","fin:report")],
              [("💰 Зарплата за месяц","sal:report")],[("💸 Постоянные расходы","exp:fixed:list")],
              [("➕ Переменный расход","exp:var:add")],[("➕ Доплата сотруднику","sal:bonus:start")])
        bot.send_message(tid,"💰 Финансы:",reply_markup=kb); return
    if cd == "burger:nom": send_nomenclature(tid,role); return
    if cd == "burger:cp": send_counterparties(tid,role); return
    if cd == "burger:mgmt": show_mgmt(tid); return

    # Раздел Сотрудники
    if cd == "staff:list":
        conn=get_db(); users=conn.execute("SELECT * FROM users ORDER BY role,name").fetchall(); conn.close()
        lines=["👥 Список сотрудников:",""]
        for usr in users:
            lines.append(f"{ROLE_LABELS.get(usr['role'],usr['role'])}: {usr['name']}")
            lines.append(f"  ID: {usr['telegram_id']}")
            if usr["role"]=="worker" and (usr["daily_rate"] or 0)>0:
                lines.append(f"  Ставка: {usr['daily_rate']:,.0f} руб/день")
        bot.send_message(tid,"\\n".join(lines)); return

    if cd == "shift:now":
        conn=get_db()
        rows_db=conn.execute("SELECT t.*,u.name FROM time_records t JOIN users u ON u.id=t.user_id WHERE t.status='active'").fetchall(); conn.close()
        if not rows_db: bot.send_message(tid,"Никого нет на смене."); return
        lines=["👷 Сейчас на смене:",""]
        for r in rows_db:
            dt=datetime.fromisoformat(r["check_in"]); hrs=(datetime.now()-dt).total_seconds()/3600
            lines.append(f"  {r['name']} — с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)")
        bot.send_message(tid,"\n".join(lines)); return

    if cd == "staff:add":
        if role not in ("admin","superadmin"): bot.send_message(tid,"Нет доступа."); return
        user_states[tid]="staff:add:id"; user_data[tid]={}
        bot.send_message(tid,"➕ Добавить сотрудника\n\nВведите Telegram ID:\n(/cancel для отмены)"); return

    if cd == "staff:del:list":
        if role not in ("admin","superadmin"): bot.send_message(tid,"Нет доступа."); return
        conn=get_db(); workers=conn.execute("SELECT * FROM users WHERE role NOT IN ('superadmin') ORDER BY name").fetchall(); conn.close()
        if not workers: bot.send_message(tid,"Нет сотрудников."); return
        rows_kb=[[(f"❌ {w['name']} ({ROLE_LABELS.get(w['role'],w['role'])})",f"staff:del:{w['telegram_id']}")] for w in workers]
        bot.send_message(tid,"Выберите кого удалить:",reply_markup=ik(*rows_kb)); return

    if cd.startswith("staff:role:"):
        role_sel=cd.split(":")[2]; d=user_data.get(tid,{})
        new_tid=d.get("new_tid"); name=d.get("name","")
        if not new_tid or not name: cancel_state(tid); return
        conn=get_db()
        try:
            conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (new_tid,name,role_sel)); conn.commit()
            bot.send_message(tid,f"Добавлен: {name} — {ROLE_LABELS.get(role_sel,role_sel)}")
            try: bot.send_message(new_tid,f"Вы добавлены как {name}. Нажмите /start")
            except: pass
        except sqlite3.IntegrityError: bot.send_message(tid,"Пользователь уже есть.")
        finally: conn.close()
        cancel_state(tid); return

    if cd.startswith("staff:del:"):
        target_tid=int(cd.split(":")[2])
        if role not in ("admin","superadmin"): return
        conn=get_db(); t=conn.execute("SELECT * FROM users WHERE telegram_id=?", (target_tid,)).fetchone(); conn.close()
        if not t: return
        conn=get_db(); conn.execute("DELETE FROM users WHERE telegram_id=?", (target_tid,)); conn.commit(); conn.close()
        bot.send_message(tid,f"{t['name']} удален."); return

    # Склад — показать остатки
    if cd == "wh:show":
        conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
        lines=[f"🏭 Склад на {datetime.now().strftime('%d.%m.%Y %H:%M')}:",""]
        for it in items:
            s=get_stock(it["id"]); icon="✅" if s>0 else "⚠️"
            lines.append(f"{icon} {it['code']} {it['name']}: {s:,.1f} {it['unit']}")
        bot.send_message(tid,"\n".join(lines)); return

    # Списки
    if cd == "ord:list": send_orders_list(tid,role); return
    if cd == "nm:list": send_nomenclature(tid,role); return
    if cd == "cp:list": send_counterparties(tid,role); return

    # Заявки
    if cd in ("rq:abs","rq:brk","rq:mts"):
        t = {"rq:abs":"absence","rq:brk":"breakdown","rq:mts":"mts"}[cd]
        p = {"rq:abs":"Укажи дату и причину:","rq:brk":"Опиши неисправность:","rq:mts":"Опиши что нужно:"}[cd]
        user_states[tid] = f"rq:{t}"
        bot.send_message(tid, p + "\n(/cancel для отмены)"); return

    # Заказы — просмотр
    if cd.startswith("ord:v:"):
        oid = int(cd.split(":")[2]); send_order_detail(tid,oid,role); return

    # Заказы — создать
    if cd == "ord:new":
        if role not in ("admin","superadmin"): return
        start_new_order(tid); return

    # Заказы — смена статуса
    if cd.startswith("ord:s:"):
        parts = cd.split(":"); oid=int(parts[2]); ns=parts[3]
        conn = get_db()
        conn.execute("UPDATE orders SET status=? WHERE id=?", (ns,oid))
        o = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        cr = conn.execute("SELECT * FROM users WHERE id=?", (o["created_by"],)).fetchone()
        conn.commit(); conn.close()
        label = ORDER_STATUS.get(ns,ns)
        msg = f"Заказ {o['number']} -> {label}\n{u['name']}"
        if cr and cr["telegram_id"] != u["telegram_id"]:
            try: bot.send_message(cr["telegram_id"], msg)
            except: pass
        if ns in ("accepted","ready"): notify(("admin","superadmin"), msg, force=True)
        send_order_detail(tid,oid,role); return

    # Заказы — удалить
    if cd.startswith("ord:del:"):
        parts = cd.split(":")
        if parts[2] == "ok":
            oid = int(parts[3])
            conn = get_db()
            conn.execute("DELETE FROM order_items WHERE order_id=?", (oid,))
            conn.execute("DELETE FROM order_comments WHERE order_id=?", (oid,))
            conn.execute("DELETE FROM orders WHERE id=?", (oid,))
            conn.commit(); conn.close()
            bot.send_message(tid, "Заказ удален."); send_orders_list(tid,role)
        else:
            oid = int(parts[2])
            conn = get_db(); o = conn.execute("SELECT number FROM orders WHERE id=?", (oid,)).fetchone(); conn.close()
            num = o["number"] if o else oid
            kb = ik([(f"Да, удалить {num}", f"ord:del:ok:{oid}")],[("Отмена",f"ord:v:{oid}")])
            bot.send_message(tid, f"Удалить заказ {num}?", reply_markup=kb)
        return

    # Заказы — комментарий
    if cd.startswith("ord:comment:"):
        oid = int(cd.split(":")[2]); user_states[tid]=f"ord:comment:{oid}"
        bot.send_message(tid,"Введите комментарий (/cancel для отмены):"); return

    # Заказы — выбор контрагента
    if cd.startswith("cp:sel:"):
        cp_id = int(cd.split(":")[2])
        conn = get_db(); cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
        if not cp: return
        user_data[tid]["cp_id"]=cp_id; user_data[tid]["cp_name"]=cp["name"]
        user_states[tid]="ord:date"
        bot.send_message(tid, f"Контрагент: {cp['name']}\nДата готовности (ДД.ММ.ГГГГ) или -:"); return

    # Заказы — выбор позиции
    if cd.startswith("ni:"):
        nom_id = int(cd.split(":")[1])
        conn = get_db(); nom = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
        if not nom: return
        user_data[tid]["current_nom"]={"id":nom_id,"name":nom["name"],"unit":nom["unit"],"code":nom["code"]}
        user_states[tid]="ord:qty"
        bot.send_message(tid, f"Количество {nom['name']} ({nom['unit']}):"); return

    if cd == "ord:save": save_order(tid,u); return
    if cd == "ord:note":
        user_states[tid]="ord:note"
        bot.send_message(tid,"Примечание (/cancel для отмены):"); return

    # Номенклатура
    if cd.startswith("nm:v:"):
        nom_id = int(cd.split(":")[2]); send_nom_detail(tid,nom_id,role); return
    if cd == "nm:add":
        if role not in ("admin","superadmin"): return
        user_states[tid]="nm:add:name"; user_data[tid]={}
        bot.send_message(tid,"Новая позиция. Название (/cancel для отмены):"); return
    if cd.startswith("nm:edit:"):
        parts=cd.split(":"); nom_id=int(parts[2]); field=parts[3]
        if role not in ("admin","superadmin"): return
        user_states[tid]=f"nm:edit:{nom_id}:{field}"
        bot.send_message(tid,"Введите новое значение (/cancel для отмены):"); return
    if cd.startswith("nm:del:"):
        nom_id=int(cd.split(":")[2])
        if role not in ("admin","superadmin"): return
        conn=get_db(); conn.execute("UPDATE nomenclature SET active=0 WHERE id=?", (nom_id,)); conn.commit(); conn.close()
        bot.send_message(tid,"Позиция удалена."); send_nomenclature(tid,role); return

    # Контрагенты
    if cd.startswith("cp:v:"):
        cp_id=int(cd.split(":")[2]); send_cp_detail(tid,cp_id,role); return
    if cd == "cp:add":
        if role not in ("admin","superadmin"): return
        user_states[tid]="cp:add:name"; user_data[tid]={}
        bot.send_message(tid,"Новый контрагент. Название (/cancel для отмены):"); return
    if cd.startswith("cp:edit:"):
        parts=cd.split(":"); cp_id=int(parts[2]); field=parts[3]
        if role not in ("admin","superadmin"): return
        user_states[tid]=f"cp:edit:{cp_id}:{field}"
        bot.send_message(tid,"Введите новое значение (/cancel для отмены):"); return
    if cd.startswith("cp:del:"):
        cp_id=int(cd.split(":")[2])
        if role not in ("admin","superadmin"): return
        conn=get_db(); conn.execute("UPDATE counterparties SET active=0 WHERE id=?", (cp_id,)); conn.commit(); conn.close()
        bot.send_message(tid,"Контрагент удален."); send_counterparties(tid,role); return

    # Производство за день
    if cd == "pd:start":
        if role not in ("manager","admin","superadmin"): return
        conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
        today=datetime.now().strftime("%d.%m.%Y")
        user_states[tid]="pd:items"; user_data[tid]={"date":today,"items":[]}
        rows=[]
        for i in range(0,len(items),2):
            row=[(f"{items[i]['code']} {items[i]['name'][:20]}", f"pd:n:{items[i]['id']}")]
            if i+1<len(items): row.append((f"{items[i+1]['code']} {items[i+1]['name'][:20]}", f"pd:n:{items[i+1]['id']}"))
            rows.append(row)
        rows.append([("Сохранить","pd:save")])
        bot.send_message(tid, f"Производство за {today}:\nВыберите позицию:", reply_markup=ik(*rows)); return

    if cd.startswith("pd:n:"):
        nom_id=int(cd.split(":")[2])
        conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
        if not nom: return
        user_data[tid]["current_nom"]={"id":nom_id,"name":nom["name"],"unit":nom["unit"]}
        user_states[tid]="pd:qty"
        bot.send_message(tid, f"Количество {nom['name']} ({nom['unit']}) за сегодня:"); return

    if cd == "pd:save":
        d=user_data.get(tid,{}); items=d.get("items",[])
        cancel_state(tid)
        if not items: bot.send_message(tid,"Ничего не введено."); return
        lines=[f"Производство за {d['date']}:",""]
        for it in items: lines.append(f"{it['name']}: {it['qty']:,.1f} {it['unit']}")
        text="\n".join(lines)
        bot.send_message(tid,text); notify(("manager","admin","superadmin"),text); return

    # Инвентаризация
    if cd == "inv:menu":
        kb=ik([("Начальные остатки","inv:init")],[("Корректировка","inv:adj")])
        bot.send_message(tid,"Инвентаризация:",reply_markup=kb); return

    if cd == "inv:init":
        conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
        rows=[[(f"{it['code']} {it['name']}",f"inv:n:{it['id']}")] for it in items]
        user_states[tid]="inv:init"
        bot.send_message(tid,"Выберите позицию для начального остатка:",reply_markup=ik(*rows)); return

    if cd.startswith("inv:n:"):
        nom_id=int(cd.split(":")[2])
        conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
        cur=get_stock(nom_id); user_data[tid]={"nom_id":nom_id,"nom_name":nom["name"],"unit":nom["unit"]}
        user_states[tid]="inv:init:qty"
        bot.send_message(tid, f"{nom['name']}\nТекущий: {cur:,.1f} {nom['unit']}\nВведите начальный остаток:"); return

    if cd == "inv:adj":
        conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
        rows=[[(f"{it['code']} {it['name']}",f"inv:adj:n:{it['id']}")] for it in items]
        user_states[tid]="inv:adj"
        bot.send_message(tid,"Выберите позицию для корректировки:",reply_markup=ik(*rows)); return

    if cd.startswith("inv:adj:n:"):
        nom_id=int(cd.split(":")[3])
        conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
        cur=get_stock(nom_id); user_data[tid]={"nom_id":nom_id,"nom_name":nom["name"],"unit":nom["unit"]}
        kb=ik([(f"+ Прибавить",f"inv:t:add:{nom_id}")],[(f"- Убрать",f"inv:t:sub:{nom_id}")],[(f"= Установить точно",f"inv:t:set:{nom_id}")])
        bot.send_message(tid, f"{nom['name']}\nТекущий: {cur:,.1f} {nom['unit']}\nТип корректировки:", reply_markup=kb); return

    if cd.startswith("inv:t:"):
        parts=cd.split(":"); action=parts[2]; nom_id=int(parts[3])
        user_data[tid]["adj_type"]=action; user_data[tid]["nom_id"]=nom_id
        user_states[tid]="inv:adj:qty"
        labels={"add":"прибавить","sub":"убрать","set":"установить точно"}
        bot.send_message(tid, f"Введите количество ({labels[action]}):"); return

    # Подтверждение отгрузки
    if cd == "cs:list":
        conn=get_db()
        ships=conn.execute("SELECT s.*,o.number as onum FROM shipments s LEFT JOIN orders o ON o.id=s.order_id WHERE s.status='pending'").fetchall()
        conn.close()
        if not ships: bot.send_message(tid,"Нет отгрузок для подтверждения."); return
        rows=[]
        for s in ships:
            stype="Прямая" if s["shipment_type"]=="direct" else f"Заказ {s['onum'] or '-'}"
            rows.append([(f"{s['number']} | {stype}", f"cs:v:{s['id']}")])
        bot.send_message(tid,"Отгрузки для подтверждения:",reply_markup=ik(*rows)); return

    if cd.startswith("cs:v:"):
        sid=int(cd.split(":")[2])
        conn=get_db()
        ship=conn.execute("SELECT s.*,o.number as onum FROM shipments s LEFT JOIN orders o ON o.id=s.order_id WHERE s.id=?", (sid,)).fetchone()
        sitems=conn.execute(
            "SELECT si.quantity,COALESCE(n.name,n2.name) as nm,COALESCE(n.unit,n2.unit) as nu "
            "FROM shipment_items si LEFT JOIN order_items oi ON oi.id=si.order_item_id "
            "LEFT JOIN nomenclature n ON n.id=oi.nomenclature_id LEFT JOIN nomenclature n2 ON n2.id=si.nomenclature_id "
            "WHERE si.shipment_id=?", (sid,)
        ).fetchall(); conn.close()
        if not ship: return
        lines=[f"{ship['number']}",f"Заказ: {ship['onum'] or 'Прямая'}",f"Дата: {fmt(ship['created_at'])}","","Позиции:"]
        for si in sitems: lines.append(f"  {si['nm']}: {si['quantity']:,.1f} {si['nu']}")
        kb=ik([(f"Подтвердить {ship['number']}",f"cs:ok:{sid}")])
        bot.send_message(tid,"\n".join(lines),reply_markup=kb); return

    if cd.startswith("cs:ok:"):
        sid=int(cd.split(":")[2]); now=datetime.now()
        conn=get_db()
        ship=conn.execute("SELECT * FROM shipments WHERE id=?", (sid,)).fetchone()
        sitems=conn.execute("SELECT si.*,oi.order_id FROM shipment_items si LEFT JOIN order_items oi ON oi.id=si.order_item_id WHERE si.shipment_id=?", (sid,)).fetchall()
        for si in sitems:
            if si["order_item_id"]:
                conn.execute("UPDATE order_items SET shipped_qty=COALESCE(shipped_qty,0)+? WHERE id=?", (si["quantity"],si["order_item_id"]))
        conn.execute("UPDATE shipments SET status='confirmed',confirmed_by=?,confirmed_at=? WHERE id=?", (u["id"],now,sid))
        if ship["order_id"]:
            conn.execute("UPDATE orders SET status='shipped' WHERE id=?", (ship["order_id"],))
        conn.commit(); conn.close()
        bot.send_message(tid,f"Отгрузка {ship['number']} подтверждена!"); return

    # Реестр отгрузок
    if cd == "ship:list":
        conn=get_db()
        ships=conn.execute("SELECT s.*,c.name as cp_name FROM shipments s LEFT JOIN counterparties c ON c.id=s.counterparty_id ORDER BY s.created_at DESC LIMIT 30").fetchall()
        conn.close()
        if not ships: bot.send_message(tid,"Отгрузок нет."); return
        lines=["Реестр отгрузок:",""]
        rows=[]
        for s in ships:
            st="Подтверждена" if s["status"]=="confirmed" else "Ожидает"
            cp=s["cp_name"] or "-"; lines.append(f"{s['number']} | {cp} | {st}")
            rows.append([(f"{s['number']} — {cp} — {st}", f"ship:v:{s['id']}")])
        bot.send_message(tid,"\n".join(lines),reply_markup=ik(*rows)); return

    if cd.startswith("ship:v:"):
        sid=int(cd.split(":")[2])
        conn=get_db()
        s=conn.execute("SELECT s.*,c.name as cp_name FROM shipments s LEFT JOIN counterparties c ON c.id=s.counterparty_id WHERE s.id=?", (sid,)).fetchone()
        sitems=conn.execute(
            "SELECT si.quantity,si.sale_price,COALESCE(n.name,n2.name) as nm,COALESCE(n.unit,n2.unit) as nu "
            "FROM shipment_items si LEFT JOIN order_items oi ON oi.id=si.order_item_id "
            "LEFT JOIN nomenclature n ON n.id=oi.nomenclature_id LEFT JOIN nomenclature n2 ON n2.id=si.nomenclature_id "
            "WHERE si.shipment_id=?", (sid,)
        ).fetchall(); conn.close()
        if not s: return
        st="Подтверждена" if s["status"]=="confirmed" else "Ожидает"
        lines=[f"{s['number']}",f"Контрагент: {s['cp_name'] or '-'}",f"Дата: {fmt(s['created_at'])}",f"Статус: {st}","","Позиции:"]
        total=0
        for si in sitems:
            rev=(si["sale_price"] or 0)*si["quantity"]; total+=rev
            lines.append(f"  {si['nm']}: {si['quantity']:,.1f} x {si['sale_price'] or 0:,.2f} = {rev:,.2f}")
        lines.append(f"\nИтого: {total:,.2f} руб.")
        bot.send_message(tid,"\\n".join(lines)); return

    # Прямая отгрузка
    if cd == "ds:start":
        if role not in ("admin","superadmin"): return
        conn=get_db(); cps=conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
        if not cps: bot.send_message(tid,"Сначала добавьте контрагента."); return
        user_states[tid]="ds:cp"; user_data[tid]={"items":[]}
        rows=[[(f"{cp['code']} — {cp['name']}", f"ds:cp:{cp['id']}")] for cp in cps]
        bot.send_message(tid,"Прямая отгрузка. Выберите контрагента:",reply_markup=ik(*rows)); return

    if cd.startswith("ds:cp:"):
        cp_id=int(cd.split(":")[2])
        conn=get_db(); cp=conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
        if not cp: return
        user_data[tid]["cp_id"]=cp_id; user_data[tid]["cp_name"]=cp["name"]
        user_states[tid]="ds:date"
        bot.send_message(tid, f"Контрагент: {cp['name']}\nДата отгрузки (ДД.ММ.ГГГГ) или - для сегодня:"); return

    if cd.startswith("ds:ni:"):
        nom_id=int(cd.split(":")[2])
        conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
        if not nom: return
        stock=get_stock(nom_id)
        user_data[tid]["current_nom"]={"id":nom_id,"name":nom["name"],"unit":nom["unit"],"code":nom["code"],"sale_price":nom["sale_price"] or 0}
        user_states[tid]="ds:qty"
        bot.send_message(tid, f"{nom['name']} ({nom['unit']})\nСклад: {stock:,.1f}\nКоличество:"); return

    if cd == "ds:save":
        d=user_data.get(tid,{})
        if not d.get("items"): bot.send_message(tid,"Добавьте позицию."); return
        shortages=[]
        for it in d["items"]:
            s=get_stock(it["nom_id"])
            if s<it["qty"]: shortages.append({"name":it["name"],"unit":it["unit"],"need":it["qty"],"stock":s,"short":it["qty"]-s})
        if shortages:
            lines=["Не хватает товара:",""]
            for sh in shortages: lines.append(f"{sh['name']}: нужно {sh['need']:,.1f}, склад {sh['stock']:,.1f}, не хватает {sh['short']:,.1f} {sh['unit']}")
            user_data[tid]["shortages"]=shortages
            kb=ik([("Отгрузить что есть + автозаказ","ds:partial")],[("Подождать","ds:wait")],[("Отменить","ds:cancel")])
            bot.send_message(tid,"\n".join(lines),reply_markup=kb); return
        create_direct_shipment(tid,d,u); return

    if cd == "ds:partial":
        d=user_data.get(tid,{}); shortages=d.get("shortages",[])
        create_direct_shipment(tid,d,u,partial=True)
        now=datetime.now(); num=next_order_num()
        conn=get_db()
        conn.execute("INSERT INTO orders (number,counterparty_id,created_by,created_at,status,notes) VALUES (?,?,?,?,'new','Автозаказ нехватка')",
                     (num,d.get("cp_id"),u["id"],now))
        oid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for sh in shortages:
            nom=conn.execute("SELECT id FROM nomenclature WHERE name=?", (sh["name"],)).fetchone()
            if nom: conn.execute("INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)", (oid,nom["id"],sh["short"]))
        conn.commit(); conn.close()
        bot.send_message(tid,f"Отгрузка создана. Автозаказ {num} на недостающее.")
        notify(("manager","admin","superadmin"),f"Автозаказ {num} создан — нехватка при отгрузке",force=True)
        cancel_state(tid); return

    if cd == "ds:wait": cancel_state(tid); bot.send_message(tid,"Отгрузка отложена."); return
    if cd == "ds:cancel": cancel_state(tid); bot.send_message(tid,"Отгрузка отменена."); return

    # Финансы
    if cd == "exp:list":
        now=datetime.now(); ms=now.replace(day=1).strftime("%d.%m.%Y")
        conn=get_db()
        lines=["Расходы " + MONTHS_RU[now.month] + " " + str(now.year) + ":", "", "Постоянные:"]
        tf=0
        for etype,elabel in FIXED_TYPES:
            last=conn.execute("SELECT amount FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 1",(etype,)).fetchone()
            if last and last["amount"]>0: lines.append(f"  {elabel}: {last['amount']:,.2f}"); tf+=last["amount"]
        lines.append(f"Итого постоянные: {tf:,.2f}"); lines.append(""); lines.append("Переменные:")
        var=conn.execute("SELECT * FROM variable_expenses WHERE expense_date>=? ORDER BY expense_date DESC",(ms,)).fetchall()
        tv=0
        for r in var: lines.append(f"  {r['expense_date']} {r['category']}: {r['amount']:,.2f}"); tv+=r["amount"]
        lines.append(f"Итого переменные: {tv:,.2f}"); lines.append(""); lines.append(f"Всего: {tf+tv:,.2f} руб.")
        conn.close(); bot.send_message(tid,"\\n".join(lines)); return

    if cd == "fin:report":
        now=datetime.now(); ms=now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        conn=get_db()
        ships=conn.execute("SELECT id FROM shipments WHERE status='confirmed' AND created_at>=?",(ms,)).fetchall()
        tr=0; tc=0
        for s in ships:
            sitems=conn.execute(
                "SELECT si.quantity,si.sale_price,COALESCE(n.cost_price,n2.cost_price,0) as cp "
                "FROM shipment_items si LEFT JOIN order_items oi ON oi.id=si.order_item_id "
                "LEFT JOIN nomenclature n ON n.id=oi.nomenclature_id LEFT JOIN nomenclature n2 ON n2.id=si.nomenclature_id "
                "WHERE si.shipment_id=?",(s["id"],)
            ).fetchall()
            for si in sitems: tr+=(si["sale_price"] or 0)*si["quantity"]; tc+=(si["cp"] or 0)*si["quantity"]
        tf=sum((conn.execute("SELECT COALESCE(amount,0) FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 1",(t[0],)).fetchone() or [0])[0] for t in FIXED_TYPES)
        tv=conn.execute("SELECT COALESCE(SUM(amount),0) FROM variable_expenses WHERE expense_date>=?",(now.replace(day=1).strftime("%d.%m.%Y"),)).fetchone()[0]
        conn.close()
        profit=tr-tc-tf-tv
        lines=["Финансовый отчет " + MONTHS_RU[now.month] + " " + str(now.year) + ":", "",
               f"Сумма отгрузок: {tr:,.2f}", f"Себестоимость: {tc:,.2f}",
               f"Расходы пост.: {tf:,.2f}", f"Расходы пер.: {tv:,.2f}", "",
               f"Прибыль: {profit:,.2f} руб."]
        bot.send_message(tid,"\\n".join(lines)); return

    if cd == "sal:report":
        now=datetime.now(); ms=now.replace(day=1,hour=0,minute=0,second=0,microsecond=0); month_str=now.strftime("%Y-%m")
        conn=get_db(); workers=conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall(); conn.close()
        lines=["Зарплата " + MONTHS_RU[now.month] + " " + str(now.year) + ":", ""]; tf=0
        for w in workers:
            conn=get_db()
            recs=conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=? AND status='closed'",(w["id"],ms)).fetchall()
            bonuses=conn.execute("SELECT * FROM salary_bonuses WHERE user_id=? AND month=?",(w["id"],month_str)).fetchall(); conn.close()
            h=sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in recs)
            rate=w["daily_rate"] or 0; auto=h*(rate/8) if rate>0 else 0
            bonus_sum=sum(b["amount"] for b in bonuses); total=auto+bonus_sum; tf+=total
            line=f"{w['name']}: {h:.1f}ч"
            if rate>0: line+=f" авто={auto:,.0f}"
            if bonus_sum>0: line+=f" доп={bonus_sum:,.0f}"
            line+=f" итого={total:,.0f}"
            lines.append(line)
        lines.append(""); lines.append(f"ФОТ: {tf:,.2f} руб.")
        bot.send_message(tid,"\\n".join(lines)); return

    if cd == "exp:fixed:list":
        conn=get_db(); rows=[]
        for etype,elabel in FIXED_TYPES:
            last=conn.execute("SELECT amount,effective_from FROM fixed_expenses WHERE type=? ORDER BY created_at DESC LIMIT 1",(etype,)).fetchone()
            amount=last["amount"] if last else 0; eff=last["effective_from"] if last else "-"
            rows.append([(f"{elabel}: {amount:,.0f} (с {eff})",f"exp:fe:{etype}")])
        conn.close(); bot.send_message(tid,"Постоянные расходы:",reply_markup=ik(*rows)); return

    if cd.startswith("exp:fe:"):
        etype=cd.split(":")[2]; user_states[tid]=f"exp:fixed:{etype}:amount"; user_data[tid]={"etype":etype}
        labels=dict(FIXED_TYPES); bot.send_message(tid, f"{labels.get(etype,etype)} — введите сумму (руб.):"); return

    if cd == "exp:var:add":
        kb=ik([("Материалы","exp:vc:Материалы"),("Транспорт","exp:vc:Транспорт")],
              [("Ремонт","exp:vc:Ремонт"),("Расходники","exp:vc:Расходники")],
              [("Офис","exp:vc:Офис"),("Прочее","exp:vc:Прочее")])
        user_states[tid]="exp:var:cat"; user_data[tid]={}
        bot.send_message(tid,"Категория расхода:",reply_markup=kb); return

    if cd.startswith("exp:vc:"):
        cat=cd.split(":")[2]; user_data[tid]["category"]=cat; user_states[tid]="exp:var:amount"
        bot.send_message(tid, f"Категория: {cat}\nВведите сумму (руб.):"); return

    if cd == "sal:bonus:start":
        conn=get_db(); workers=conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall(); conn.close()
        if not workers: bot.send_message(tid,"Нет рабочих."); return
        rows=[[(w["name"],f"sal:u:{w['id']}")] for w in workers]
        user_states[tid]="sal:bonus:user"; user_data[tid]={}
        bot.send_message(tid,"Выберите сотрудника:",reply_markup=ik(*rows)); return

    if cd.startswith("sal:u:"):
        uid=int(cd.split(":")[2])
        conn=get_db(); w=conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone(); conn.close()
        if not w: return
        user_data[tid]["uid"]=uid; user_data[tid]["uname"]=w["name"]
        user_states[tid]="sal:bonus:amount"
        bot.send_message(tid, f"{w['name']}\nВведите сумму доплаты (руб.):"); return

    # Отчет по сотрудникам
    if cd == "rp:staff":
        now=datetime.now(); ms=now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        conn=get_db(); workers=conn.execute("SELECT * FROM users WHERE role='worker' ORDER BY name").fetchall(); conn.close()
        if not workers: bot.send_message(tid,"Нет рабочих."); return
        lines=["Отчет по сотрудникам " + MONTHS_RU[now.month] + " " + str(now.year) + ":", ""]
        for w in workers:
            conn=get_db()
            recs=conn.execute("SELECT * FROM time_records WHERE user_id=? AND check_in>=? AND status='closed'",(w["id"],ms)).fetchall(); conn.close()
            h=sum((datetime.fromisoformat(r["check_out"])-datetime.fromisoformat(r["check_in"])).total_seconds()/3600 for r in recs)
            d=len({datetime.fromisoformat(r["check_in"]).date() for r in recs})
            rate=w["daily_rate"] or 0; earn=h*(rate/8) if rate>0 else 0
            line=f"{w['name']}: {d} дн. {h:.1f} ч."
            if rate>0: line+=f"  {earn:,.0f} руб."
            lines.append(line)
        bot.send_message(tid,"\\n".join(lines)); return

# ─── ВСПОМОГАТЕЛЬНЫЕ ─────────────────────────────────────────────────────────

def btn_staff_from_cb(call):
    u = get_user(call.from_user.id)
    if not u: return
    rows = [
        [("📋 Список сотрудников","staff:list")],
        [("👷 Кто сейчас на смене","shift:now")],
        [("📊 Отчёт по сотрудникам","rp:staff")],
    ]
    if u["role"] in ("admin","superadmin"):
        rows.append([("➕ Добавить сотрудника","staff:add")])
        rows.append([("❌ Удалить сотрудника","staff:del:list")])
    bot.send_message(call.from_user.id, "👥 Сотрудники:", reply_markup=ik(*rows))

def show_mgmt(tid):
    conn = get_db(); users = conn.execute("SELECT * FROM users ORDER BY role,name").fetchall(); conn.close()
    lines = ["⚙️ Управление:", ""]
    for usr in users:
        lines.append(f"{ROLE_LABELS.get(usr['role'],usr['role'])}: {usr['name']}")
        lines.append(f"  ID: {usr['telegram_id']}")
    lines.append("\n/add [id] [имя] [роль]")
    lines.append("/remove [id]")
    lines.append("/setrate [id] [сумма]")
    bot.send_message(tid, "\n".join(lines))

# ─── ДЕТАЛИ ───────────────────────────────────────────────────────────────────

def send_order_detail(tid, oid, role):
    conn = get_db()
    o = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o: conn.close(); bot.send_message(tid,"Заказ не найден."); return
    cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (o["counterparty_id"],)).fetchone()
    cr = conn.execute("SELECT name FROM users WHERE id=?", (o["created_by"],)).fetchone()
    items = conn.execute("SELECT oi.quantity,n.name,n.unit,n.code FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id WHERE oi.order_id=?", (oid,)).fetchall()
    comments = conn.execute("SELECT oc.text,oc.created_at,u.name as uname FROM order_comments oc JOIN users u ON u.id=oc.user_id WHERE oc.order_id=? ORDER BY oc.created_at DESC LIMIT 5", (oid,)).fetchall()
    conn.close()
    status = ORDER_STATUS.get(o["status"],o["status"])
    lines = [f"Заказ {o['number']}",
             f"Контрагент: {cp['name'] if cp else '-'} ({cp['code'] if cp else '-'})",
             f"Создан: {fmt(o['created_at'])}",
             f"Готовность: {o['desired_date'] or '-'}",
             f"Создал: {cr['name'] if cr else '-'}",
             f"Статус: {status}"]
    if o["notes"]: lines.append(f"Примечание: {o['notes']}")
    lines.append(""); lines.append("Позиции:")
    for it in items: lines.append(f"  {it['code']} {it['name']} — {it['quantity']:,.1f} {it['unit']}")
    if comments:
        lines.append(""); lines.append("Комментарии:")
        for com in comments: lines.append(f"  {com['uname']}: {com['text']}\n  {fmt(com['created_at'])}")
    kb_rows = []
    NEXT = {"new":("Принять",f"ord:s:{oid}:accepted"),"accepted":("В работу",f"ord:s:{oid}:in_progress"),"in_progress":("Готово",f"ord:s:{oid}:ready")}
    if o["status"] in NEXT: kb_rows.append([NEXT[o["status"]]])
    if role in ("admin","superadmin"): kb_rows.append([("Удалить заказ",f"ord:del:{oid}")])
    kb_rows.append([("Добавить комментарий",f"ord:comment:{oid}")])
    kb_rows.append([("Назад к заказам","ord:list")])
    bot.send_message(tid, "\n".join(lines), reply_markup=ik(*kb_rows))

def send_nom_detail(tid, nom_id, role):
    conn = get_db(); it = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); conn.close()
    if not it: bot.send_message(tid,"Не найдено."); return
    lines = [f"{it['code']} {it['name']}",f"Ед.изм.: {it['unit']}",f"Примечание: {it['notes'] or '-'}",
             f"Себестоимость: {it['cost_price'] or 0:,.2f}",f"Прайс: {it['sale_price'] or 0:,.2f}",f"Добавлена: {fmt(it['created_at'])}"]
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([("Изменить название",f"nm:edit:{nom_id}:name"),("Изменить прим.",f"nm:edit:{nom_id}:notes")])
        kb_rows.append([("Себестоимость",f"nm:edit:{nom_id}:cost_price"),("Прайс",f"nm:edit:{nom_id}:sale_price")])
        kb_rows.append([("Удалить",f"nm:del:{nom_id}")])
    kb_rows.append([("Назад","nm:list")])
    bot.send_message(tid, "\n".join(lines), reply_markup=ik(*kb_rows))

def send_cp_detail(tid, cp_id, role):
    conn = get_db(); cp = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone(); conn.close()
    if not cp: bot.send_message(tid,"Не найдено."); return
    lines = [f"{cp['code']} {cp['name']}",f"Телефон: {cp['phone'] or '-'}",f"Email: {cp['email'] or '-'}",
             f"Адрес: {cp['address'] or '-'}",f"Примечание: {cp['notes'] or '-'}",f"Добавлен: {fmt(cp['created_at'])}"]
    kb_rows = []
    if role in ("admin","superadmin"):
        kb_rows.append([("Изм. название",f"cp:edit:{cp_id}:name"),("Изм. телефон",f"cp:edit:{cp_id}:phone")])
        kb_rows.append([("Изм. email",f"cp:edit:{cp_id}:email"),("Изм. адрес",f"cp:edit:{cp_id}:address")])
        kb_rows.append([("Изм. примечание",f"cp:edit:{cp_id}:notes"),("Удалить",f"cp:del:{cp_id}")])
    kb_rows.append([("Назад","cp:list")])
    bot.send_message(tid, "\n".join(lines), reply_markup=ik(*kb_rows))

# ─── СОЗДАНИЕ ЗАКАЗА ──────────────────────────────────────────────────────────

def start_new_order(tid):
    conn = get_db(); cps = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall(); conn.close()
    if not cps: bot.send_message(tid,"Сначала добавьте контрагента."); return
    user_states[tid]="ord:cp"; user_data[tid]={"items":[]}
    rows=[[(f"{cp['code']} — {cp['name']}",f"cp:sel:{cp['id']}")] for cp in cps]
    bot.send_message(tid,"Новый заказ. Выберите контрагента:",reply_markup=ik(*rows))

def show_item_picker(tid):
    d=user_data.get(tid,{}); added=d.get("items",[])
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    lines=["Добавьте позиции:",""]
    if added:
        for it in added: lines.append(f"  {it['code']} {it['name']} — {it['qty']:,.1f} {it['unit']}")
        lines.append("")
    if d.get("notes"): lines.append(f"Примечание: {d['notes']}"); lines.append("")
    rows=[]
    for i in range(0,len(items),2):
        row=[(f"{items[i]['code']} {items[i]['name'][:22]}",f"ni:{items[i]['id']}")]
        if i+1<len(items): row.append((f"{items[i+1]['code']} {items[i+1]['name'][:22]}",f"ni:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("Примечание","ord:note"),("Сохранить заказ","ord:save")])
    prev=d.get("picker_msg_id")
    if prev:
        try: bot.delete_message(tid,prev)
        except: pass
    msg=bot.send_message(tid,"\n".join(lines),reply_markup=ik(*rows))
    user_data[tid]["picker_msg_id"]=msg.message_id

def save_order(tid, u):
    d=user_data.get(tid,{})
    if not d.get("items"): bot.send_message(tid,"Добавьте позицию."); return
    now=datetime.now(); num=next_order_num()
    conn=get_db()
    conn.execute("INSERT INTO orders (number,counterparty_id,created_by,created_at,desired_date,status,notes) VALUES (?,?,?,?,?,'new',?)",
                 (num,d["cp_id"],u["id"],now,d.get("desired_date"),d.get("notes")))
    oid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]: conn.execute("INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)", (oid,it["nom_id"],it["qty"]))
    conn.commit(); conn.close()
    cancel_state(tid)
    bot.send_message(tid, f"Заказ {num} создан!\nКонтрагент: {d['cp_name']}\nПозиций: {len(d['items'])}")
    lines=[f"Новый заказ {num}",f"Контрагент: {d['cp_name']}",f"Готовность: {d.get('desired_date') or '-'}","","Позиции:"]
    for it in d["items"]: lines.append(f"  {it['code']} {it['name']} — {it['qty']:,.1f} {it['unit']}")
    notify(("manager","admin","superadmin"),"\n".join(lines),force=True)

# ─── ПРЯМАЯ ОТГРУЗКА ──────────────────────────────────────────────────────────

def show_ds_picker(tid):
    d=user_data.get(tid,{}); added=d.get("items",[])
    conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
    lines=["Прямая отгрузка — позиции:",""]
    if added:
        for it in added: lines.append(f"  {it['code']} {it['name']} — {it['qty']:,.1f} x {it['price']:,.2f}")
        lines.append("")
    rows=[]
    for i in range(0,len(items),2):
        row=[(f"{items[i]['code']} {items[i]['name'][:22]}",f"ds:ni:{items[i]['id']}")]
        if i+1<len(items): row.append((f"{items[i+1]['code']} {items[i+1]['name'][:22]}",f"ds:ni:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("Оформить отгрузку","ds:save")])
    prev=d.get("picker_msg_id")
    if prev:
        try: bot.delete_message(tid,prev)
        except: pass
    msg=bot.send_message(tid,"\n".join(lines),reply_markup=ik(*rows))
    user_data[tid]["picker_msg_id"]=msg.message_id

def create_direct_shipment(tid, d, u, partial=False):
    now=datetime.now(); num=next_ship_num()
    conn=get_db()
    conn.execute("INSERT INTO shipments (number,created_by,created_at,ship_date,status,shipment_type,notes,counterparty_id) VALUES (?,?,?,?,'pending','direct',?,?)",
                 (num,u["id"],now,d.get("ship_date"),d.get("notes"),d.get("cp_id")))
    sid=conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for it in d["items"]:
        qty=min(it["qty"],get_stock(it["nom_id"])) if partial else it["qty"]
        if qty>0: conn.execute("INSERT INTO shipment_items (shipment_id,nomenclature_id,quantity,sale_price) VALUES (?,?,?,?)", (sid,it["nom_id"],qty,it.get("price",0)))
    conn.commit(); conn.close()
    cancel_state(tid)
    bot.send_message(tid, f"Отгрузка {num} создана. Ожидает подтверждения.")
    notify(("manager",),f"Прямая отгрузка {num} — подтвердите в разделе Производство",force=True)

# ─── ТЕКСТОВЫЙ ВВОД ───────────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.from_user.id in user_states)
def handle_text(message):
    tid=message.from_user.id; state=user_states.get(tid,""); text=message.text.strip(); u=get_user(tid)
    if not u: return

    # Добавление сотрудника через диалог
    if state == "staff:add:id":
        try: new_tid=int(text)
        except: bot.send_message(tid,"ID должен быть числом."); return
        user_data[tid]["new_tid"]=new_tid; user_states[tid]="staff:add:name"
        bot.send_message(tid,"Введите имя и фамилию:"); return

    if state == "staff:add:name":
        user_data[tid]["name"]=text; user_states[tid]="staff:add:role"
        roles_allowed = ("worker","manager","admin") if u["role"]=="superadmin" else ("worker","manager")
        rows=[[(f"{ROLE_LABELS.get(r,r)}",f"staff:role:{r}")] for r in roles_allowed]
        bot.send_message(tid,"Выберите роль:",reply_markup=ik(*rows)); return

    # Заявки
    if state.startswith("rq:"):
        rtype=state.replace("rq:",""); labels={"absence":"Не выйдет","breakdown":"Неисправность","mts":"МТС"}
        cancel_state(tid)
        notify(("manager","admin","superadmin"), f"{u['name']} — {labels.get(rtype,rtype)}: {text}",force=True)
        bot.send_message(tid,"Заявка отправлена."); return

    # Создание заказа
    if state == "ord:date":
        user_data[tid]["desired_date"]=None if text in ("-","пропустить") else text
        user_states[tid]="ord:items"; show_item_picker(tid); return

    if state == "ord:qty":
        try: qty=float(text.replace(",",".")); assert qty>0
        except: bot.send_message(tid,"Введите число больше нуля."); return
        d=user_data[tid]; nom=d.pop("current_nom")
        d["items"].append({"nom_id":nom["id"],"name":nom["name"],"unit":nom["unit"],"code":nom["code"],"qty":qty})
        user_states[tid]="ord:items"; bot.send_message(tid, f"{nom['name']} — {qty:,.1f} {nom['unit']} добавлено")
        show_item_picker(tid); return

    if state == "ord:note":
        user_data[tid]["notes"]=text; user_states[tid]="ord:items"
        bot.send_message(tid,"Примечание добавлено."); show_item_picker(tid); return

    if state.startswith("ord:comment:"):
        oid=int(state.split(":")[2])
        conn=get_db(); conn.execute("INSERT INTO order_comments (order_id,user_id,text) VALUES (?,?,?)", (oid,u["id"],text)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"Комментарий добавлен."); send_order_detail(tid,oid,u["role"]); return

    # Прямая отгрузка
    if state == "ds:date":
        user_data[tid]["ship_date"]=datetime.now().strftime("%d.%m.%Y") if text=="-" else text
        user_states[tid]="ds:items"; show_ds_picker(tid); return

    if state == "ds:qty":
        try: qty=float(text.replace(",",".")); assert qty>0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; nom=d.pop("current_nom"); dp=nom.get("sale_price",0)
        user_data[tid]["pending_item"]={"nom_id":nom["id"],"name":nom["name"],"unit":nom["unit"],"code":nom["code"],"qty":qty,"price":dp}
        user_states[tid]="ds:price"
        bot.send_message(tid, f"{nom['name']} — {qty:,.1f}\nЦена продажи (прайс: {dp:,.2f}) или - для прайса:"); return

    if state == "ds:price":
        d=user_data[tid]; item=d.pop("pending_item")
        if text!="-":
            try: item["price"]=float(text.replace(",",".")); assert item["price"]>=0
            except: bot.send_message(tid,"Введите число или -."); user_data[tid]["pending_item"]=item; return
        d["items"].append(item); user_states[tid]="ds:items"
        bot.send_message(tid, f"Добавлено: {item['name']} x {item['qty']:,.1f} x {item['price']:,.2f}")
        show_ds_picker(tid); return

    # Производство за день
    if state == "pd:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; nom=d.pop("current_nom"); today=d["date"]; now=datetime.now()
        conn=get_db()
        ex=conn.execute("SELECT * FROM daily_production WHERE date=? AND nomenclature_id=? AND recorded_by=?", (today,nom["id"],u["id"])).fetchone()
        if ex: conn.execute("UPDATE daily_production SET quantity=quantity+?,recorded_at=? WHERE id=?", (qty,now,ex["id"]))
        else: conn.execute("INSERT INTO daily_production (date,nomenclature_id,quantity,recorded_by,recorded_at) VALUES (?,?,?,?,?)", (today,nom["id"],qty,u["id"],now))
        updated=conn.execute("SELECT dp.*,n.name,n.unit FROM daily_production dp JOIN nomenclature n ON n.id=dp.nomenclature_id WHERE dp.date=? AND dp.recorded_by=?", (today,u["id"])).fetchall()
        conn.commit(); conn.close()
        d["items"]=[{"name":e["name"],"unit":e["unit"],"qty":e["quantity"]} for e in updated]
        user_states[tid]="pd:items"; bot.send_message(tid, f"{nom['name']}: {qty:,.1f} {nom['unit']} записано")
        # Снова показываем пикер
        conn=get_db(); items=conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall(); conn.close()
        rows=[]
        for i in range(0,len(items),2):
            row=[(f"{items[i]['code']} {items[i]['name'][:20]}",f"pd:n:{items[i]['id']}")]
            if i+1<len(items): row.append((f"{items[i+1]['code']} {items[i+1]['name'][:20]}",f"pd:n:{items[i+1]['id']}"))
            rows.append(row)
        rows.append([("Сохранить","pd:save")])
        lines=[f"Производство за {today}:",""]
        for it in d["items"]: lines.append(f"{it['name']}: {it['qty']:,.1f} {it['unit']}")
        bot.send_message(tid,"\n".join(lines)+"\n\nДобавить ещё:",reply_markup=ik(*rows)); return

    # Инвентаризация
    if state == "inv:init:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; conn=get_db()
        conn.execute("UPDATE nomenclature SET initial_stock=? WHERE id=?", (qty,d["nom_id"])); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid, f"{d['nom_name']}: начальный остаток {qty:,.1f} {d['unit']}"); return

    if state == "inv:adj:qty":
        try: qty=float(text.replace(",",".")); assert qty>=0
        except: bot.send_message(tid,"Введите число."); return
        d=user_data[tid]; adj_type=d.get("adj_type","add"); nom_id=d["nom_id"]
        conn=get_db(); nom=conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone(); cur=get_stock(nom_id)
        if adj_type=="set": diff=qty-cur; atype="add" if diff>=0 else "sub"; aval=abs(diff)
        elif adj_type=="add": atype="add"; aval=qty
        else: atype="sub"; aval=qty
        conn.execute("INSERT INTO stock_adjustments (nomenclature_id,quantity,type,comment,created_by) VALUES (?,?,?,?,?)", (nom_id,aval,atype,"Инвентаризация",u["id"]))
        conn.commit(); conn.close(); new_s=get_stock(nom_id)
        cancel_state(tid); bot.send_message(tid, f"{nom['name']}\nБыло: {cur:,.1f} — Стало: {new_s:,.1f} {nom['unit']}"); return

    # Номенклатура
    if state == "nm:add:name": user_data[tid]["name"]=text; user_states[tid]="nm:add:unit"; bot.send_message(tid,"Единица измерения (м, шт, кг...):"); return
    if state == "nm:add:unit": user_data[tid]["unit"]=text; user_states[tid]="nm:add:notes"; bot.send_message(tid,"Примечание (или - пропустить):"); return
    if state == "nm:add:notes":
        d=user_data[tid]; notes=None if text=="-" else text
        conn=get_db()
        try:
            last=conn.execute("SELECT code FROM nomenclature WHERE code LIKE 'НОМ-%' ORDER BY id DESC LIMIT 1").fetchone()
            num=int(last["code"].split("-")[1])+1 if last else 14
            code=f"НОМ-{num:03d}"
            conn.execute("INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)", (code,d["name"],d["unit"],notes)); conn.commit()
            bot.send_message(tid, f"Добавлено: {code} {d['name']}")
        except Exception as e: bot.send_message(tid, f"Ошибка: {e}")
        finally: conn.close()
        cancel_state(tid); return

    if state.startswith("nm:edit:"):
        parts=state.split(":"); nom_id=int(parts[2]); field=parts[3]
        conn=get_db(); conn.execute(f"UPDATE nomenclature SET {field}=? WHERE id=?", (text,nom_id)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"Обновлено."); send_nom_detail(tid,nom_id,u["role"]); return

    # Контрагенты
    if state == "cp:add:name": user_data[tid]["name"]=text; user_states[tid]="cp:add:phone"; bot.send_message(tid,"Телефон (или - пропустить):"); return
    if state == "cp:add:phone": user_data[tid]["phone"]=None if text=="-" else text; user_states[tid]="cp:add:email"; bot.send_message(tid,"Email (или - пропустить):"); return
    if state == "cp:add:email": user_data[tid]["email"]=None if text=="-" else text; user_states[tid]="cp:add:address"; bot.send_message(tid,"Адрес отгрузки (или - пропустить):"); return
    if state == "cp:add:address": user_data[tid]["address"]=None if text=="-" else text; user_states[tid]="cp:add:notes"; bot.send_message(tid,"Примечание (или - пропустить):"); return
    if state == "cp:add:notes":
        d=user_data[tid]; notes=None if text=="-" else text; code=next_cp_code()
        conn=get_db()
        try:
            conn.execute("INSERT INTO counterparties (code,name,phone,email,address,notes) VALUES (?,?,?,?,?,?)", (code,d["name"],d.get("phone"),d.get("email"),d.get("address"),notes)); conn.commit()
            bot.send_message(tid, f"Контрагент добавлен: {code} {d['name']}")
        except Exception as e: bot.send_message(tid, f"Ошибка: {e}")
        finally: conn.close()
        cancel_state(tid); return

    if state.startswith("cp:edit:"):
        parts=state.split(":"); cp_id=int(parts[2]); field=parts[3]
        conn=get_db(); conn.execute(f"UPDATE counterparties SET {field}=? WHERE id=?", (text,cp_id)); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid,"Обновлено."); send_cp_detail(tid,cp_id,u["role"]); return

    # Постоянные расходы
    if state.startswith("exp:fixed:") and state.endswith(":amount"):
        etype=state.split(":")[2]
        try: amount=float(text.replace(",",".")); assert amount>=0
        except: bot.send_message(tid,"Введите сумму."); return
        user_data[tid]["amount"]=amount; user_states[tid]=f"exp:fixed:{etype}:from"
        bot.send_message(tid,"С какой даты (ДД.ММ.ГГГГ) или - для сегодня:"); return

    if state.startswith("exp:fixed:") and state.endswith(":from"):
        etype=state.split(":")[2]; eff=datetime.now().strftime("%d.%m.%Y") if text=="-" else text; d=user_data[tid]
        conn=get_db(); conn.execute("INSERT INTO fixed_expenses (type,amount,description,changed_by,effective_from) VALUES (?,?,?,?,?)", (etype,d["amount"],"",u["id"],eff)); conn.commit(); conn.close()
        labels=dict(FIXED_TYPES); cancel_state(tid); bot.send_message(tid, f"{labels.get(etype,etype)}: {d['amount']:,.2f} руб. с {eff}"); return

    # Переменные расходы
    if state == "exp:var:amount":
        try: amount=float(text.replace(",",".")); assert amount>=0
        except: bot.send_message(tid,"Введите сумму."); return
        user_data[tid]["amount"]=amount; user_states[tid]="exp:var:desc"; bot.send_message(tid,"Описание (или - пропустить):"); return

    if state == "exp:var:desc":
        d=user_data[tid]; desc=None if text=="-" else text; today=datetime.now().strftime("%d.%m.%Y")
        conn=get_db(); conn.execute("INSERT INTO variable_expenses (category,amount,description,expense_date,created_by) VALUES (?,?,?,?,?)", (d["category"],d["amount"],desc,today,u["id"])); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid, f"{d['category']}: {d['amount']:,.2f} руб. добавлено"); return

    # Доплата
    if state == "sal:bonus:amount":
        try: amount=float(text.replace(",",".")); assert amount>=0
        except: bot.send_message(tid,"Введите сумму."); return
        user_data[tid]["amount"]=amount; user_states[tid]="sal:bonus:desc"; bot.send_message(tid,"Описание (или - пропустить):"); return

    if state == "sal:bonus:desc":
        d=user_data[tid]; desc=None if text=="-" else text; month_str=datetime.now().strftime("%Y-%m")
        conn=get_db(); conn.execute("INSERT INTO salary_bonuses (user_id,amount,description,month,created_by) VALUES (?,?,?,?,?)", (d["uid"],d["amount"],desc,month_str,u["id"])); conn.commit(); conn.close()
        cancel_state(tid); bot.send_message(tid, f"Доплата {d['uname']}: {d['amount']:,.2f} руб."); return

# ─── НАПОМИНАНИЯ ──────────────────────────────────────────────────────────────

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
                if count==0 and elapsed>=3600 and is_work_time():
                    try: bot.send_message(rec["telegram_id"],"Напоминание: ты не отметил уход. Нажми Ушел с работы")
                    except: pass
                    conn.execute("UPDATE time_records SET reminder_count=1,last_reminder=? WHERE id=?", (now,rec["id"])); conn.commit()
                elif count==1 and last and (now-last).total_seconds()>=3600 and is_work_time():
                    try: bot.send_message(rec["telegram_id"],"Второе напоминание: отметь уход!")
                    except: pass
                    conn.execute("UPDATE time_records SET reminder_count=2,last_reminder=? WHERE id=?", (now,rec["id"])); conn.commit()
                elif count>=2 and last and (now-last).total_seconds()>=3600:
                    conn.execute("UPDATE time_records SET status='no_checkout',check_out=? WHERE id=?", (now,rec["id"])); conn.commit()
                    try: bot.send_message(rec["telegram_id"],"Смена закрыта автоматически.")
                    except: pass
                    notify(("manager","admin","superadmin"),f"{rec['name']} не отметил уход. Смена с {dt.strftime('%H:%M %d.%m')}.",force=True)
            # Напоминание о новых заказах каждые 2 часа
            new_orders=conn.execute(
                "SELECT o.*,c.name as cp_name FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id "
                "WHERE o.status='new' AND o.created_at<=?", (now-timedelta(hours=2),)
            ).fetchall()
            if new_orders and is_work_time():
                managers=conn.execute("SELECT telegram_id FROM users WHERE role='manager'").fetchall()
                for o in new_orders:
                    msg=f"Напоминание! Заказ {o['number']} от {o['cp_name'] or '-'} ожидает принятия."
                    for mgr in managers:
                        try: bot.send_message(mgr["telegram_id"],msg)
                        except: pass
            conn.close()
        except Exception as e: print(f"[reminder] {e}")

threading.Thread(target=reminder_loop, daemon=True).start()
print("Bot started!")
bot.infinity_polling()

