import os
import sqlite3
from datetime import datetime
from telebot import TeleBot
from telebot.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
bot = TeleBot(BOT_TOKEN)
DB_PATH = "/app/data/orders.db"

ROLE_LABELS = {
    "manager":    "👔 Начальник Цеха",
    "admin":      "⚙️ Администратор",
    "superadmin": "👑 Главный Админ",
}

ORDER_STATUS_LABELS = {
    "new":         "🆕 Новый",
    "accepted":    "✅ Принят",
    "in_progress": "🔧 В работе",
    "ready":       "🏁 Готов",
}

INITIAL_NOMENCLATURE = [
    ("НОМ-001", "Гарпун Вид 1 (уз) белый",    "м",  "Намотка 200м"),
    ("НОМ-002", "Гарпун Вид 1 (уз) чёрный",   "м",  "Намотка 200м"),
    ("НОМ-003", "Гарпун Вид 2 (шир) белый",   "м",  "Намотка 200м"),
    ("НОМ-004", "Гарпун Вид 2 (шир) чёрный",  "м",  "Намотка 200м"),
    ("НОМ-005", "Вставка Т Элит",             "м",  "Первичное сырьё, намотка 50/150м"),
    ("НОМ-006", "Вставка Т чёрная",           "м",  "Намотка 50/150м"),
    ("НОМ-007", "Вставка Т",                  "м",  "Намотка 50/150м"),
    ("НОМ-008", "Вставка Уголок белая",       "м",  "Намотка 50/100м"),
    ("НОМ-009", "Вставка Уголок чёрная",      "м",  "Намотка 50/100м"),
    ("НОМ-010", "Багет ПВХ 150 гм",          "м",  "Вид 1, для пистолета"),
    ("НОМ-011", "Багет ПВХ 140 гм",          "м",  "Вид 2"),
    ("НОМ-012", "Платформа 60-110",           "шт", "Серая, 50шт/короб"),
    ("НОМ-013", "Платформа 90",               "шт", "Серая, 50шт/короб"),
]

INITIAL_USERS = [(915402089, "Katran 150", "superadmin")]

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
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS nomenclature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            notes TEXT,
            active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            counterparty_id INTEGER NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            desired_date TEXT,
            status TEXT DEFAULT 'new',
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            nomenclature_id INTEGER NOT NULL,
            quantity REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS order_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    if conn.execute("SELECT COUNT(*) FROM nomenclature").fetchone()[0] == 0:
        conn.executemany(
            "INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)",
            INITIAL_NOMENCLATURE
        )

    for tid, name, role in INITIAL_USERS:
        ex = conn.execute("SELECT id FROM users WHERE telegram_id=?", (tid,)).fetchone()
        if ex:
            conn.execute("UPDATE users SET role=? WHERE telegram_id=?", (role, tid))
        else:
            conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (tid, name, role))

    conn.commit()
    conn.close()
    print("DB ready")

init_db()

# ═══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════════════════

def get_user(tid):
    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    conn.close()
    return u

def c(val): return val or "-"

def esc(s):
    if not s: return ""
    for ch in ["*", "_", "`", "[", "]"]:
        s = str(s).replace(ch, "\\" + ch)
    return s

def fmt_dt(s):
    if not s: return "-"
    try: return datetime.fromisoformat(s).strftime("%d.%m.%Y %H:%M")
    except: return str(s)

def cancel_state(tid):
    user_states.pop(tid, None)
    user_data.pop(tid, None)

def next_order_number():
    conn = get_db()
    year = datetime.now().year
    last = conn.execute(
        "SELECT number FROM orders WHERE number LIKE ? ORDER BY id DESC LIMIT 1",
        (f"{year}-%",)
    ).fetchone()
    conn.close()
    num = int(last["number"].split("-")[1]) + 1 if last else 1
    return f"{year}-{num:03d}"

def next_cp_code():
    conn = get_db()
    last = conn.execute(
        "SELECT code FROM counterparties WHERE code LIKE 'БОТ-%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if last:
        try: num = int(last["code"].split("-")[1]) + 1
        except: num = 1
    else:
        num = 1
    return f"БОТ-{num:03d}"

def notify_roles(roles, text):
    conn = get_db()
    ph   = ",".join("?" * len(roles))
    rows = conn.execute(f"SELECT telegram_id FROM users WHERE role IN ({ph})", roles).fetchall()
    conn.close()
    for r in rows:
        try: bot.send_message(r["telegram_id"], text, parse_mode="Markdown")
        except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════════════════════

def ik(*rows):
    kb = InlineKeyboardMarkup()
    for row in rows:
        kb.row(*[InlineKeyboardButton(t, callback_data=cd) for t, cd in row])
    return kb

def ans(call):
    try: bot.answer_callback_query(call.id)
    except: pass

def main_rk():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📦 Заказы"), KeyboardButton("📋 Номенклатура"))
    kb.row(KeyboardButton("👥 Контрагенты"), KeyboardButton("⚙️ Управление"))
    return kb

def send_menu(tid, role, name):
    emoji = {"manager": "👔", "admin": "⚙️", "superadmin": "👑"}
    bot.send_message(
        tid,
        f"{emoji.get(role, '')} Привет, {name}!",
        reply_markup=main_rk()
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
        bot.send_message(
            message.from_user.id,
            f"Вы не зарегистрированы.\nВаш ID: <code>{message.from_user.id}</code>",
            parse_mode="HTML"
        )

@bot.message_handler(commands=["myid"])
def cmd_myid(message):
    bot.send_message(
        message.from_user.id,
        f"Ваш ID: <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    cancel_state(message.from_user.id)
    user = get_user(message.from_user.id)
    if user:
        send_menu(message.from_user.id, user["role"], user["name"])
    else:
        bot.send_message(message.from_user.id, "Отменено.")

@bot.message_handler(commands=["add"])
def cmd_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin", "superadmin"):
        bot.send_message(message.from_user.id, "Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 4:
        bot.send_message(
            message.from_user.id,
            "/add [id] [имя фамилия] [роль]\nРоли: manager | admin"
        ); return
    try: tid = int(parts[1])
    except: bot.send_message(message.from_user.id, "ID должен быть числом."); return
    role = parts[-1]
    name = " ".join(parts[2:-1])
    if role not in ("manager", "admin", "superadmin"):
        bot.send_message(message.from_user.id, "Недопустимая роль."); return
    conn = get_db()
    try:
        conn.execute("INSERT INTO users (telegram_id,name,role) VALUES (?,?,?)", (tid, name, role))
        conn.commit()
        bot.send_message(message.from_user.id, f"Добавлен: {name} — {ROLE_LABELS.get(role, role)}")
        try: bot.send_message(tid, f"Вы добавлены как {name}. Нажмите /start")
        except: pass
    except sqlite3.IntegrityError:
        bot.send_message(message.from_user.id, "Пользователь уже есть.")
    finally:
        conn.close()

@bot.message_handler(commands=["remove"])
def cmd_remove(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin", "superadmin"):
        bot.send_message(message.from_user.id, "Нет доступа."); return
    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.from_user.id, "/remove [id]"); return
    try: tid = int(parts[1])
    except: bot.send_message(message.from_user.id, "ID числом."); return
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not target:
        bot.send_message(message.from_user.id, "Не найден."); conn.close(); return
    conn.execute("DELETE FROM users WHERE telegram_id=?", (tid,))
    conn.commit(); conn.close()
    bot.send_message(message.from_user.id, f"{target['name']} удалён.")

# ═══════════════════════════════════════════════════════════════════════════════
# КНОПКИ МЕНЮ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "📦 Заказы")
def btn_orders(message):
    user = get_user(message.from_user.id)
    if not user: return
    send_orders_list(message.from_user.id, user["role"])

@bot.message_handler(func=lambda m: m.text == "📋 Номенклатура")
def btn_nomenclature(message):
    user = get_user(message.from_user.id)
    if not user: return
    send_nomenclature(message.from_user.id, user["role"])

@bot.message_handler(func=lambda m: m.text == "👥 Контрагенты")
def btn_counterparties(message):
    user = get_user(message.from_user.id)
    if not user: return
    send_counterparties(message.from_user.id, user["role"])

@bot.message_handler(func=lambda m: m.text == "⚙️ Управление")
def btn_management(message):
    user = get_user(message.from_user.id)
    if not user: return
    conn = get_db()
    users = conn.execute("SELECT * FROM users ORDER BY role, name").fetchall()
    conn.close()
    text = "Сотрудники:\n"
    for u in users:
        text += f"\n{ROLE_LABELS.get(u['role'], u['role'])}: {u['name']}"
        text += f"\nID: {u['telegram_id']}"
    text += "\n\n/add [id] [имя] [роль]\n/remove [id]"
    bot.send_message(message.from_user.id, text)

# ═══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИИ ОТПРАВКИ СПИСКОВ
# ═══════════════════════════════════════════════════════════════════════════════

def send_orders_list(tid, role):
    conn = get_db()
    if role == "manager":
        orders = conn.execute(
            "SELECT o.id, o.number, o.status, o.desired_date, c.name as cp_name "
            "FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id "
            "WHERE o.status != 'ready' ORDER BY o.created_at DESC"
        ).fetchall()
    else:
        orders = conn.execute(
            "SELECT o.id, o.number, o.status, o.desired_date, c.name as cp_name "
            "FROM orders o LEFT JOIN counterparties c ON c.id=o.counterparty_id "
            "ORDER BY o.created_at DESC LIMIT 30"
        ).fetchall()
    conn.close()

    kb_rows = []
    if role in ("admin", "superadmin"):
        kb_rows.append([("+ Создать заказ", "ord:new")])

    if not orders:
        text = "Заказов нет."
    else:
        text = "Заказы:\n"
        for o in orders:
            st  = ORDER_STATUS_LABELS.get(o["status"], o["status"])
            cp  = o["cp_name"] or "-"
            due = f" до {o['desired_date']}" if o["desired_date"] else ""
            text += f"\n{o['number']} | {cp} | {st}{due}"
            kb_rows.append([(f"{o['number']} — {cp} — {st}", f"ord:v:{o['id']}")])

    bot.send_message(tid, text, reply_markup=ik(*kb_rows) if kb_rows else None)

def send_nomenclature(tid, role):
    conn = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall()
    conn.close()

    kb_rows = []
    if role in ("admin", "superadmin"):
        kb_rows.append([("+ Добавить позицию", "nm:add")])

    text = "Номенклатура:\n"
    for it in items:
        text += f"\n{it['code']} {it['name']} ({it['unit']})"
        kb_rows.append([(f"{it['code']} — {it['name']}", f"nm:v:{it['id']}")])

    bot.send_message(tid, text, reply_markup=ik(*kb_rows) if kb_rows else None)

def send_counterparties(tid, role):
    conn = get_db()
    cps  = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall()
    conn.close()

    kb_rows = []
    if role in ("admin", "superadmin"):
        kb_rows.append([("+ Добавить контрагента", "cp:add")])

    if not cps:
        text = "Контрагентов нет."
    else:
        text = "Контрагенты:\n"
        for cp in cps:
            text += f"\n{cp['code']} {cp['name']}"
            kb_rows.append([(f"{cp['code']} — {cp['name']}", f"cp:v:{cp['id']}")])

    bot.send_message(tid, text, reply_markup=ik(*kb_rows) if kb_rows else None)

# ═══════════════════════════════════════════════════════════════════════════════
# ИНЛАЙН КОЛБЭКИ
# ═══════════════════════════════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    user = get_user(call.from_user.id)
    if not user:
        bot.answer_callback_query(call.id, "Не зарегистрированы.", show_alert=True)
        return
    ans(call)
    cd   = call.data
    tid  = call.from_user.id
    role = user["role"]

    # Списки
    if cd == "ord:list":
        send_orders_list(tid, role); return
    if cd == "nm:list":
        send_nomenclature(tid, role); return
    if cd == "cp:list":
        send_counterparties(tid, role); return

    # Просмотр заказа
    if cd.startswith("ord:v:"):
        oid = int(cd.split(":")[2])
        send_order_detail(tid, oid, role); return

    # Создать заказ
    if cd == "ord:new":
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        start_new_order(tid); return

    # Смена статуса
    if cd.startswith("ord:s:"):
        parts      = cd.split(":")
        oid        = int(parts[2])
        new_status = parts[3]
        change_status(tid, oid, new_status, user)
        send_order_detail(tid, oid, role); return

    # Удалить заказ — подтверждение
    if cd.startswith("ord:del:"):
        parts = cd.split(":")
        if parts[2] == "ok":
            oid = int(parts[3])
            conn = get_db()
            conn.execute("DELETE FROM order_items WHERE order_id=?", (oid,))
            conn.execute("DELETE FROM order_comments WHERE order_id=?", (oid,))
            conn.execute("DELETE FROM orders WHERE id=?", (oid,))
            conn.commit(); conn.close()
            bot.send_message(tid, "Заказ удалён.")
            send_orders_list(tid, role)
        else:
            oid  = int(parts[2])
            conn = get_db()
            o    = conn.execute("SELECT number FROM orders WHERE id=?", (oid,)).fetchone()
            conn.close()
            num  = o["number"] if o else oid
            kb   = ik(
                [(f"Да, удалить {num}", f"ord:del:ok:{oid}")],
                [("Отмена", f"ord:v:{oid}")]
            )
            bot.send_message(tid, f"Удалить заказ {num}?", reply_markup=kb)
        return

    # Комментарий
    if cd.startswith("ord:comment:"):
        oid = int(cd.split(":")[2])
        user_states[tid] = f"ord:comment:{oid}"
        bot.send_message(tid, "Введите комментарий (/cancel для отмены):")
        return

    # Выбор контрагента при создании заказа
    if cd.startswith("cp:sel:"):
        cp_id = int(cd.split(":")[2])
        conn  = get_db()
        cp    = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone()
        conn.close()
        if not cp: return
        user_data[tid]["cp_id"]   = cp_id
        user_data[tid]["cp_name"] = cp["name"]
        user_states[tid] = "ord:date"
        bot.send_message(
            tid,
            f"Контрагент: {cp['name']}\n\nВведите дату готовности (ДД.ММ.ГГГГ) или - чтобы пропустить:"
        )
        return

    # Выбор позиции номенклатуры
    if cd.startswith("ni:"):
        nom_id = int(cd.split(":")[1])
        conn   = get_db()
        nom    = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
        conn.close()
        if not nom: return
        user_data[tid]["current_nom"] = {
            "id": nom_id, "name": nom["name"],
            "unit": nom["unit"], "code": nom["code"]
        }
        user_states[tid] = "ord:qty"
        bot.send_message(tid, f"Количество {nom['name']} ({nom['unit']}):")
        return

    # Сохранить заказ
    if cd == "ord:save":
        d = user_data.get(tid, {})
        if not d.get("items"):
            bot.send_message(tid, "Добавьте хотя бы одну позицию."); return
        save_order(tid, user); return

    # Примечание к заказу
    if cd == "ord:note":
        user_states[tid] = "ord:note"
        bot.send_message(tid, "Введите примечание (/cancel для отмены):")
        return

    # Просмотр номенклатуры
    if cd.startswith("nm:v:"):
        nom_id = int(cd.split(":")[2])
        send_nom_detail(tid, nom_id, role); return

    # Добавить номенклатуру
    if cd == "nm:add":
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        user_states[tid] = "nm:add:name"
        user_data[tid]   = {}
        bot.send_message(tid, "Новая позиция.\n\nВведите название (/cancel для отмены):")
        return

    # Редактировать номенклатуру
    if cd.startswith("nm:edit:"):
        parts  = cd.split(":")
        nom_id = int(parts[2])
        field  = parts[3]
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        user_states[tid] = f"nm:edit:{nom_id}:{field}"
        bot.send_message(tid, f"Введите новое значение (/cancel для отмены):")
        return

    # Удалить номенклатуру
    if cd.startswith("nm:del:"):
        nom_id = int(cd.split(":")[2])
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        conn = get_db()
        conn.execute("UPDATE nomenclature SET active=0 WHERE id=?", (nom_id,))
        conn.commit(); conn.close()
        bot.send_message(tid, "Позиция удалена.")
        send_nomenclature(tid, role); return

    # Просмотр контрагента
    if cd.startswith("cp:v:"):
        cp_id = int(cd.split(":")[2])
        send_cp_detail(tid, cp_id, role); return

    # Добавить контрагента
    if cd == "cp:add":
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        user_states[tid] = "cp:add:name"
        user_data[tid]   = {}
        bot.send_message(tid, "Новый контрагент.\n\nВведите название (/cancel для отмены):")
        return

    # Редактировать контрагента
    if cd.startswith("cp:edit:"):
        parts = cd.split(":")
        cp_id = int(parts[2])
        field = parts[3]
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        user_states[tid] = f"cp:edit:{cp_id}:{field}"
        bot.send_message(tid, f"Введите новое значение (/cancel для отмены):")
        return

    # Удалить контрагента
    if cd.startswith("cp:del:"):
        cp_id = int(cd.split(":")[2])
        if role not in ("admin", "superadmin"):
            bot.send_message(tid, "Нет доступа."); return
        conn = get_db()
        conn.execute("UPDATE counterparties SET active=0 WHERE id=?", (cp_id,))
        conn.commit(); conn.close()
        bot.send_message(tid, "Контрагент удалён.")
        send_counterparties(tid, role); return

# ═══════════════════════════════════════════════════════════════════════════════
# ДЕТАЛИ
# ═══════════════════════════════════════════════════════════════════════════════

def send_order_detail(tid, oid, role):
    conn = get_db()
    o = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    if not o:
        conn.close()
        bot.send_message(tid, "Заказ не найден."); return
    cp    = conn.execute("SELECT * FROM counterparties WHERE id=?", (o["counterparty_id"],)).fetchone()
    cr    = conn.execute("SELECT name FROM users WHERE id=?", (o["created_by"],)).fetchone()
    items = conn.execute(
        "SELECT oi.quantity, n.name, n.unit, n.code "
        "FROM order_items oi JOIN nomenclature n ON n.id=oi.nomenclature_id "
        "WHERE oi.order_id=?", (oid,)
    ).fetchall()
    comments = conn.execute(
        "SELECT oc.text, oc.created_at, u.name as uname "
        "FROM order_comments oc JOIN users u ON u.id=oc.user_id "
        "WHERE oc.order_id=? ORDER BY oc.created_at DESC LIMIT 5",
        (oid,)
    ).fetchall()
    conn.close()

    status = ORDER_STATUS_LABELS.get(o["status"], o["status"])
    text   = (
        f"Заказ {o['number']}\n"
        f"Контрагент: {cp['name'] if cp else '-'} ({c(cp['code']) if cp else '-'})\n"
        f"Создан: {fmt_dt(o['created_at'])}\n"
        f"Готовность: {o['desired_date'] or '-'}\n"
        f"Создал: {cr['name'] if cr else '-'}\n"
        f"Статус: {status}\n"
    )
    if o["notes"]:
        text += f"Примечание: {o['notes']}\n"
    text += "\nПозиции:\n"
    for it in items:
        text += f"  {it['code']} {it['name']} — {it['quantity']:,.1f} {it['unit']}\n"
    if comments:
        text += "\nКомментарии:\n"
        for com in comments:
            text += f"  {com['uname']}: {com['text']}\n"
            text += f"  {fmt_dt(com['created_at'])}\n"

    kb_rows = []
    NEXT_STATUS = {
        "new":         ("Принять",   f"ord:s:{oid}:accepted"),
        "accepted":    ("В работу",  f"ord:s:{oid}:in_progress"),
        "in_progress": ("Готово",    f"ord:s:{oid}:ready"),
    }
    if o["status"] in NEXT_STATUS:
        label, cb = NEXT_STATUS[o["status"]]
        kb_rows.append([(label, cb)])

    if role in ("admin", "superadmin"):
        kb_rows.append([("Удалить заказ", f"ord:del:{oid}")])

    kb_rows.append([("Добавить комментарий", f"ord:comment:{oid}")])
    kb_rows.append([("Назад к заказам", "ord:list")])

    bot.send_message(tid, text, reply_markup=ik(*kb_rows))

def send_nom_detail(tid, nom_id, role):
    conn = get_db()
    it   = conn.execute("SELECT * FROM nomenclature WHERE id=?", (nom_id,)).fetchone()
    conn.close()
    if not it:
        bot.send_message(tid, "Не найдено."); return

    text = (
        f"{it['code']} {it['name']}\n"
        f"Единица: {it['unit']}\n"
        f"Примечание: {it['notes'] or '-'}\n"
        f"Добавлена: {fmt_dt(it['created_at'])}"
    )
    kb_rows = []
    if role in ("admin", "superadmin"):
        kb_rows.append([("Изменить название", f"nm:edit:{nom_id}:name")])
        kb_rows.append([("Изменить примечание", f"nm:edit:{nom_id}:notes")])
        kb_rows.append([("Удалить", f"nm:del:{nom_id}")])
    kb_rows.append([("Назад", "nm:list")])

    bot.send_message(tid, text, reply_markup=ik(*kb_rows))

def send_cp_detail(tid, cp_id, role):
    conn = get_db()
    cp   = conn.execute("SELECT * FROM counterparties WHERE id=?", (cp_id,)).fetchone()
    conn.close()
    if not cp:
        bot.send_message(tid, "Не найдено."); return

    text = (
        f"{cp['code']} {cp['name']}\n"
        f"Телефон: {cp['phone'] or '-'}\n"
        f"Email: {cp['email'] or '-'}\n"
        f"Адрес: {cp['address'] or '-'}\n"
        f"Примечание: {cp['notes'] or '-'}\n"
        f"Добавлен: {fmt_dt(cp['created_at'])}"
    )
    kb_rows = []
    if role in ("admin", "superadmin"):
        kb_rows.append([("Изменить название", f"cp:edit:{cp_id}:name")])
        kb_rows.append([("Изменить телефон", f"cp:edit:{cp_id}:phone")])
        kb_rows.append([("Изменить адрес", f"cp:edit:{cp_id}:address")])
        kb_rows.append([("Удалить", f"cp:del:{cp_id}")])
    kb_rows.append([("Назад", "cp:list")])

    bot.send_message(tid, text, reply_markup=ik(*kb_rows))

# ═══════════════════════════════════════════════════════════════════════════════
# СОЗДАНИЕ ЗАКАЗА
# ═══════════════════════════════════════════════════════════════════════════════

def start_new_order(tid):
    conn = get_db()
    cps  = conn.execute("SELECT * FROM counterparties WHERE active=1 ORDER BY name").fetchall()
    conn.close()
    if not cps:
        bot.send_message(tid, "Сначала добавьте контрагента (кнопка Контрагенты)."); return
    user_states[tid] = "ord:cp"
    user_data[tid]   = {"items": []}
    rows = [[(f"{cp['code']} — {cp['name']}", f"cp:sel:{cp['id']}")] for cp in cps]
    bot.send_message(tid, "Новый заказ. Выберите контрагента:", reply_markup=ik(*rows))

def show_item_picker(tid):
    d     = user_data.get(tid, {})
    added = d.get("items", [])
    conn  = get_db()
    items = conn.execute("SELECT * FROM nomenclature WHERE active=1 ORDER BY code").fetchall()
    conn.close()

    text = "Добавьте позиции в заказ:\n"
    if added:
        text += "\nДобавлено:\n"
        for it in added:
            text += f"  {it['code']} {it['name']} — {it['qty']:,.1f} {it['unit']}\n"
    if d.get("notes"):
        text += f"\nПримечание: {d['notes']}"

    rows = []
    for i in range(0, len(items), 2):
        row = [(f"{items[i]['code']} {items[i]['name'][:22]}", f"ni:{items[i]['id']}")]
        if i + 1 < len(items):
            row.append((f"{items[i+1]['code']} {items[i+1]['name'][:22]}", f"ni:{items[i+1]['id']}"))
        rows.append(row)
    rows.append([("Примечание", "ord:note"), ("Сохранить заказ", "ord:save")])

    prev = d.get("picker_msg_id")
    if prev:
        try: bot.delete_message(tid, prev)
        except: pass
    msg = bot.send_message(tid, text, reply_markup=ik(*rows))
    user_data[tid]["picker_msg_id"] = msg.message_id

def save_order(tid, user):
    d      = user_data.get(tid, {})
    now    = datetime.now()
    number = next_order_number()
    conn   = get_db()
    try:
        conn.execute(
            "INSERT INTO orders (number,counterparty_id,created_by,created_at,desired_date,status,notes) "
            "VALUES (?,?,?,?,?,'new',?)",
            (number, d["cp_id"], user["id"], now, d.get("desired_date"), d.get("notes"))
        )
        oid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for it in d["items"]:
            conn.execute(
                "INSERT INTO order_items (order_id,nomenclature_id,quantity) VALUES (?,?,?)",
                (oid, it["nom_id"], it["qty"])
            )
        conn.commit()
    finally:
        conn.close()

    cancel_state(tid)
    bot.send_message(
        tid,
        f"Заказ {number} создан!\n"
        f"Контрагент: {d['cp_name']}\n"
        f"Позиций: {len(d['items'])}"
    )

    msg = (
        f"Новый заказ {number}\n"
        f"Контрагент: {d['cp_name']}\n"
        f"Готовность: {d.get('desired_date') or '-'}\n"
        f"Дата: {now.strftime('%d.%m.%Y %H:%M')}\n\nПозиции:\n"
    )
    for it in d["items"]:
        msg += f"  {it['code']} {it['name']} — {it['qty']:,.1f} {it['unit']}\n"
    notify_roles(("manager", "admin", "superadmin"), msg)

def change_status(tid, oid, new_status, user):
    conn = get_db()
    try:
        conn.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
        order   = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        creator = conn.execute("SELECT * FROM users WHERE id=?", (order["created_by"],)).fetchone()
        conn.commit()
        label = ORDER_STATUS_LABELS.get(new_status, new_status)
        msg   = f"Заказ {order['number']} — {label}\nИзменил: {user['name']}"
        if creator and creator["telegram_id"] != user["telegram_id"]:
            try: bot.send_message(creator["telegram_id"], msg)
            except: pass
        if new_status in ("accepted", "ready"):
            notify_roles(("admin", "superadmin"), msg)
    finally:
        conn.close()

# ═══════════════════════════════════════════════════════════════════════════════
# ТЕКСТОВЫЙ ВВОД
# ═══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.from_user.id in user_states)
def handle_text(message):
    tid   = message.from_user.id
    state = user_states.get(tid, "")
    text  = message.text.strip()
    user  = get_user(tid)
    if not user: return

    if state == "ord:date":
        user_data[tid]["desired_date"] = None if text in ("-", "пропустить") else text
        user_states[tid] = "ord:items"
        show_item_picker(tid); return

    if state == "ord:qty":
        try:
            qty = float(text.replace(",", "."))
            assert qty > 0
        except:
            bot.send_message(tid, "Введите число больше нуля."); return
        d   = user_data[tid]
        nom = d.pop("current_nom")
        d["items"].append({
            "nom_id": nom["id"], "name": nom["name"],
            "unit": nom["unit"], "code": nom["code"], "qty": qty
        })
        user_states[tid] = "ord:items"
        bot.send_message(tid, f"{nom['name']} — {qty:,.1f} {nom['unit']} добавлено")
        show_item_picker(tid); return

    if state == "ord:note":
        user_data[tid]["notes"] = text
        user_states[tid] = "ord:items"
        bot.send_message(tid, "Примечание добавлено.")
        show_item_picker(tid); return

    if state.startswith("ord:comment:"):
        oid = int(state.split(":")[2])
        conn = get_db()
        conn.execute(
            "INSERT INTO order_comments (order_id,user_id,text) VALUES (?,?,?)",
            (oid, user["id"], text)
        )
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid, "Комментарий добавлен.")
        send_order_detail(tid, oid, user["role"]); return

    if state == "nm:add:name":
        user_data[tid]["name"] = text
        user_states[tid] = "nm:add:unit"
        bot.send_message(tid, "Единица измерения (м, шт, кг...):"); return

    if state == "nm:add:unit":
        user_data[tid]["unit"] = text
        user_states[tid] = "nm:add:notes"
        bot.send_message(tid, "Примечание (или - пропустить):"); return

    if state == "nm:add:notes":
        d     = user_data[tid]
        notes = None if text == "-" else text
        conn  = get_db()
        try:
            last = conn.execute(
                "SELECT code FROM nomenclature WHERE code LIKE 'НОМ-%' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            num  = int(last["code"].split("-")[1]) + 1 if last else 14
            code = f"НОМ-{num:03d}"
            conn.execute(
                "INSERT INTO nomenclature (code,name,unit,notes) VALUES (?,?,?,?)",
                (code, d["name"], d["unit"], notes)
            )
            conn.commit()
            bot.send_message(tid, f"Добавлено: {code} {d['name']}")
        except Exception as e:
            bot.send_message(tid, f"Ошибка: {e}")
        finally:
            conn.close()
        cancel_state(tid); return

    if state.startswith("nm:edit:"):
        parts  = state.split(":")
        nom_id = int(parts[2])
        field  = parts[3]
        conn   = get_db()
        conn.execute(f"UPDATE nomenclature SET {field}=? WHERE id=?", (text, nom_id))
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid, "Обновлено.")
        send_nom_detail(tid, nom_id, user["role"]); return

    if state == "cp:add:name":
        user_data[tid]["name"] = text
        user_states[tid] = "cp:add:phone"
        bot.send_message(tid, "Телефон (или - пропустить):"); return

    if state == "cp:add:phone":
        user_data[tid]["phone"] = None if text == "-" else text
        user_states[tid] = "cp:add:email"
        bot.send_message(tid, "Email (или - пропустить):"); return

    if state == "cp:add:email":
        user_data[tid]["email"] = None if text == "-" else text
        user_states[tid] = "cp:add:address"
        bot.send_message(tid, "Адрес отгрузки (или - пропустить):"); return

    if state == "cp:add:address":
        user_data[tid]["address"] = None if text == "-" else text
        user_states[tid] = "cp:add:notes"
        bot.send_message(tid, "Примечание (или - пропустить):"); return

    if state == "cp:add:notes":
        d     = user_data[tid]
        notes = None if text == "-" else text
        code  = next_cp_code()
        conn  = get_db()
        try:
            conn.execute(
                "INSERT INTO counterparties (code,name,phone,email,address,notes) VALUES (?,?,?,?,?,?)",
                (code, d["name"], d.get("phone"), d.get("email"), d.get("address"), notes)
            )
            conn.commit()
            bot.send_message(tid, f"Контрагент добавлен: {code} {d['name']}")
        except Exception as e:
            bot.send_message(tid, f"Ошибка: {e}")
        finally:
            conn.close()
        cancel_state(tid); return

    if state.startswith("cp:edit:"):
        parts = state.split(":")
        cp_id = int(parts[2])
        field = parts[3]
        conn  = get_db()
        conn.execute(f"UPDATE counterparties SET {field}=? WHERE id=?", (text, cp_id))
        conn.commit(); conn.close()
        cancel_state(tid)
        bot.send_message(tid, "Обновлено.")
        send_cp_detail(tid, cp_id, user["role"]); return

# ═══════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════════════════════════════════════════

print("Bot started!")
bot.infinity_polling()
