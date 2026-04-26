import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from telebot import TeleBot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

BOT_TOKEN = os.environ["BOT_TOKEN"]
bot = TeleBot(BOT_TOKEN)
DB_PATH = "timetrack.db"

# ─── Роли ─────────────────────────────────────────────────────────────────────
# worker     — Рабочий
# manager    — Начальник Цеха
# admin      — Администратор
# superadmin — Главный админ

ROLE_LABELS = {
    "worker":     "👷 Рабочий",
    "manager":    "👔 Начальник Цеха",
    "admin":      "⚙️ Администратор",
    "superadmin": "👑 Главный админ",
}

# Роли которые получают уведомления и могут смотреть отчёты
SUPERVISOR_ROLES = ("manager", "admin", "superadmin")

# Роли которые admin может добавлять/удалять
ADMIN_MANAGED   = ("worker", "manager")
# Роли которые superadmin может добавлять/удалять
SUPER_MANAGED   = ("worker", "manager", "admin")

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
            role        TEXT NOT NULL
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
    ''')
    conn.commit()
    conn.close()

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

# ─── Клавиатуры ───────────────────────────────────────────────────────────────

def worker_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("✅ Пришёл на работу"), KeyboardButton("🚪 Ушёл с работы"))
    kb.row(KeyboardButton("📈 Моя статистика"))
    return kb

def manager_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Отчёт за неделю"), KeyboardButton("📊 Отчёт за месяц"))
    kb.row(KeyboardButton("👷 Кто сейчас на смене"))
    return kb

def admin_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Отчёт за неделю"), KeyboardButton("📊 Отчёт за месяц"))
    kb.row(KeyboardButton("👷 Кто сейчас на смене"), KeyboardButton("👥 Сотрудники"))
    return kb

def superadmin_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Отчёт за неделю"), KeyboardButton("📊 Отчёт за месяц"))
    kb.row(KeyboardButton("👷 Кто сейчас на смене"), KeyboardButton("👥 Сотрудники"))
    return kb

KEYBOARDS = {
    "worker":     worker_kb,
    "manager":    manager_kb,
    "admin":      admin_kb,
    "superadmin": superadmin_kb,
}

def send_menu(telegram_id, role, name):
    emoji = {"worker": "👷", "manager": "👔", "admin": "⚙️", "superadmin": "👑"}
    kb = KEYBOARDS.get(role, worker_kb)()
    bot.send_message(telegram_id, f"{emoji.get(role,'')} Привет, {name}!", reply_markup=kb)

# ─── /start  /myid ────────────────────────────────────────────────────────────

@bot.message_handler(commands=["start"])
def cmd_start(message):
    user = get_user(message.from_user.id)
    if user:
        send_menu(message.from_user.id, user["role"], user["name"])
    else:
        bot.send_message(
            message.from_user.id,
            "👋 Вы не зарегистрированы в системе.\n\n"
            "Сообщите администратору ваш Telegram ID:\n"
            f"<code>{message.from_user.id}</code>",
            parse_mode="HTML"
        )

@bot.message_handler(commands=["myid"])
def cmd_myid(message):
    bot.send_message(
        message.from_user.id,
        f"Ваш Telegram ID: <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

# ─── /setup — регистрация первого главного админа ─────────────────────────────

@bot.message_handler(commands=["setup"])
def cmd_setup(message):
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE role = 'superadmin'").fetchone()
    if existing:
        conn.close()
        bot.send_message(message.from_user.id, "⛔ Главный админ уже зарегистрирован. Команда недоступна.")
        return

    first = message.from_user.first_name or ""
    last  = message.from_user.last_name  or ""
    name  = f"{first} {last}".strip() or f"SuperAdmin_{message.from_user.id}"

    existing_user = conn.execute(
        "SELECT id FROM users WHERE telegram_id = ?", (message.from_user.id,)
    ).fetchone()

    if existing_user:
        conn.execute(
            "UPDATE users SET role = 'superadmin', name = ? WHERE telegram_id = ?",
            (name, message.from_user.id)
        )
    else:
        conn.execute(
            "INSERT INTO users (telegram_id, name, role) VALUES (?, ?, 'superadmin')",
            (message.from_user.id, name)
        )
    conn.commit()
    conn.close()

    bot.send_message(
        message.from_user.id,
        f"✅ Вы зарегистрированы как *Главный админ*!\n\n"
        f"Имя: *{name}*\n"
        f"ID: `{message.from_user.id}`\n\n"
        f"⚠️ Команда /setup теперь заблокирована.\n\n"
        f"Добавляйте сотрудников командой /add:\n"
        f"`/add [telegram_id] [имя] [роль]`\n"
        f"Роли: `worker` | `manager` | `admin`",
        parse_mode="Markdown"
    )
    send_menu(message.from_user.id, "superadmin", name)

# ─── /add — добавление сотрудников ───────────────────────────────────────────

@bot.message_handler(commands=["add"])
def cmd_add(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin", "superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа.")
        return

    parts = message.text.split()
    if len(parts) < 4:
        if user["role"] == "superadmin":
            roles_hint = "`worker` | `manager` | `admin`"
        else:
            roles_hint = "`worker` | `manager`"
        bot.send_message(
            message.from_user.id,
            f"📌 *Использование:*\n`/add [telegram_id] [имя] [роль]`\n\n"
            f"Доступные роли: {roles_hint}\n\n"
            f"Пример:\n`/add 123456789 Иван Иванов worker`",
            parse_mode="Markdown"
        )
        return

    tid_str = parts[1]
    role    = parts[-1]
    name    = " ".join(parts[2:-1])

    # Проверяем права на добавление данной роли
    allowed = SUPER_MANAGED if user["role"] == "superadmin" else ADMIN_MANAGED
    if role not in allowed:
        bot.send_message(
            message.from_user.id,
            f"❌ Вы не можете добавить роль `{role}`.\n"
            f"Доступные роли: {', '.join(f'`{r}`' for r in allowed)}",
            parse_mode="Markdown"
        )
        return

    try:
        tid = int(tid_str)
    except ValueError:
        bot.send_message(message.from_user.id, "❌ Telegram ID должен быть числом.")
        return

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (telegram_id, name, role) VALUES (?, ?, ?)",
            (tid, name, role)
        )
        conn.commit()
        bot.send_message(
            message.from_user.id,
            f"✅ Добавлен: *{name}* — {ROLE_LABELS[role]}",
            parse_mode="Markdown"
        )
        try:
            bot.send_message(tid, f"✅ Вы зарегистрированы как *{name}*.\nНажмите /start", parse_mode="Markdown")
        except Exception:
            pass
    except sqlite3.IntegrityError:
        bot.send_message(message.from_user.id, "⚠️ Этот пользователь уже есть в системе.")
    finally:
        conn.close()

# ─── /remove — удаление сотрудников ──────────────────────────────────────────

@bot.message_handler(commands=["remove"])
def cmd_remove(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin", "superadmin"):
        bot.send_message(message.from_user.id, "⛔ Нет доступа.")
        return

    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.from_user.id, "Использование: `/remove [telegram_id]`", parse_mode="Markdown")
        return
    try:
        tid = int(parts[1])
    except ValueError:
        bot.send_message(message.from_user.id, "❌ Telegram ID должен быть числом.")
        return

    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE telegram_id = ?", (tid,)).fetchone()
    if not target:
        bot.send_message(message.from_user.id, "❌ Пользователь не найден.")
        conn.close()
        return

    # Проверяем права на удаление данной роли
    allowed = SUPER_MANAGED if user["role"] == "superadmin" else ADMIN_MANAGED
    if target["role"] not in allowed:
        bot.send_message(
            message.from_user.id,
            f"❌ Вы не можете удалить пользователя с ролью {ROLE_LABELS.get(target['role'], target['role'])}.",
        )
        conn.close()
        return

    conn.execute("DELETE FROM users WHERE telegram_id = ?", (tid,))
    conn.commit()
    conn.close()
    bot.send_message(
        message.from_user.id,
        f"✅ *{target['name']}* удалён из системы.",
        parse_mode="Markdown"
    )

# ─── Рабочий: приход / уход ───────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "✅ Пришёл на работу")
def check_in(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker":
        return

    conn = get_db()
    active = conn.execute(
        "SELECT * FROM time_records WHERE user_id = ? AND status = 'active'",
        (user["id"],)
    ).fetchone()

    if active:
        t = datetime.fromisoformat(active["check_in"]).strftime("%H:%M")
        bot.send_message(message.from_user.id,
            f"⚠️ Ты уже на работе с {t}.\nСначала нажми '🚪 Ушёл с работы'.")
        conn.close()
        return

    now = datetime.now()
    conn.execute("INSERT INTO time_records (user_id, check_in) VALUES (?, ?)", (user["id"], now))
    conn.commit()
    conn.close()

    bot.send_message(message.from_user.id,
        f"✅ Приход зафиксирован в *{now.strftime('%H:%M')}*", parse_mode="Markdown")
    notify_supervisors(f"✅ *{user['name']}* пришёл на работу в {now.strftime('%H:%M')}")

@bot.message_handler(func=lambda m: m.text == "🚪 Ушёл с работы")
def check_out(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker":
        return

    conn = get_db()
    active = conn.execute(
        "SELECT * FROM time_records WHERE user_id = ? AND status = 'active'",
        (user["id"],)
    ).fetchone()

    if not active:
        bot.send_message(message.from_user.id, "⚠️ Ты не отмечен как на работе!")
        conn.close()
        return

    now = datetime.now()
    check_in_dt = datetime.fromisoformat(active["check_in"])
    hours = (now - check_in_dt).total_seconds() / 3600

    conn.execute(
        "UPDATE time_records SET check_out = ?, status = 'closed' WHERE id = ?",
        (now, active["id"])
    )
    conn.commit()
    conn.close()

    bot.send_message(
        message.from_user.id,
        f"👋 Уход зафиксирован в *{now.strftime('%H:%M')}*\n"
        f"⏱ Отработано: *{hours:.1f} ч.*",
        parse_mode="Markdown"
    )
    notify_supervisors(
        f"🚪 *{user['name']}* ушёл с работы в {now.strftime('%H:%M')} "
        f"(отработал {hours:.1f} ч.)"
    )

# ─── Статистика рабочего ──────────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text == "📈 Моя статистика")
def my_stats(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] != "worker":
        return

    conn = get_db()
    now = datetime.now()

    def calc(since):
        records = conn.execute(
            "SELECT * FROM time_records WHERE user_id = ? AND check_in >= ? AND status = 'closed'",
            (user["id"], since)
        ).fetchall()
        total = sum(
            (datetime.fromisoformat(r["check_out"]) - datetime.fromisoformat(r["check_in"])).total_seconds() / 3600
            for r in records
        )
        days = len({datetime.fromisoformat(r["check_in"]).date() for r in records})
        return days, total

    week_days,  week_hours  = calc(now - timedelta(days=7))
    month_days, month_hours = calc(now - timedelta(days=30))

    active = conn.execute(
        "SELECT check_in FROM time_records WHERE user_id = ? AND status = 'active'",
        (user["id"],)
    ).fetchone()
    conn.close()

    lines = [f"📈 *Моя статистика — {user['name']}*\n"]
    if active:
        dt  = datetime.fromisoformat(active["check_in"])
        hrs = (now - dt).total_seconds() / 3600
        lines.append(f"🟢 Сейчас на смене с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)\n")

    lines.append(f"📅 *За 7 дней:*  {week_days} дн. | {week_hours:.1f} ч.")
    lines.append(f"📆 *За 30 дней:* {month_days} дн. | {month_hours:.1f} ч.")

    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

# ─── Отчёты (manager / admin / superadmin) ────────────────────────────────────

def generate_report(days):
    conn = get_db()
    since  = datetime.now() - timedelta(days=days)
    period = "неделю" if days == 7 else "месяц"

    workers = conn.execute(
        "SELECT * FROM users WHERE role = 'worker' ORDER BY name"
    ).fetchall()

    if not workers:
        conn.close()
        return "📊 Нет зарегистрированных рабочих."

    lines = [
        f"📊 *Отчёт за {period}*",
        f"_{since.strftime('%d.%m')} — {datetime.now().strftime('%d.%m.%Y')}_\n"
    ]
    for w in workers:
        records  = conn.execute(
            "SELECT * FROM time_records WHERE user_id = ? AND check_in >= ?",
            (w["id"], since)
        ).fetchall()
        closed  = [r for r in records if r["status"] == "closed"]
        no_mark = [r for r in records if r["status"] == "no_checkout"]

        total_hours = sum(
            (datetime.fromisoformat(r["check_out"]) - datetime.fromisoformat(r["check_in"])).total_seconds() / 3600
            for r in closed
        )
        days_worked = len({datetime.fromisoformat(r["check_in"]).date() for r in closed})

        lines.append(f"👷 *{w['name']}*")
        lines.append(f"   Дней: {days_worked}  |  Часов: {total_hours:.1f}")
        if no_mark:
            lines.append(f"   ⚠️ Не отметил уход: {len(no_mark)} раз(а)")
        lines.append("")

    conn.close()
    return "\n".join(lines)

@bot.message_handler(func=lambda m: m.text == "📊 Отчёт за неделю")
def report_week(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES:
        return
    bot.send_message(message.from_user.id, generate_report(7), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "📊 Отчёт за месяц")
def report_month(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES:
        return
    bot.send_message(message.from_user.id, generate_report(30), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "👷 Кто сейчас на смене")
def on_shift(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in SUPERVISOR_ROLES:
        return

    conn = get_db()
    active = conn.execute(
        """SELECT u.name, t.check_in FROM time_records t
           JOIN users u ON u.id = t.user_id
           WHERE t.status = 'active' ORDER BY t.check_in"""
    ).fetchall()
    conn.close()

    if not active:
        bot.send_message(message.from_user.id, "👷 Сейчас никого нет на смене.")
        return

    lines = ["👷 *Сейчас на смене:*\n"]
    for r in active:
        dt  = datetime.fromisoformat(r["check_in"])
        hrs = (datetime.now() - dt).total_seconds() / 3600
        lines.append(f"• *{r['name']}* — с {dt.strftime('%H:%M')} ({hrs:.1f} ч.)")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "👥 Сотрудники")
def list_users(message):
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("admin", "superadmin"):
        return

    conn = get_db()
    # admin видит только worker и manager
    # superadmin видит всех
    if user["role"] == "superadmin":
        users = conn.execute(
            "SELECT * FROM users ORDER BY role, name"
        ).fetchall()
    else:
        users = conn.execute(
            "SELECT * FROM users WHERE role IN ('worker','manager') ORDER BY role, name"
        ).fetchall()
    conn.close()

    if not users:
        bot.send_message(message.from_user.id, "Список пуст.")
        return

    lines = ["👥 *Список сотрудников:*\n"]
    for u in users:
        lines.append(f"{ROLE_LABELS.get(u['role'], u['role'])}: *{u['name']}*")
        lines.append(f"`ID: {u['telegram_id']}`\n")
    bot.send_message(message.from_user.id, "\n".join(lines), parse_mode="Markdown")

# ─── Фоновые напоминания ──────────────────────────────────────────────────────

def reminder_loop():
    """
    Каждые 30 мин проверяем незакрытые смены:
      1-е напоминание: через 1 ч после прихода
      2-е напоминание: через 1 ч после первого
      Авто-закрытие:   через 1 ч после второго + уведомление руководству
    """
    while True:
        time.sleep(1800)
        try:
            conn = get_db()
            now  = datetime.now()
            rows = conn.execute(
                """SELECT t.*, u.name, u.telegram_id FROM time_records t
                   JOIN users u ON u.id = t.user_id
                   WHERE t.status = 'active'"""
            ).fetchall()

            for rec in rows:
                check_in_dt = datetime.fromisoformat(rec["check_in"])
                count   = rec["reminder_count"]
                last    = datetime.fromisoformat(rec["last_reminder"]) if rec["last_reminder"] else None
                elapsed = (now - check_in_dt).total_seconds()

                if count == 0 and elapsed >= 3600:
                    bot.send_message(rec["telegram_id"],
                        "⏰ *Напоминание:* ты ещё не отметил уход с работы.\n"
                        "Нажми '🚪 Ушёл с работы'", parse_mode="Markdown")
                    conn.execute(
                        "UPDATE time_records SET reminder_count=1, last_reminder=? WHERE id=?",
                        (now, rec["id"])
                    )
                    conn.commit()

                elif count == 1 and last and (now - last).total_seconds() >= 3600:
                    bot.send_message(rec["telegram_id"],
                        "⏰ *Второе напоминание:* пожалуйста, отметь уход с работы!\n"
                        "Нажми '🚪 Ушёл с работы'", parse_mode="Markdown")
                    conn.execute(
                        "UPDATE time_records SET reminder_count=2, last_reminder=? WHERE id=?",
                        (now, rec["id"])
                    )
                    conn.commit()

                elif count >= 2 and last and (now - last).total_seconds() >= 3600:
                    conn.execute(
                        "UPDATE time_records SET status='no_checkout', check_out=? WHERE id=?",
                        (now, rec["id"])
                    )
                    conn.commit()
                    bot.send_message(rec["telegram_id"],
                        "⚠️ Твоя смена автоматически закрыта — ты не отметил уход.\n"
                        "Руководство уведомлено.")
                    notify_supervisors(
                        f"⚠️ *{rec['name']}* не отметил уход с работы.\n"
                        f"Смена началась в {check_in_dt.strftime('%H:%M')}.\n"
                        f"Отмечено в базе как _пропущенный выход_."
                    )

            conn.close()
        except Exception as e:
            print(f"[reminder_loop error] {e}")

threading.Thread(target=reminder_loop, daemon=True).start()

# ─── Запуск ───────────────────────────────────────────────────────────────────

print("Bot started!")
bot.infinity_polling()
