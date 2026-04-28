# -*- coding: utf-8 -*-
"""
Telegram Bot для сбора заявок с иерархическим меню
"""
import sys
import asyncio
import logging
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Твой токен от @BotFather для @KaifTime_Franch_bot
TOKEN = '8379322979:AAG_Xumo0yy5rSg0sEtcrhu758uRoPJB5_s'

# ID твоего Telegram-канала
CHAT_ID = '-1003052947504'

# Хранилища
user_data = {}                 # Для временных данных диалога заявки
completed_applications = {}    # Завершённые заявки
channel_message_ids = {}       # ID сообщений в канале для редактирования

# ---------- Клавиатуры ----------
def main_keyboard():
    """Главное меню: Продажи, Склад, Производство"""
    return ReplyKeyboardMarkup(
        [["Продажи"], ["Склад"], ["Производство"]],
        resize_keyboard=True, one_time_keyboard=False
    )

def sales_keyboard():
    """Подменю Продажи"""
    return ReplyKeyboardMarkup(
        [["Заказы", "Новый заказ"], ["Номенклатура", "Контрагенты"], ["Назад"]],
        resize_keyboard=True, one_time_keyboard=False
    )

def warehouse_keyboard():
    """Подменю Склад"""
    return ReplyKeyboardMarkup(
        [["Текущие остатки", "Инвентаризация"], ["Назад"]],
        resize_keyboard=True, one_time_keyboard=False
    )

def production_keyboard():
    """Подменю Производство"""
    return ReplyKeyboardMarkup(
        [["Производство за день", "Подтвердить отгрузку"], ["Отчёт по производству"], ["Назад"]],
        resize_keyboard=True, one_time_keyboard=False
    )

# ---------- Обработчики команд ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает главное меню"""
    logger.info("Пользователь %s открыл меню", update.effective_user.username)
    await update.message.reply_text(
        "Добро пожаловать в систему управления производством. Выберите раздел:",
        reply_markup=main_keyboard()
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена текущего диалога заявки (возврат в меню Продаж)"""
    chat_id = update.effective_chat.id
    if chat_id in user_data:
        del user_data[chat_id]
        logger.info("Диалог заявки отменён пользователем %s", chat_id)
    await update.message.reply_text(
        "Диалог отменён. Вы вернулись в раздел Продажи.",
        reply_markup=sales_keyboard()
    )

# ---------- Обработка текстовых сообщений (меню + диалог) ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text
    logger.info("Получено сообщение от %s: %s", chat_id, text)

    # 1. Проверяем, не находимся ли мы в процессе диалога новой заявки
    if chat_id in user_data:
        # Если пользователь хочет выйти
        if text == "Отмена":
            return await cancel(update, context)

        # Продолжаем диалог согласно шагу
        step = user_data[chat_id]["step"]

        if step == "waiting_name":
            user_data[chat_id]["name"] = text
            logger.info("Сохранено имя: %s", text)
            await update.message.reply_text(
                f"Приятно познакомиться, {text}. А где живёт начинающий предприниматель?",
                reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True)
            )
            user_data[chat_id]["step"] = "waiting_city"

        elif step == "waiting_city":
            user_data[chat_id]["city"] = text
            logger.info("Сохранён город: %s", text)
            await update.message.reply_text(
                f"{text}, отлично. Оставьте ваш номер телефона для связи:",
                reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True)
            )
            user_data[chat_id]["step"] = "waiting_phone"

        elif step == "waiting_phone":
            user_data[chat_id]["phone"] = text
            logger.info("Сохранён номер: %s", text)

            # Отправка заявки в канал
            current_time = update.message.date.strftime("%d.%m.%Y %H:%M")
            message_text = (
                f"Новая заявка!\n"
                f"Имя: {user_data[chat_id]['name']}\n"
                f"Город: {user_data[chat_id]['city']}\n"
                f"Телефон: {user_data[chat_id]['phone']}\n"
                f"Время: {current_time}"
            )
            try:
                sent_message = await context.bot.send_message(CHAT_ID, message_text)
                channel_message_ids[chat_id] = sent_message.message_id
                logger.info("Заявка отправлена в канал %s", CHAT_ID)
            except Exception as e:
                logger.error("Ошибка отправки в канал: %s", e)
                await update.message.reply_text(
                    "Не удалось сохранить заявку. Попробуйте позже.",
                    reply_markup=sales_keyboard()
                )
                del user_data[chat_id]
                return

            # Сохраняем завершённую заявку для сбора дополнительных сообщений
            completed_applications[chat_id] = {
                "name": user_data[chat_id]["name"],
                "city": user_data[chat_id]["city"],
                "phone": user_data[chat_id]["phone"],
                "time": current_time,
                "additional_messages": []
            }

            # Завершаем диалог и возвращаемся в Продажи
            await update.message.reply_text(
                "Отлично! Заявка принята. Ожидайте связи в рабочее время с 12:00 до 17:00 по МСК.\n"
                "Если есть что добавить – просто напишите сообщение.",
                reply_markup=sales_keyboard()
            )
            del user_data[chat_id]
        return

    # 2. Если активного диалога нет, но есть завершённая заявка – добавляем сообщение
    if chat_id in completed_applications:
        completed_applications[chat_id]["additional_messages"].append(text)
        logger.info("Добавлено дополнительное сообщение: %s", text)

        # Обновляем запись в канале
        try:
            additional_text = "\n".join(
                f"• {msg}" for msg in completed_applications[chat_id]["additional_messages"]
            )
            full_text = (
                f"Новая заявка!\n"
                f"Имя: {completed_applications[chat_id]['name']}\n"
                f"Город: {completed_applications[chat_id]['city']}\n"
                f"Телефон: {completed_applications[chat_id]['phone']}\n"
                f"Время: {completed_applications[chat_id]['time']}\n"
                f"Дополнительные сообщения:\n{additional_text}"
            )
            if chat_id in channel_message_ids:
                await context.bot.edit_message_text(
                    chat_id=CHAT_ID,
                    message_id=channel_message_ids[chat_id],
                    text=full_text
                )
            else:
                sent_message = await context.bot.send_message(CHAT_ID, full_text)
                channel_message_ids[chat_id] = sent_message.message_id
        except Exception as e:
            logger.error("Ошибка обновления заявки: %s", e)

        await update.message.reply_text(
            "Сообщение добавлено к вашей заявке.",
            reply_markup=sales_keyboard()
        )
        return

    # 3. Обработка навигации по меню
    if text == "Продажи":
        await update.message.reply_text("Раздел Продажи:", reply_markup=sales_keyboard())
    elif text == "Склад":
        await update.message.reply_text("Раздел Склад:", reply_markup=warehouse_keyboard())
    elif text == "Производство":
        await update.message.reply_text("Раздел Производство:", reply_markup=production_keyboard())
    elif text == "Назад":
        await update.message.reply_text("Главное меню:", reply_markup=main_keyboard())

    # Функционал Продаж (кроме Нового заказа)
    elif text == "Заказы":
        await update.message.reply_text("📦 Здесь будет список заказов.", reply_markup=sales_keyboard())
    elif text == "Номенклатура":
        await update.message.reply_text("📋 Здесь будет список номенклатуры.", reply_markup=sales_keyboard())
    elif text == "Контрагенты":
        await update.message.reply_text("👥 Здесь будут контрагенты.", reply_markup=sales_keyboard())

    # Запуск диалога новой заявки
    elif text == "Новый заказ":
        user_data[chat_id] = {"step": "waiting_name"}
        await update.message.reply_text(
            "Давайте оформим новый заказ. Как вас зовут?",
            reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True)
        )

    # Функционал Склада
    elif text == "Текущие остатки":
        await update.message.reply_text("📊 Остатки на складе (в разработке).", reply_markup=warehouse_keyboard())
    elif text == "Инвентаризация":
        await update.message.reply_text("⚙️ Инвентаризация (в разработке).", reply_markup=warehouse_keyboard())

    # Функционал Производства
    elif text == "Производство за день":
        await update.message.reply_text("📊 Производство за день (в разработке).", reply_markup=production_keyboard())
    elif text == "Подтвердить отгрузку":
        await update.message.reply_text("✅ Подтверждение отгрузки (в разработке).", reply_markup=production_keyboard())
    elif text == "Отчёт по производству":
        await update.message.reply_text("📈 Отчёт по производству (в разработке).", reply_markup=production_keyboard())

    else:
        await update.message.reply_text("Пожалуйста, используйте кнопки меню.", reply_markup=main_keyboard())

# ---------- Запуск бота ----------
async def main():
    logger.info("Запуск бота с меню...")
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel))  # на случай команды /cancel
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Бот готов к работе")
    await application.run_polling()

if __name__ == '__main__':
    asyncio.run(main())
