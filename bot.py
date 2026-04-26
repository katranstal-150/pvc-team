import os
from telebot import TeleBot

bot = TeleBot(os.environ["BOT_TOKEN"])

@bot.message_handler(commands=["start"])
def start(message):
    bot.reply_to(message, "Привет! Я эхо-бот 🤖 Напиши что-нибудь!")

@bot.message_handler(commands=["help"])
def help(message):
    bot.reply_to(message, "Я просто повторяю всё что ты пишешь 😊")

@bot.message_handler(func=lambda m: True)
def echo(message):
    bot.reply_to(message, message.text)

bot.infinity_polling()
