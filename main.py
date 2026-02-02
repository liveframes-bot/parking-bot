import re
import os

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
import gspread

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1Zq1GarjOPmftln_g45djjz0bvUKTZQubgDBmfbTH26A/edit?usp=sharing"
CREDENTIALS_FILE = "virtual-charger-404013-95cc392801ce.json"

SHEET_NAME = "Ответы на форму (1)"  # имя листа
COL_PLATE = 7   # G
COL_NAME = 5    # E
COL_PHONE = 11  # K

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")


def normalize_plate(text: str) -> str:
    if not text:
        return ""
    t = text.upper()
    repl = {
        "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M",
        "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T",
        "У": "Y", "Х": "X",
    }
    for ru, en in repl.items():
        t = t.replace(ru, en)
    t = re.sub(r"[^A-Z0-9]", "", t)
    return t


def load_plates():
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SHEET_NAME)

    values = ws.get_all_values()
    plates_index = {}

    for row in values[1:]:
        try:
            plate_cell = row[COL_PLATE - 1]
            name = row[COL_NAME - 1]
            phone = row[COL_PHONE - 1]
        except IndexError:
            continue

        if not plate_cell:
            continue

        parts = re.split(r"[,\n;]+", plate_cell)
        for part in parts:
            norm = normalize_plate(part)
            if norm:
                plates_index[norm] = (name, phone)

    return plates_index


# глобальный словарь номеров
plates = {}
plates.update(load_plates())
print("Всего номеров в индексе:", len(plates))
print("Примеры:", list(plates.keys())[:20])


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        "Привет! Отправь госномер (например: А643ЕЕ77), "
        "а я верну имя и телефон владельца."
    )


@dp.message(Command("reload"))
async def cmd_reload(message: Message):
    await message.reply("Обновляю данные из таблицы...")
    try:
        global plates
        new_plates = load_plates()
        plates.clear()
        plates.update(new_plates)
        await message.reply(f"Готово. Загружено номеров: {len(plates)}")
    except Exception as e:
        await message.reply(f"Ошибка при обновлении: {e}")


@dp.message()
async def handle_message(message: Message):
    text = (message.text or "").strip()
    norm = normalize_plate(text)
    if not norm:
        return

    data = plates.get(norm)
    if data:
        name, phone = data
        reply = f"Номер: {text}\nВладелец: {name}\nТелефон: {phone}"
    else:
        reply = "По этому номеру ничего не найдено."

    await message.reply(reply)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
