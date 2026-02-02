import re
import os
import json

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

import gspread

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1Zq1GarjOPmftln_g45djjz0bvUKTZQubgDBmfbTH26A/edit?usp=sharing"
SHEET_NAME = "Ответы на форму (1)"  # имя листа

COL_PLATE = 7   # G: "Гос. № автомобиля"
COL_NAME = 5    # E: "Ф.И.О. (полностью)"
COL_PHONE = 11  # K: телефон

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")


def normalize_plate(text: str) -> str:
    if not text:
        return ""
    t = text.upper()

    # русские буквы -> латиница (госномера)
    repl = {
        "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M",
        "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T",
        "У": "Y", "Х": "X",
    }
    for ru, en in repl.items():
        t = t.replace(ru, en)

    # убираем всё кроме латинских букв и цифр
    t = re.sub(r"[^A-Z0-9]", "", t)
    return t


def load_plates():
    # читаем JSON ключ сервисного аккаунта из переменной окружения
    service_account_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not service_account_info:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    creds_dict = json.loads(service_account_info)
    gc = gspread.service_account_from_dict(creds_dict)

    sh = gc.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SHEET_NAME)
    values = ws.get_all_values()

    plates_index = {}

    # пропускаем заголовок (values[0])
    for row in values[1:]:
        try:
            plate_cell = row[COL_PLATE - 1]
            name = row[COL_NAME - 1]
            phone = row[COL_PHONE - 1]
        except IndexError:
            continue

        if not plate_cell:
            continue

        # в одной ячейке может быть несколько номеров через запятую/перевод строки
        parts = re.split(r"[,;\n]+", plate_cell)
        for part in parts:
            norm = normalize_plate(part)
            if norm:
                plates_index[norm] = (name, phone)

    return plates_index


def mask_owner_name(full_name: str) -> str:
    """
    'Иванов Иван Иванович' -> 'И***** Иван Иванович'
    Если одна фамилия без пробелов: 'Иванов' -> 'И*****'
    """
    s = full_name.strip()
    if not s:
        return ""

    parts = s.split()
    if not parts:
        return ""

    last_name = parts[0]
    if len(last_name) <= 1:
        masked_last = last_name
    else:
        masked_last = last_name[0] + "*" * (len(last_name) - 1)

    tail = " ".join(parts[1:])  # имя, отчество и т.п.
    return f"{masked_last} {tail}".strip()


plates = {}
plates.update(load_plates())

print("Всего номеров в индексе:", len(plates))
print("Примеры:", list(plates.keys())[:20])

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        "Здравствуйте!\n\n"
        "Пожалуйста, введите полностью госномер автомобиля "
        "(например: A777AA777)."
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

    # если из текста после нормализации ничего не осталось — просим ввести номер
    if not norm:
        await message.reply(
            "Пожалуйста, введите полный госномер автомобиля "
            "(например: A777AA777)."
        )
        return

    data = plates.get(norm)
    if data:
        name, phone = data
        masked_name = mask_owner_name(name)
        reply = f"Номер: {text}\nВладелец: {masked_name}\nТелефон: {phone}"
    else:
        reply = (
            f"По номеру {text} ничего не найдено.\n\n"
            "Проверьте, что номер введён полностью, без ошибок "
            "и в формате наподобие A777AA777."
        )

    await message.reply(reply)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
