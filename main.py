import re
import os
import json
import asyncio

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
import gspread
from aiohttp import web

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1Zq1GarjOPmftln_g45djjz0bvUKTZQubgDBmfbTH26A/edit?usp=sharing"

SHEET_NAME = "Ответы на форму (1)"  # имя листа

COL_PLATE = 7  # G: "Гос. № автомобиля"
COL_NAME = 5   # E: "Ф.И.О. (полностью)"
COL_PHONE = 11 # K: телефон

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# --- авторизованные пользователи в памяти процесса ---
authorized_users: set[int] = set()


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


def looks_like_phone(phone: str) -> bool:
    digits = "".join(ch for ch in phone if ch.isdigit())
    return 10 <= len(digits) <= 12


def normalize_phone(phone: str) -> str:
    digits = "".join(ch for ch in phone if ch.isdigit())
    return digits[-10:]


def load_sheet():
    # читаем JSON ключ сервисного аккаунта из переменной окружения
    service_account_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not service_account_info:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    creds_dict = json.loads(service_account_info)
    gc = gspread.service_account_from_dict(creds_dict)
    sh = gc.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(SHEET_NAME)
    values = ws.get_all_values()
    return values


def load_plates_and_phones():
    """
    Загружаем индекс по номерам авто + список телефонов из таблицы.
    plates_index: {normalized_plate: (name, phone)}
    phones_normalized: множество нормализованных телефонов
    """
    values = load_sheet()

    plates_index: dict[str, tuple[str, str]] = {}
    phones_normalized: set[str] = set()

    # пропускаем заголовок (values[0])
    for row in values[1:]:
        try:
            plate_cell = row[COL_PLATE - 1]
            name = row[COL_NAME - 1]
            phone = row[COL_PHONE - 1]
        except IndexError:
            continue

        # телефоны
        if phone:
            phones_normalized.add(normalize_phone(phone))

        # номера авто
        if not plate_cell:
            continue
        # в одной ячейке может быть несколько номеров через запятую/перевод строки
        parts = re.split(r"[,;\n]+", plate_cell)
        for part in parts:
            norm = normalize_plate(part)
            if norm:
                plates_index[norm] = (name, phone)

    return plates_index, phones_normalized


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


# загружаем все данные один раз при старте
plates, phones_allowed = load_plates_and_phones()

print("Всего номеров в индексе:", len(plates))
print("Примеры:", list(plates.keys())[:20])
print("Всего телефонов в базе:", len(phones_allowed))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def is_phone_allowed(phone: str) -> bool:
    if not looks_like_phone(phone):
        return False
    norm = normalize_phone(phone)
    return norm in phones_allowed


def is_authorized(user_id: int) -> bool:
    return user_id in authorized_users


@dp.message(Command("start"))
async def cmd_start(message: Message):
    if is_authorized(message.from_user.id):
        await bot.send_message(
            chat_id=message.chat.id,
            text="Вы уже авторизованы, введите номер автомобиля (например: A777AA777).",
        )
        return

    await bot.send_message(
        chat_id=message.chat.id,
        text=(
            "Здравствуйте!\n\n"
            "Введите номер телефона, для идентификации в базе нашего двора.\n"
            "Формат: +7XXXXXXXXXX или 8XXXXXXXXXX."
        ),
    )


@dp.message(Command("reload"))
async def cmd_reload(message: Message):
    await bot.send_message(chat_id=message.chat.id, text="Обновляю данные из таблицы...")
    try:
        global plates, phones_allowed
        new_plates, new_phones = load_plates_and_phones()
        plates.clear()
        plates.update(new_plates)
        phones_allowed.clear()
        phones_allowed.update(new_phones)
        await bot.send_message(
            chat_id=message.chat.id,
            text=f"Готово. Загружено номеров: {len(plates)}, телефонов: {len(phones_allowed)}.",
        )
    except Exception as e:
        await bot.send_message(
            chat_id=message.chat.id,
            text=f"Ошибка при обновлении: {e}",
        )


@dp.message(F.text)
async def handle_text(message: Message):
    text = (message.text or "").strip()

    # если пользователь ещё не авторизован — считаем это попыткой ввести телефон
    if not is_authorized(message.from_user.id):
        phone = text
        if not is_phone_allowed(phone):
            await bot.send_message(
                chat_id=message.chat.id,
                text=(
                    "Ваш номер не найден в базе или введён в неверном формате.\n"
                    "Введите номер телефона, для идентификации в базе нашего двора.\n"
                    "Формат: +7XXXXXXXXXX или 8XXXXXXXXXX."
                ),
            )
            return

        authorized_users.add(message.from_user.id)
        await bot.send_message(
            chat_id=message.chat.id,
            text=(
                "Вы найдены в базе, можете пользоваться ботом.\n"
                "Теперь введите полностью госномер автомобиля (например: A777AA777)."
            ),
        )
        return

    # дальше — только для авторизованных: обработка госномера
    norm = normalize_plate(text)
    if not norm:
        return

    # Формат: 1 буква + 3 цифры + 2 буквы + 2–3 цифры (A777AA777, M175HH750 и т.п.)
    if not re.fullmatch(r"[A-Z]\d{3}[A-Z]{2}\d{2,3}", norm):
        # Не похоже на номер — игнорируем
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

    await bot.send_message(chat_id=message.chat.id, text=reply)


# --- HTTP‑сервер для Render ---
async def health(request):
    return web.Response(text="OK")


async def start_http_app():
    app = web.Application()
    app.add_routes([web.get("/health", health)])
    runner = web.AppRunner(app)
    await runner.setup()
    # Render передаёт порт через переменную PORT, по умолчанию 10000
    port = int(os.environ.get("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"HTTP server started on port {port}")


async def main():
    # запускаем HTTP‑сервер и aiogram‑бота параллельно
    await asyncio.gather(
        start_http_app(),
        dp.start_polling(bot),
    )


if __name__ == "__main__":
    asyncio.run(main())
