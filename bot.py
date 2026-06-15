import os
import re
import json
import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import gspread
from google.oauth2.service_account import Credentials

# ======== НАСТРОЙКИ ========
TELEGRAM_TOKEN = os.environ['TELEGRAM_TOKEN']
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
SHEET_NAME = os.environ.get('SHEET_NAME', 'Лист1')
GOOGLE_CREDS_JSON = os.environ['GOOGLE_CREDS_JSON']

# ======== ЛОГИРОВАНИЕ ========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======== ИНИЦИАЛИЗАЦИЯ ========
bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Подключение к Google Sheets
def init_gsheets():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    return sheet

sheet = init_gsheets()


# ======== СОСТОЯНИЯ FSM ========
class UserState(StatesGroup):
    waiting_for_phone = State()
    waiting_for_plate = State()


# ======== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========
def mask_fio(fio: str) -> str:
    """Скрывает фамилию, оставляет первую букву: Иванов → И*"""
    parts = fio.strip().split()
    if not parts:
        return fio
    last_name = parts[0]
    if len(last_name) <= 1:
        masked_last = last_name
    else:
        masked_last = last_name[0] + '*' * (len(last_name) - 1)
    if len(parts) > 1:
        return masked_last + ' ' + ' '.join(parts[1:])
    return masked_last


def normalize_plate(plate: str) -> str:
    return re.sub(r'\s+', '', plate).upper()


def is_valid_phone(phone: str) -> bool:
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and (digits.startswith('7') or digits.startswith('8')):
        return True
    if len(digits) == 10:
        return True
    return False


def find_user_by_phone(phone: str):
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    elif len(digits) == 10:
        digits = '7' + digits
    try:
        records = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Ошибка чтения таблицы: {e}")
        return None
    for row in records[1:]:
        if len(row) < 4:
            continue
        user_phones = re.findall(r'\d+', row[3])
        for p in user_phones:
            p_clean = re.sub(r'\D', '', p)
            if len(p_clean) == 11 and p_clean.startswith('8'):
                p_clean = '7' + p_clean[1:]
            if p_clean == digits:
                return {
                    'id': row[0], 'plate': row[1], 'fio': row[2],
                    'phone': row[3], 'category': row[4] if len(row) > 4 else ''
                }
    return None


def find_by_plate(plate: str):
    plate_norm = normalize_plate(plate)
    try:
        records = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Ошибка чтения таблицы: {e}")
        return None
    for row in records[1:]:
        if len(row) < 3:
            continue
        plates = re.split(r'[,;]', row[1])
        for p in plates:
            if normalize_plate(p.strip()) == plate_norm:
                return {
                    'id': row[0], 'plate': row[1], 'fio': row[2],
                    'phone': row[3] if len(row) > 3 else '',
                    'category': row[4] if len(row) > 4 else ''
                }
    return None


# ======== КОМАНДЫ БОТА ========
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True
    )
    await message.answer(
        "👋 Привет! Я бот парковки MD.\n\n"
        "🔍 Могу найти владельца авто по гос. номеру.\n\n"
        "Для начала зарегистрируйтесь — отправьте свой номер телефона кнопкой ниже "
        "(он должен быть в базе жильцов).",
        reply_markup=keyboard
    )
    await state.set_state(UserState.waiting_for_phone)


@dp.message(F.contact)
async def process_contact(message: Message, state: FSMContext):
    phone = message.contact.phone_number
    user = find_user_by_phone(phone)
    if user:
        await state.update_data(phone=phone, user_id=user['id'], fio=user['fio'])
        await message.answer(
            f"✅ Регистрация успешна!\n\n"
            f"Здравствуйте, <b>{mask_fio(user['fio'])}</b>.\n\n"
            f"Введите <b>гос. номер автомобиля</b> (например, <code>А123БВ777</code>):",
            parse_mode="HTML"
        )
        await state.set_state(UserState.waiting_for_plate)
    else:
        await message.answer(
            "❌ Ваш номер не найден в базе жильцов.\n\n"
            "Возможные причины:\n"
            "• Номер указан в другой форме\n"
            "• Вы ещё не зарегистрированы в системе парковки\n\n"
            "Обратитесь к администратору.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.clear()


@dp.message(UserState.waiting_for_phone, F.text)
async def phone_text_fallback(message: Message, state: FSMContext):
    if is_valid_phone(message.text):
        user = find_user_by_phone(message.text)
        if user:
            await state.update_data(phone=message.text, user_id=user['id'], fio=user['fio'])
            await message.answer(
                f"✅ Регистрация успешна!\n\n"
                f"Здравствуйте, <b>{mask_fio(user['fio'])}</b>.\n\n"
                f"Введите <b>гос. номер</b>:",
                parse_mode="HTML"
            )
            await state.set_state(UserState.waiting_for_plate)
        else:
            await message.answer("❌ Номер не найден в базе.")
    else:
        await message.answer(
            "⚠️ Отправьте номер кнопкой «📱 Отправить номер» "
            "или введите корректный номер в формате +79XXXXXXXXX"
        )


@dp.message(UserState.waiting_for_plate)
async def process_plate(message: Message, state: FSMContext):
    plate_input = message.text.strip()
    if not re.match(r'^[А-Яа-я0-9\s]+$', plate_input):
        await message.answer("⚠️ Номер содержит недопустимые символы. Попробуйте ещё раз:")
        return
    result = find_by_plate(plate_input)
    if result:
        masked = mask_fio(result['fio'])
        phones = result['phone'] if result['phone'] else 'не указан'
        response = (
            f"🚗 <b>Гос. номер:</b> <code>{plate_input.upper()}</code>\n\n"
            f"👤 <b>Владелец:</b> {masked}\n"
            f"📞 <b>Телефон:</b> {phones}\n"
            f"📂 <b>Категория:</b> {result['category']}\n"
        )
        await message.answer(response, parse_mode="HTML")
    else:
        await message.answer(
            f"❌ Автомобиль с номером <code>{plate_input.upper()}</code> не найден в базе."
        )


# ======== SELF-PING (АНТИ-СОН) ========
async def self_ping():
    """Каждые 5 минут опрашивает Telegram API, чтобы Render не усыпил Worker"""
    while True:
        try:
            await asyncio.sleep(300)
            me = await bot.get_me()
            logger.info(f"💓 Self-ping OK: @{me.username}")
        except Exception as e:
            logger.warning(f"⚠️ Self-ping error: {e}")


async def on_startup():
    """Действия при запуске"""
    me = await bot.get_me()
    logger.info(f"✅ Бот запущен: @{me.username}")
    asyncio.create_task(self_ping())
    logger.info("💓 Self-ping активен (каждые 5 мин)")


# ======== ЗАПУСК ========
if __name__ == "__main__":
    async def main():
        dp.startup.register(on_startup)
        await dp.start_polling(bot)
    asyncio.run(main())
