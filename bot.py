import os
import re
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
TELEGRAM_TOKEN = "СЮДА_ВСТАВЬТЕ_ТОКЕН_БОТА"
GOOGLE_CREDENTIALS_FILE = "credentials.json"
SPREADSHEET_ID = "13Hs2ar_7KlDqtVbFUYQMqX39dshqkWCkRk5PPDZqB3Q"
SHEET_NAME = "Лист1"  # или имя вашего листа

# ======== ЛОГИРОВАНИЕ ========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======== ИНИЦИАЛИЗАЦИЯ ========
bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Подключение к Google Sheets
def init_gsheets():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    return sheet

sheet = init_gsheets()


# ======== СОСТОЯНИЯ FSM ========
class UserState(StatesGroup):
    waiting_for_phone = State()
    waiting_for_plate = State()
    registered = State()


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
    
    # Остальные части (имя, отчество) — без изменений
    if len(parts) > 1:
        return masked_last + ' ' + ' '.join(parts[1:])
    return masked_last


def normalize_plate(plate: str) -> str:
    """Приводит гос. номер к единому виду"""
    return re.sub(r'\s+', '', plate).upper()


def is_valid_phone(phone: str) -> bool:
    """Проверяет российский номер телефона"""
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and (digits.startswith('7') or digits.startswith('8')):
        return True
    if len(digits) == 10:
        return True
    return False


def find_user_by_phone(phone: str):
    """Ищет пользователя по телефону в таблице"""
    digits = re.sub(r'\D', '', phone)
    # Приводим к формату 79XXXXXXXXX
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    elif len(digits) == 10:
        digits = '7' + digits
    
    try:
        records = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Ошибка чтения таблицы: {e}")
        return None
    
    for row in records[1:]:  # пропускаем заголовок
        if len(row) < 4:
            continue
        user_phones = re.findall(r'\d+', row[3])
        for p in user_phones:
            p_clean = re.sub(r'\D', '', p)
            if len(p_clean) == 11 and p_clean.startswith('8'):
                p_clean = '7' + p_clean[1:]
            if p_clean == digits:
                return {
                    'id': row[0],
                    'plate': row[1],
                    'fio': row[2],
                    'phone': row[3],
                    'category': row[4] if len(row) > 4 else ''
                }
    return None


def find_by_plate(plate: str):
    """Ищет владельца по гос. номеру"""
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
                    'id': row[0],
                    'plate': row[1],
                    'fio': row[2],
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
        "Для начала нужно зарегистрироваться — отправьте свой номер телефона "
        "кнопкой ниже (он должен быть в базе жильцов).",
        reply_markup=keyboard
    )
    await state.set_state(UserState.waiting_for_phone)


@dp.message(F.contact)
async def process_contact(message: Message, state: FSMContext):
    """Обработка отправленного контакта"""
    phone = message.contact.phone_number
    user = find_user_by_phone(phone)
    
    if user:
        await state.update_data(
            phone=phone,
            user_id=user['id'],
            fio=user['fio']
        )
        await message.answer(
            f"✅ Регистрация успешна!\n\n"
            f"Здравствуйте, <b>{mask_fio(user['fio'])}</b>.\n\n"
            f"Теперь введите <b>гос. номер автомобиля</b> "
            f"(например, <code>А123БВ777</code>):",
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
    """Если пользователь ввёл телефон текстом"""
    if is_valid_phone(message.text):
        await process_contact_contact_text(message, state)
    else:
        await message.answer(
            "⚠️ Пожалуйста, отправьте номер кнопкой «📱 Отправить номер» "
            "или введите корректный номер в формате +79XXXXXXXXX"
        )


async def process_contact_contact_text(message: Message, state: FSMContext):
    phone = message.text
    user = find_user_by_phone(phone)
    if user:
        await state.update_data(phone=phone, user_id=user['id'], fio=user['fio'])
        await message.answer(
            f"✅ Регистрация успешна!\n\n"
            f"Здравствуйте, <b>{mask_fio(user['fio'])}</b>.\n\n"
            f"Введите <b>гос. номер</b>:",
            parse_mode="HTML"
        )
        await state.set_state(UserState.waiting_for_plate)
    else:
        await message.answer("❌ Номер не найден в базе.")


@dp.message(UserState.waiting_for_plate)
async def process_plate(message: Message, state: FSMContext):
    """Поиск владельца по гос. номеру"""
    plate_input = message.text.strip()
    
    if not re.match(r'^[А-Яа-я0-9\s]+$', plate_input):
        await message.answer("⚠️ Номер содержит недопустимые символы. Попробуйте ещё раз:")
        return
    
    result = find_by_plate(plate_input)
    
    if result:
        masked = mask_fio(result['fio'])
        phones = result['phone'] if result['phone'] else 'не указан'
        
        # Если у пользователя несколько машин, покажем все
        plates_display = result['plate'] if result['plate'] else '—'
        
        response = (
            f"🚗 <b>Гос. номер:</b> <code>{plate_input.upper()}</code>\n\n"
            f"👤 <b>Владелец:</b> {masked}\n"
            f"📞 <b>Телефон:</b> {phones}\n"
            f"📂 <b>Категория:</b> {result['category']}\n"
        )
        await message.answer(response, parse_mode="HTML")
    else:
        await message.answer(
            f"❌ Автомобиль с номером <code>{plate_input.upper()}</code> "
            f"не найден в базе.\n\n"
            f"Проверьте правильность ввода или попробуйте другой номер."
        )


# ======== ОБРАБОТЧИК ДЛЯ ЗАРЕГИСТРИРОВАННЫХ ========
@dp.message(UserState.registered)
async def registered_user_handler(message: Message, state: FSMContext):
    """После регистрации — каждое сообщение = новый поиск"""
    if message.text and re.match(r'^[А-Яа-я0-9\s]+$', message.text):
        await process_plate(message, state)
    elif message.text == "/start":
        await cmd_start(message, state)
    else:
        await message.answer(
            "🔍 Введите гос. номер автомобиля для поиска владельца.\n"
            "Например: <code>А123БВ777</code>\n\n"
            "/start — сбросить регистрацию",
            parse_mode="HTML"
        )


# ======== ЗАПУСК ========
async def main():
    logger.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
