import os
import re
import json
import asyncio
import logging
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import gspread
from google.oauth2.service_account import Credentials
import aiohttp  # для внешних пингов

# ======== НАСТРОЙКИ ========
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
SHEET_NAME = os.environ.get('SHEET_NAME', 'Лист1')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDS_JSON')

if not TELEGRAM_TOKEN or not SPREADSHEET_ID or not GOOGLE_CREDS_JSON:
    raise ValueError("Отсутствуют обязательные переменные окружения!")

# ======== ЛОГИРОВАНИЕ ========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======== HEALTH-СЕРВЕР ДЛЯ RENDER ========
class HealthHandler(BaseHTTPRequestHandler):
    """Обработчик health-запросов для Render"""
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        elif self.path == '/ping':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'PONG')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Полностью отключаем логи health-сервера
        pass


def run_health_server():
    """Запуск health-сервера на порту 8080"""
    server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
    logger.info("🏥 Health-сервер запущен на порту 8080")
    server.serve_forever()


# Запускаем health-сервер в отдельном потоке
health_thread = Thread(target=run_health_server, daemon=True)
health_thread.start()

# ======== ИНИЦИАЛИЗАЦИЯ ========
bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ======== КОНСТАНТЫ ========
RESULTS_PER_PAGE = 5  # Количество результатов на одной странице

# Карта замены русских букв на английские в номере
RUS_TO_ENG = {
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M',
    'Н': 'H', 'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T',
    'У': 'Y', 'Х': 'X'
}
ENG_TO_RUS = {v: k for k, v in RUS_TO_ENG.items()}

# ======== ПОДКЛЮЧЕНИЕ GOOGLE SHEETS ========
def init_gsheets():
    """Инициализация подключения к Google Sheets"""
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
        logger.info(f"✅ Подключение к Google Sheets успешно (лист: {SHEET_NAME})")
        return sheet
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Google Sheets: {e}")
        raise

sheet = init_gsheets()


# ======== СОСТОЯНИЯ FSM ========
class UserState(StatesGroup):
    waiting_for_phone = State()
    waiting_for_plate = State()
    searching_plate = State()


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
    """
    Нормализует номер:
    - Удаляет пробелы
    - Приводит к верхнему регистру
    - Заменяет русские буквы на английские (для поиска)
    """
    cleaned = re.sub(r'\s+', '', plate).upper()
    # Заменяем русские буквы на английские
    result = []
    for ch in cleaned:
        result.append(RUS_TO_ENG.get(ch, ch))
    return ''.join(result)


def get_display_plate(original_plate: str) -> str:
    """
    Форматирует номер для отображения (русскими буквами)
    """
    cleaned = re.sub(r'\s+', '', original_plate).upper()
    result = []
    for ch in cleaned:
        # Если буква английская, заменяем на русскую (для красоты)
        result.append(ENG_TO_RUS.get(ch, ch))
    return ''.join(result)


def get_plate_numbers(plate_field: str) -> list:
    """
    Извлекает все номера из строки (разделители , ;)
    """
    if not plate_field:
        return []
    numbers = re.split(r'[,;]', plate_field)
    return [normalize_plate(n.strip()) for n in numbers if n.strip()]


def is_valid_phone(phone: str) -> bool:
    """Проверка валидности номера телефона"""
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and (digits.startswith('7') or digits.startswith('8')):
        return True
    if len(digits) == 10:
        return True
    return False


def format_phone_link(phone: str) -> str:
    """Форматирует телефон для кликабельной ссылки tel:"""
    digits = re.sub(r'\D', '', phone)
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    elif len(digits) == 10:
        digits = '7' + digits
    return f"tel:+{digits}"


def get_all_users():
    """Получает всех пользователей из таблицы"""
    try:
        records = sheet.get_all_values()
        users = []
        for row in records[1:]:  # Пропускаем заголовок
            if len(row) >= 3:
                users.append({
                    'id': row[0],
                    'plate': row[1] if len(row) > 1 else '',
                    'fio': row[2] if len(row) > 2 else '',
                    'phone': row[3] if len(row) > 3 else '',
                    'category': row[4] if len(row) > 4 else ''
                })
        return users
    except Exception as e:
        logger.error(f"Ошибка чтения таблицы: {e}")
        return []


def find_user_by_phone(phone: str):
    """Поиск пользователя по номеру телефона"""
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    elif len(digits) == 10:
        digits = '7' + digits
    
    users = get_all_users()
    for user in users:
        user_phones = re.findall(r'\d+', user['phone'])
        for p in user_phones:
            p_clean = re.sub(r'\D', '', p)
            if len(p_clean) == 11 and p_clean.startswith('8'):
                p_clean = '7' + p_clean[1:]
            if p_clean == digits:
                return user
    return None


def find_by_plate_partial(query: str):
    """
    Поиск автомобилей по части номера.
    Возвращает ВСЕ совпадения с указанной строкой.
    """
    query_norm = normalize_plate(query)
    users = get_all_users()
    results = []
    
    for user in users:
        if not user['plate']:
            continue
        # Получаем все номера автомобиля (может быть несколько через запятую)
        plate_numbers = get_plate_numbers(user['plate'])
        for plate_num in plate_numbers:
            # Проверяем, содержит ли номер искомую строку
            if query_norm in plate_num:
                results.append({
                    'id': user['id'],
                    'plate_raw': user['plate'],
                    'plate_normalized': plate_num,
                    'fio': user['fio'],
                    'phone': user['phone'],
                    'category': user['category']
                })
                break  # Добавляем пользователя только один раз
    
    # Удаляем дубликаты (на случай, если у пользователя несколько номеров и оба подходят)
    seen = set()
    unique_results = []
    for r in results:
        if r['id'] not in seen:
            seen.add(r['id'])
            unique_results.append(r)
    
    return unique_results

def format_search_result(user: dict) -> str:
    """
    Форматирует один результат поиска для отображения
    """
    masked = mask_fio(user['fio'])
    display_plate = get_display_plate(user['plate_raw'])
    
    # Обработка телефонов
    phone_display = 'не указан'
    if user['phone'] and user['phone'].strip():
        # Разделяем по常见 разделителям
        raw_phones = re.split(r'[,;\s]+', user['phone'])
        formatted_phones = []
        
        for raw_phone in raw_phones:
            raw_phone = raw_phone.strip()
            if not raw_phone:
                continue
            
            # Извлекаем цифры
            digits = ''.join(filter(str.isdigit, raw_phone))
            if not digits:
                continue
            
            # Нормализуем формат
            if len(digits) == 11 and digits[0] == '8':
                digits = '7' + digits[1:]
            elif len(digits) == 10:
                digits = '7' + digits
            
            # Создаём кликабельную ссылку с плюсом
            formatted_phones.append(f'<a href="tel:{digits}">+{digits}</a>')
        
        if formatted_phones:
            phone_display = ', '.join(formatted_phones)
    
    response = (
        f"🚗 <b>Гос. номер:</b> <code>{display_plate}</code>\n"
        f"👤 <b>Владелец:</b> {masked}\n"
        f"📞 <b>Телефон:</b> {phone_display}\n"
        f"📂 <b>Категория:</b> {user['category']}\n"
    )
    return response

# ======== КОМАНДЫ БОТА ========
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start - регистрация"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📝 Регистрация")]],
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


@dp.message(F.text == "📝 Регистрация")
async def registration_button(message: Message, state: FSMContext):
    """Кнопка регистрации - запрашивает контакт"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True
    )
    await message.answer(
        "📱 Пожалуйста, отправьте свой номер телефона, нажав на кнопку ниже.\n\n"
        "Это необходимо для подтверждения вашей регистрации в системе.",
        reply_markup=keyboard
    )


@dp.message(F.contact)
async def process_contact(message: Message, state: FSMContext):
    """Обработка контакта, отправленного кнопкой"""
    phone = message.contact.phone_number
    user = find_user_by_phone(phone)
    if user:
        await state.update_data(phone=phone, user_id=user['id'], fio=user['fio'])
        await message.answer(
            f"✅ Регистрация успешна!\n\n"
            f"Здравствуйте, <b>{mask_fio(user['fio'])}</b>.\n\n"
            f"Теперь вы можете искать владельцев по гос. номеру.\n\n"
            f"🔍 <b>Введите гос. номер автомобиля</b> (можно частично, например, <code>А123</code>):",
            parse_mode="HTML",
            reply_markup=types.ReplyKeyboardRemove()
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
    """Обработка ручного ввода телефона"""
    if is_valid_phone(message.text):
        user = find_user_by_phone(message.text)
        if user:
            await state.update_data(phone=message.text, user_id=user['id'], fio=user['fio'])
            await message.answer(
                f"✅ Регистрация успешна!\n\n"
                f"Здравствуйте, <b>{mask_fio(user['fio'])}</b>.\n\n"
                f"🔍 <b>Введите гос. номер</b> (можно частично):",
                parse_mode="HTML",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await state.set_state(UserState.waiting_for_plate)
        else:
            await message.answer("❌ Номер не найден в базе.")
    else:
        await message.answer(
            "⚠️ Пожалуйста, используйте кнопку «📱 Отправить номер» "
            "или введите корректный номер в формате +79XXXXXXXXX"
        )


# Хранилище результатов поиска для пагинации
search_cache = {}


@dp.message(UserState.waiting_for_plate)
async def process_plate(message: Message, state: FSMContext):
    """Поиск по номеру автомобиля"""
    plate_input = message.text.strip()
    
    # Данные пользователя для логирования
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name or str(user_id)
    
    logger.info(f"🔍 ПОИСК: Пользователь '{username}' (id:{user_id}) ищет номер '{plate_input}'")
    
    if not plate_input:
        await message.answer("⚠️ Пожалуйста, введите номер автомобиля.")
        return
    
    # Проверка на допустимые символы (буквы, цифры, пробелы)
    if not re.match(r'^[А-Яа-яA-Za-z0-9\s]+$', plate_input):
        await message.answer("⚠️ Номер содержит недопустимые символы. Используйте буквы и цифры.")
        return
    
    results = find_by_plate_partial(plate_input)
    
    if not results:
        logger.info(f"❌ ПОИСК: Номер '{plate_input}' НЕ НАЙДЕН (пользователь: {username})")
        await message.answer(
            f"❌ Автомобили с номером, содержащим <code>{plate_input.upper()}</code>, не найдены в базе.\n\n"
            f"💡 Попробуйте ввести больше символов или проверьте правильность номера.",
            parse_mode="HTML"
        )
        return
    
    logger.info(f"✅ ПОИСК: Найдено {len(results)} результат(ов) для '{plate_input}' (пользователь: {username})")
    
    # Сохраняем результаты в кэш
    chat_id = message.chat.id
    search_cache[chat_id] = {
        'results': results,
        'page': 0,
        'total_pages': (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
    }
    
    await send_search_results(message, chat_id, 0)


async def send_search_results(message: Message, chat_id: int, page: int):
    """Отправляет страницу с результатами поиска"""
    cache = search_cache.get(chat_id)
    if not cache:
        await message.answer("❌ Результаты поиска устарели. Пожалуйста, выполните поиск заново.")
        return
    
    results = cache['results']
    total_pages = cache['total_pages']
    start_idx = page * RESULTS_PER_PAGE
    end_idx = min(start_idx + RESULTS_PER_PAGE, len(results))
    page_results = results[start_idx:end_idx]
    
    # Формируем сообщение
    response_parts = [f"🔍 <b>Найдено автомобилей: {len(results)}</b>\n"]
    
    for i, result in enumerate(page_results, start=start_idx + 1):
        formatted = format_search_result(result)
        response_parts.append(f"{i}. {formatted}")
        response_parts.append("─" * 30)
    
    response_text = "\n".join(response_parts)
    
    # Создаем кнопки пагинации
    inline_keyboard = []
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"search_page_{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"search_page_{page + 1}"))
    
    if nav_buttons:
        inline_keyboard.append(nav_buttons)
    
    # Кнопка "Новый поиск"
    inline_keyboard.append([InlineKeyboardButton(text="🔄 Новый поиск", callback_data="search_new")])
    
    reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard) if inline_keyboard else None
    
    await message.answer(response_text, parse_mode="HTML", reply_markup=reply_markup)


@dp.callback_query(lambda c: c.data and c.data.startswith("search_page_"))
async def handle_search_page(callback: CallbackQuery):
    """Обработчик пагинации результатов поиска"""
    page = int(callback.data.split("_")[-1])
    chat_id = callback.message.chat.id
    
    await callback.answer()
    await send_search_results(callback.message, chat_id, page)
    await callback.message.delete()  # Удаляем предыдущее сообщение


@dp.callback_query(lambda c: c.data == "search_new")
async def handle_new_search(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Новый поиск'"""
    chat_id = callback.message.chat.id
    if chat_id in search_cache:
        del search_cache[chat_id]
    
    await callback.answer()
    await callback.message.answer(
        "🔍 Введите номер автомобиля для поиска:",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(UserState.waiting_for_plate)
    await callback.message.delete()


# ======== УЛУЧШЕННЫЙ SELF-PING (АНТИ-СОН) ========
# Переменные для отслеживания состояния
ping_failures = 0
PING_INTERVAL = 120  # 2 минуты вместо 5
MAX_FAILURES = 5

async def self_ping():
    """
    Улучшенный механизм предотвращения засыпания:
    - Пинг Telegram API каждые 2 минуты
    - Пинг внутреннего health-сервера
    - Пинг внешнего URL Render
    - Счетчик ошибок и автоматическое восстановление
    """
    global ping_failures
    
    # Получаем URL из переменной окружения или используем стандартный
    render_url = os.environ.get('RENDER_EXTERNAL_URL', 'https://parking-bot-z8y2.onrender.com')
    health_url = f"{render_url}/health"
    
    logger.info("💪 Запущен улучшенный Keep-Alive:")
    logger.info(f"   - Интервал: {PING_INTERVAL} сек")
    logger.info(f"   - Telegram API: каждые {PING_INTERVAL} сек")
    logger.info(f"   - Health-сервер: каждые {PING_INTERVAL} сек")
    logger.info(f"   - Внешний URL: {health_url}")
    
    while True:
        try:
            # 1. Пинг через Telegram API
            try:
                me = await bot.get_me()
                logger.debug(f"💓 Telegram ping OK: @{me.username}")
            except Exception as e:
                logger.warning(f"⚠️ Telegram ping error: {e}")
                ping_failures += 1
            
            # 2. Пинг внутреннего health-сервера
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('http://localhost:8080/health', timeout=5) as resp:
                        if resp.status == 200:
                            logger.debug("💚 Internal health check OK")
                        else:
                            logger.warning(f"⚠️ Internal health check: status {resp.status}")
                            ping_failures += 1
            except Exception as e:
                logger.debug(f"ℹ️ Internal health check: {e}")
            
            # 3. Пинг внешнего URL (чтобы Render видел активность)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(health_url, timeout=10) as resp:
                        if resp.status == 200:
                            logger.debug("🌐 External ping OK")
                        else:
                            logger.warning(f"⚠️ External ping: status {resp.status}")
                            ping_failures += 1
            except Exception as e:
                logger.debug(f"ℹ️ External ping: {e}")
            
            # 4. Если ошибок слишком много - пробуем восстановить соединение
            if ping_failures >= MAX_FAILURES:
                logger.warning(f"⚠️ Слишком много ошибок ({ping_failures}), пробуем восстановить соединение...")
                try:
                    # Пересоздаем сессию бота
                    await bot.session.close()
                    bot.session = aiohttp.ClientSession()
                    me = await bot.get_me()
                    logger.info(f"✅ Соединение восстановлено: @{me.username}")
                    ping_failures = 0
                except Exception as e:
                    logger.error(f"❌ Ошибка восстановления: {e}")
                    ping_failures = 0  # Сбрасываем, чтобы не зациклиться
            
            # 5. Логируем успешный пинг (каждый 5-й раз для экономии логов)
            if ping_failures == 0:
                logger.info(f"💓 Self-ping OK (интервал {PING_INTERVAL} сек)")
            
        except Exception as e:
            logger.error(f"❌ Self-ping critical error: {e}")
            ping_failures += 1
        
        await asyncio.sleep(PING_INTERVAL)


async def keep_alive_monitor():
    """
    Дополнительный монитор, который проверяет, жив ли бот,
    и перезапускает polling если нужно
    """
    while True:
        try:
            # Проверяем, отвечает ли бот
            await bot.get_me()
            await asyncio.sleep(60)  # Проверка каждую минуту
        except Exception as e:
            logger.error(f"❌ Бот не отвечает: {e}")
            logger.info("🔄 Пробуем перезапустить polling...")
            # Здесь можно добавить логику перезапуска
            await asyncio.sleep(10)


async def on_startup():
    """Действия при запуске"""
    me = await bot.get_me()
    logger.info(f"✅ Бот запущен: @{me.username}")
    
    # Запускаем улучшенный self-ping
    asyncio.create_task(self_ping())
    logger.info("💪 Улучшенный Keep-Alive активен (каждые 2 мин)")
    
    # Запускаем монитор состояния
    asyncio.create_task(keep_alive_monitor())
    logger.info("🔍 Монитор состояния активен")


# ======== ЗАПУСК ========
if __name__ == "__main__":
    async def main():
        dp.startup.register(on_startup)
        await dp.start_polling(bot)
    asyncio.run(main())
