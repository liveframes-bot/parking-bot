import os
import re
import json
import asyncio
import logging
import datetime
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
import aiohttp

# ======== НАСТРОЙКИ ========
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
SHEET_NAME = os.environ.get('SHEET_NAME', 'Лист1')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDS_JSON')

# Имена листов
REG_SHEET_NAME = 'Регистрации'
SEARCH_SHEET_NAME = 'Поиски'

# Админы (Telegram ID через запятую)
ADMIN_IDS_STR = os.environ.get('ADMIN_IDS', '')
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip().isdigit()]

if not TELEGRAM_TOKEN or not SPREADSHEET_ID or not GOOGLE_CREDS_JSON:
    raise ValueError("Отсутствуют обязательные переменные окружения!")

# ======== ЛОГИРОВАНИЕ ========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======== HEALTH-СЕРВЕР ДЛЯ RENDER ========
class HealthHandler(BaseHTTPRequestHandler):
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
        pass


def run_health_server():
    server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
    logger.info("🏥 Health-сервер запущен на порту 8080")
    server.serve_forever()


health_thread = Thread(target=run_health_server, daemon=True)
health_thread.start()

# ======== ИНИЦИАЛИЗАЦИЯ ========
bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ======== КОНСТАНТЫ ========
RESULTS_PER_PAGE = 5

RUS_TO_ENG = {
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M',
    'Н': 'H', 'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T',
    'У': 'Y', 'Х': 'X'
}
ENG_TO_RUS = {v: k for k, v in RUS_TO_ENG.items()}

# ======== ПОДКЛЮЧЕНИЕ GOOGLE SHEETS ========
def init_gsheets():
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        main_sheet = spreadsheet.worksheet(SHEET_NAME)
        
        # Лист "Регистрации"
        try:
            reg_sheet = spreadsheet.worksheet(REG_SHEET_NAME)
        except gspread.WorksheetNotFound:
            reg_sheet = spreadsheet.add_worksheet(title=REG_SHEET_NAME, rows=1000, cols=6)
            reg_sheet.append_row([
                'Дата', 'Telegram ID', 'Username', 'ФИО', 'Телефон', 'Имя в TG'
            ])
            reg_sheet.format('A1:F1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.7, 'green': 0.85, 'blue': 1.0}
            })
            logger.info(f"📄 Создан лист '{REG_SHEET_NAME}'")
        
        # Лист "Поиски"
        try:
            search_sheet = spreadsheet.worksheet(SEARCH_SHEET_NAME)
        except gspread.WorksheetNotFound:
            search_sheet = spreadsheet.add_worksheet(title=SEARCH_SHEET_NAME, rows=5000, cols=7)
            search_sheet.append_row([
                'Дата', 'Telegram ID', 'Username', 'Имя в TG', 'Запрос', 'Найдено', 'ID владельцев'
            ])
            search_sheet.format('A1:G1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 1.0, 'green': 0.9, 'blue': 0.7}
            })
            logger.info(f"📄 Создан лист '{SEARCH_SHEET_NAME}'")
        
        logger.info(f"✅ Google Sheets: '{SHEET_NAME}', '{REG_SHEET_NAME}', '{SEARCH_SHEET_NAME}'")
        return main_sheet, reg_sheet, search_sheet
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Google Sheets: {e}")
        raise

sheet, reg_sheet, search_sheet = init_gsheets()

# ======== КЭШИ ========
# Кэш зарегистрированных: telegram_id -> row_number в Лист1
REGISTERED_TG_TO_ROW = {}
ROW_TO_REGISTERED_TG = {}

# Кэш дедупликации поиска: (tg_id, query_normalized) -> datetime
SEARCH_DEDUP_CACHE = {}
DEDUP_WINDOW_SECONDS = 300  # 5 минут


def rebuild_registered_cache():
    """Перестраивает кэш зарегистрированных пользователей"""
    global REGISTERED_TG_TO_ROW, ROW_TO_REGISTERED_TG
    try:
        reg_rows = reg_sheet.get_all_values()
        registered_data = []
        for row in reg_rows[1:]:
            if len(row) >= 5 and row[1]:
                registered_data.append({
                    'tg_id': row[1].strip(),
                    'fio': row[3].strip(),
                    'phone': row[4].strip()
                })
        
        main_rows = sheet.get_all_values()
        REGISTERED_TG_TO_ROW = {}
        ROW_TO_REGISTERED_TG = {}
        
        for idx, row in enumerate(main_rows[1:], start=2):
            if len(row) < 3:
                continue
            row_fio = row[2].strip()
            row_phone = row[3].strip() if len(row) > 3 else ''
            
            for reg in registered_data:
                phone_digits_main = re.sub(r'\D', '', row_phone)
                phone_digits_reg = re.sub(r'\D', '', reg['phone'])
                
                if phone_digits_main and phone_digits_main == phone_digits_reg:
                    REGISTERED_TG_TO_ROW[reg['tg_id']] = idx
                    ROW_TO_REGISTERED_TG[idx] = reg['tg_id']
                    break
                elif row_fio and reg['fio'] and row_fio.lower() == reg['fio'].lower():
                    REGISTERED_TG_TO_ROW[reg['tg_id']] = idx
                    ROW_TO_REGISTERED_TG[idx] = reg['tg_id']
                    break
        
        logger.info(f"📊 Кэш зарегистрированных: {len(REGISTERED_TG_TO_ROW)} совпадений")
    except Exception as e:
        logger.error(f"Ошибка построения кэша: {e}")


# Построить кэш при старте
rebuild_registered_cache()


# ======== СОСТОЯНИЯ FSM ========
class UserState(StatesGroup):
    waiting_for_phone = State()
    waiting_for_plate = State()


# ======== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========
def mask_fio(fio: str) -> str:
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
    cleaned = re.sub(r'\s+', '', plate).upper()
    result = []
    for ch in cleaned:
        result.append(RUS_TO_ENG.get(ch, ch))
    return ''.join(result)


def get_display_plate(original_plate: str) -> str:
    cleaned = re.sub(r'\s+', '', original_plate).upper()
    result = []
    for ch in cleaned:
        result.append(ENG_TO_RUS.get(ch, ch))
    return ''.join(result)


def get_plate_numbers(plate_field: str) -> list:
    if not plate_field:
        return []
    numbers = re.split(r'[,;]', plate_field)
    return [normalize_plate(n.strip()) for n in numbers if n.strip()]


def is_valid_phone(phone: str) -> bool:
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and (digits.startswith('7') or digits.startswith('8')):
        return True
    if len(digits) == 10:
        return True
    return False


def get_all_users():
    try:
        records = sheet.get_all_values()
        users = []
        for row in records[1:]:
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
    query_norm = normalize_plate(query)
    users = get_all_users()
    results = []
    
    for user in users:
        if not user['plate']:
            continue
        plate_numbers = get_plate_numbers(user['plate'])
        for plate_num in plate_numbers:
            if query_norm in plate_num:
                results.append({
                    'id': user['id'],
                    'plate_raw': user['plate'],
                    'plate_normalized': plate_num,
                    'fio': user['fio'],
                    'phone': user['phone'],
                    'category': user['category']
                })
                break
    
    seen = set()
    unique_results = []
    for r in results:
        if r['id'] not in seen:
            seen.add(r['id'])
            unique_results.append(r)
    return unique_results


def format_search_result(user: dict) -> str:
    masked = mask_fio(user['fio'])
    display_plate = get_display_plate(user['plate_raw'])
    
    phone_display = 'не указан'
    if user['phone'] and user['phone'].strip():
        raw_phones = re.split(r'[,;\s]+', user['phone'])
        formatted_phones = []
        for raw_phone in raw_phones:
            raw_phone = raw_phone.strip()
            if not raw_phone:
                continue
            digits = ''.join(filter(str.isdigit, raw_phone))
            if not digits:
                continue
            if len(digits) == 11 and digits[0] == '8':
                digits = '7' + digits[1:]
            elif len(digits) == 10:
                digits = '7' + digits
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


# ======== ЛОГИРОВАНИЕ В GOOGLE SHEETS ========
def log_registration_to_sheet(user_id: int, username: str, tg_name: str, user_data: dict):
    try:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reg_sheet.append_row([
            timestamp,
            str(user_id),
            f"@{username}" if username else '',
            user_data.get('fio', ''),
            user_data.get('phone', ''),
            tg_name
        ], value_input_option='USER_ENTERED')
        logger.info(f"📝 Регистрация записана: {user_id} (@{username})")
    except Exception as e:
        logger.error(f"Ошибка записи регистрации: {e}")


def log_search_to_sheet(user_id: int, username: str, tg_name: str, query: str, found: int, owner_ids: list):
    """
    Сохраняет поисковый запрос с защитой от дублей.
    Возвращает True если записано, False если дубль.
    """
    query_normalized = re.sub(r'\s+', '', query).upper()
    cache_key = (user_id, query_normalized)
    now = datetime.datetime.now()
    
    if cache_key in SEARCH_DEDUP_CACHE:
        last_time = SEARCH_DEDUP_CACHE[cache_key]
        if (now - last_time).total_seconds() < DEDUP_WINDOW_SECONDS:
            logger.debug(f"⏭️ Дубль поиска пропущен: {cache_key}")
            return False
    
    try:
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
        search_sheet.append_row([
            timestamp,
            str(user_id),
            f"@{username}" if username else '',
            tg_name,
            query,
            found,
            ', '.join(owner_ids) if owner_ids else '-'
        ], value_input_option='USER_ENTERED')
        
        SEARCH_DEDUP_CACHE[cache_key] = now
        
        if len(SEARCH_DEDUP_CACHE) > 500:
            SEARCH_DEDUP_CACHE.clear()
        
        logger.info(f"🔍 Поиск записан: '{query}' (found={found})")
        return True
    except Exception as e:
        logger.error(f"Ошибка записи поиска: {e}")
        return False


def highlight_registered_owners(rows_to_highlight: set):
    """Подсвечивает строки зарегистрированных владельцев жёлтым"""
    try:
        all_values = sheet.get_all_values()
        if len(all_values) > 1:
            last_row = len(all_values)
            sheet.format(f'A1:E{last_row}', {
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
            })
        
        for row_num in rows_to_highlight:
            sheet.format(f'A{row_num}:E{row_num}', {
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.6}
            })
        
        if rows_to_highlight:
            logger.info(f"🎨 Подсвечено {len(rows_to_highlight)} строк зарегистрированных")
    except Exception as e:
        logger.error(f"Ошибка подсветки: {e}")


# ======== УВЕДОМЛЕНИЯ АДМИНАМ ========
async def notify_admins_new_registration(user_id: int, username: str, tg_name: str, user_data: dict):
    """Уведомляет всех админов о новой регистрации"""
    if not ADMIN_IDS:
        return
    
    text = (
        f"🆕 <b>Новая регистрация!</b>\n\n"
        f"👤 <b>ФИО:</b> {user_data.get('fio', '—')}\n"
        f"📞 <b>Телефон:</b> {user_data.get('phone', '—')}\n"
        f"📂 <b>Категория:</b> {user_data.get('category', '—')}\n"
        f"🆔 <b>Telegram ID:</b> <code>{user_id}</code>\n"
        f"👨‍💻 <b>Username:</b> @{username if username else '—'}\n"
        f"📛 <b>Имя в TG:</b> {tg_name or '—'}\n"
        f"🕐 <b>Время:</b> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")


# ======== КОМАНДЫ БОТА ========
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
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
    phone = message.contact.phone_number
    user = find_user_by_phone(phone)
    if user:
        await state.update_data(phone=phone, user_id=user['id'], fio=user['fio'])
        
        tg_name = message.from_user.first_name or ''
        tg_username = message.from_user.username or ''
        
        # Записываем регистрацию
        log_registration_to_sheet(
            user_id=message.from_user.id,
            username=tg_username,
            tg_name=tg_name,
            user_data=user
        )
        
        # Обновляем кэш
        rebuild_registered_cache()
        
        # Уведомляем админов
        await notify_admins_new_registration(
            user_id=message.from_user.id,
            username=tg_username,
            tg_name=tg_name,
            user_data=user
        )
        
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
    if is_valid_phone(message.text):
        user = find_user_by_phone(message.text)
        if user:
            await state.update_data(phone=message.text, user_id=user['id'], fio=user['fio'])
            
            tg_name = message.from_user.first_name or ''
            tg_username = message.from_user.username or ''
            
            # Записываем регистрацию
            log_registration_to_sheet(
                user_id=message.from_user.id,
                username=tg_username,
                tg_name=tg_name,
                user_data=user
            )
            
            # Обновляем кэш
            rebuild_registered_cache()
            
            # Уведомляем админов
            await notify_admins_new_registration(
                user_id=message.from_user.id,
                username=tg_username,
                tg_name=tg_name,
                user_data=user
            )
            
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


search_cache = {}


@dp.message(UserState.waiting_for_plate)
async def process_plate(message: Message, state: FSMContext):
    plate_input = message.text.strip()
    
    user_id = message.from_user.id
    username = message.from_user.username or ''
    tg_name = message.from_user.first_name or ''
    
    logger.info(f"🔍 ПОИСК: '{username}' (id:{user_id}) → '{plate_input}'")
    
    if not plate_input:
        await message.answer("⚠️ Пожалуйста, введите номер автомобиля.")
        return
    
    if not re.match(r'^[А-Яа-яA-Za-z0-9\s]+$', plate_input):
        await message.answer("⚠️ Номер содержит недопустимые символы. Используйте буквы и цифры.")
        return
    
    results = find_by_plate_partial(plate_input)
    
    # Записываем в Google Sheets (с защитой от дублей)
    owner_ids = [r['id'] for r in results]
    log_search_to_sheet(user_id, username, tg_name, plate_input, len(results), owner_ids)
    
    if not results:
        await message.answer(
            f"❌ Автомобили с номером, содержащим <code>{plate_input.upper()}</code>, не найдены в базе.\n\n"
            f"💡 Попробуйте ввести больше символов или проверьте правильность номера.",
            parse_mode="HTML"
        )
        return
    
    # Подсвечиваем только зарегистрированных владельцев
    rows_to_highlight = set()
    for r in results:
        try:
            cell = sheet.find(r['id'])
            if cell and cell.row in ROW_TO_REGISTERED_TG:
                rows_to_highlight.add(cell.row)
        except Exception:
            pass
    
    if rows_to_highlight:
        highlight_registered_owners(rows_to_highlight)
    
    chat_id = message.chat.id
    search_cache[chat_id] = {
        'results': results,
        'page': 0,
        'total_pages': (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
    }
    
    await send_search_results(message, chat_id, 0)


async def send_search_results(message: Message, chat_id: int, page: int):
    cache = search_cache.get(chat_id)
    if not cache:
        await message.answer("❌ Результаты поиска устарели. Пожалуйста, выполните поиск заново.")
        return
    
    results = cache['results']
    total_pages = cache['total_pages']
    start_idx = page * RESULTS_PER_PAGE
    end_idx = min(start_idx + RESULTS_PER_PAGE, len(results))
    page_results = results[start_idx:end_idx]
    
    response_parts = [f"🔍 <b>Найдено автомобилей: {len(results)}</b>\n"]
    
    for i, result in enumerate(page_results, start=start_idx + 1):
        formatted = format_search_result(result)
        response_parts.append(f"{i}. {formatted}")
        response_parts.append("─" * 30)
    
    response_text = "\n".join(response_parts)
    
    inline_keyboard = []
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"search_page_{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"search_page_{page + 1}"))
    
    if nav_buttons:
        inline_keyboard.append(nav_buttons)
    
    inline_keyboard.append([InlineKeyboardButton(text="🔄 Новый поиск", callback_data="search_new")])
    
    reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard) if inline_keyboard else None
    
    await message.answer(response_text, parse_mode="HTML", reply_markup=reply_markup)


@dp.callback_query(lambda c: c.data and c.data.startswith("search_page_"))
async def handle_search_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[-1])
    chat_id = callback.message.chat.id
    
    await callback.answer()
    await send_search_results(callback.message, chat_id, page)
    await callback.message.delete()


@dp.callback_query(lambda c: c.data == "search_new")
async def handle_new_search(callback: CallbackQuery, state: FSMContext):
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


# ======== АДМИНСКИЕ КОМАНДЫ ========
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


@dp.message(Command("registrations"))
async def cmd_registrations(message: Message):
    """Показывает последние регистрации"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для админов.")
        return
    
    try:
        rows = reg_sheet.get_all_values()
        if len(rows) <= 1:
            await message.answer("📭 Пока никто не зарегистрировался.")
            return
        
        total = len(rows) - 1
        recent = list(reversed(rows[1:]))[:20]
        
        response_parts = [f"📋 <b>Регистраций всего: {total}</b>\n"]
        response_parts.append("<b>Последние 20:</b>\n")
        
        for row in recent:
            if len(row) >= 6:
                response_parts.append(
                    f"👤 {row[3]}\n"
                    f"   🆔 <code>{row[1]}</code> | {row[2]} | {row[5]}\n"
                    f"   📞 {row[4]}\n"
                    f"   🕐 {row[0]}\n"
                )
        
        text = "\n".join(response_parts)
        if len(text) > 4000:
            text = text[:4000] + "\n\n<i>... (показаны первые 4000 символов)</i>"
        
        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("searches"))
async def cmd_searches(message: Message):
    """Показывает последние поисковые запросы"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Эта команда только для админов.")
        return
    
    try:
        rows = search_sheet.get_all_values()
        if len(rows) <= 1:
            await message.answer("📭 Поисков ещё не было.")
            return
        
        total = len(rows) - 1
        recent = list(reversed(rows[1:]))[:20]
        
        response_parts = [f"🔍 <b>Поисков всего: {total}</b>\n"]
        response_parts.append("<b>Последние 20:</b>\n")
        
        for row in recent:
            if len(row) >= 7:
                response_parts.append(
                    f"🚗 <b>{row[4]}</b> → {row[5]} совпадений\n"
                    f"   👤 {row[2]} ({row[3]}) 🆔 <code>{row[1]}</code>\n"
                    f"   🕐 {row[0]}\n"
                )
        
        text = "\n".join(response_parts)
        if len(text) > 4000:
            text = text[:4000] + "\n\n<i>... (показаны первые 4000 символов)</i>"
        
        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("refresh_cache"))
async def cmd_refresh_cache(message: Message):
    """Перестраивает кэш зарегистрированных"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Только для админов.")
        return
    rebuild_registered_cache()
    await message.answer(
        f"✅ Кэш обновлён.\n"
        f"Зарегистрировано в боте: {len(REGISTERED_TG_TO_ROW)} совпадений с жильцами."
    )


@dp.message(Command("clear_highlight"))
async def cmd_clear_highlight(message: Message):
    """Сбрасывает всю подсветку"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Только для админов.")
        return
    try:
        all_values = sheet.get_all_values()
        if len(all_values) > 1:
            last_row = len(all_values)
            sheet.format(f'A1:E{last_row}', {
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
            })
        await message.answer("✅ Подсветка сброшена.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("cleanup_searches"))
async def cmd_cleanup_searches(message: Message):
    """Удаляет записи из листа 'Поиски' старше N дней. Пример: /cleanup_searches 30"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Только для админов.")
        return
    
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await message.answer(
            "⚠️ Укажите количество дней.\n"
            "Пример: <code>/cleanup_searches 30</code>",
            parse_mode="HTML"
        )
        return
    
    days = int(args[1])
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
    cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
    
    await message.answer(f"🧹 Очищаю записи старше {days} дней...")
    
    try:
        rows = search_sheet.get_all_values()
        if len(rows) <= 1:
            await message.answer("📭 Лист пуст.")
            return
        
        rows_to_delete = []
        for idx in range(len(rows) - 1, 0, -1):
            row = rows[idx]
            if len(row) >= 1 and row[0] < cutoff_str:
                rows_to_delete.append(idx + 1)
        
        if not rows_to_delete:
            await message.answer("✅ Нечего удалять — все записи свежие.")
            return
        
        deleted = 0
        for row_num in rows_to_delete:
            search_sheet.delete_rows(row_num)
            deleted += 1
        
        await message.answer(f"✅ Удалено {deleted} записей старше {days} дней.")
        logger.info(f"🧹 Админ очистил {deleted} записей поиска")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("cleanup_registrations"))
async def cmd_cleanup_registrations(message: Message):
    """Удаляет дубли в листе 'Регистрации' (оставляет самую раннюю запись)"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Только для админов.")
        return
    
    await message.answer("🧹 Удаляю дубли регистраций...")
    
    try:
        rows = reg_sheet.get_all_values()
        if len(rows) <= 1:
            await message.answer("📭 Лист пуст.")
            return
        
        seen = {}
        rows_to_delete = []
        
        for idx, row in enumerate(rows):
            if idx == 0:
                continue
            if len(row) < 2:
                continue
            
            tg_id = row[1].strip()
            timestamp = row[0]
            
            if tg_id and tg_id in seen:
                rows_to_delete.append(idx + 1)
            else:
                seen[tg_id] = timestamp
        
        if not rows_to_delete:
            await message.answer("✅ Дублей нет.")
            return
        
        for row_num in sorted(rows_to_delete, reverse=True):
            reg_sheet.delete_rows(row_num)
        
        await message.answer(f"✅ Удалено {len(rows_to_delete)} дублей регистраций.")
        logger.info(f"🧹 Удалено {len(rows_to_delete)} дублей в 'Регистрации'")
        
        # Обновляем кэш
        rebuild_registered_cache()
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# ======== KEEP-ALIVE ========
ping_failures = 0
PING_INTERVAL = 120
MAX_FAILURES = 5


async def self_ping():
    global ping_failures
    render_url = os.environ.get('RENDER_EXTERNAL_URL', 'https://parking-bot-z8y2.onrender.com')
    health_url = f"{render_url}/health"
    
    logger.info("💪 Keep-Alive активен:")
    logger.info(f"   - Интервал: {PING_INTERVAL} сек")
    logger.info(f"   - URL: {health_url}")
    
    while True:
        try:
            try:
                me = await bot.get_me()
                logger.debug(f"💓 Telegram ping OK: @{me.username}")
            except Exception as e:
                logger.warning(f"⚠️ Telegram ping: {e}")
                ping_failures += 1
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('http://localhost:8080/health', timeout=5) as resp:
                        if resp.status == 200:
                            logger.debug("💚 Local health OK")
                        else:
                            ping_failures += 1
            except Exception:
                pass
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(health_url, timeout=10) as resp:
                        if resp.status == 200:
                            logger.debug("🌐 External ping OK")
                        else:
                            ping_failures += 1
            except Exception:
                pass
            
            if ping_failures >= MAX_FAILURES:
                logger.warning(f"⚠️ {ping_failures} ошибок, перезапуск сессии...")
                try:
                    await bot.session.close()
                    bot.session = aiohttp.ClientSession()
                    await bot.get_me()
                    ping_failures = 0
                    logger.info("✅ Сессия восстановлена")
                except Exception as e:
                    logger.error(f"❌ Восстановление: {e}")
                    ping_failures = 0
            
            if ping_failures == 0:
                logger.info(f"💓 Self-ping OK")
        
        except Exception as e:
            logger.error(f"❌ Self-ping error: {e}")
            ping_failures += 1
        
        await asyncio.sleep(PING_INTERVAL)


async def keep_alive_monitor():
    while True:
        try:
            await bot.get_me()
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"❌ Бот не отвечает: {e}")
            await asyncio.sleep(10)


async def on_startup():
    me = await bot.get_me()
    logger.info(f"✅ Бот запущен: @{me.username}")
    asyncio.create_task(self_ping())
    asyncio.create_task(keep_alive_monitor())
    logger.info("💪 Keep-Alive активен")


# ======== ЗАПУСК ========
if __name__ == "__main__":
    async def main():
        dp.startup.register(on_startup)
        await dp.start_polling(bot)
    asyncio.run(main())
