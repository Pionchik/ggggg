import asyncio
import logging
import sqlite3
import os
import json
import hmac
import hashlib
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, WebAppInfo
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiohttp import web

load_dotenv()

BOT_TOKEN  = os.getenv("BOT_TOKEN")
ADMIN_IDS  = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# ⚠️ Замените на реальный HTTPS-URL вашего сайта (GitHub Pages / VPS / Vercel)
WEB_APP_URL = os.getenv("WEBAPP_URL") or os.getenv("WEB_APP_URL") or "https://pionchik.github.io/tax/"
API_PORT    = int(os.getenv("PORT") or os.getenv("API_PORT", "8080"))
# URL по которому index.html будет слать заказы (ngrok или VPS)
API_URL     = os.getenv("API_URL", f"http://localhost:{API_PORT}")

PRICE_PER_KM = 40
BASE_PRICE   = 170
CITY_NAME    = "Енакиево"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== DATABASE ====================

def init_db():
    conn = sqlite3.connect("taxi.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY, tg_id INTEGER UNIQUE, username TEXT, full_name TEXT,
        phone TEXT, role TEXT DEFAULT 'passenger', is_banned INTEGER DEFAULT 0,
        is_online INTEGER DEFAULT 0, balance REAL DEFAULT 0, total_earned REAL DEFAULT 0,
        total_orders INTEGER DEFAULT 0, rating REAL DEFAULT 5.0, rating_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT, passenger_id INTEGER, driver_id INTEGER,
        order_type TEXT DEFAULT 'taxi', from_address TEXT, to_address TEXT,
        distance REAL DEFAULT 0, price REAL DEFAULT 0, status TEXT DEFAULT 'pending',
        comment TEXT, eta_minutes INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')), accepted_at TEXT, completed_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ban_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, user_id INTEGER,
        reason TEXT, action TEXT, created_at TEXT DEFAULT (datetime('now')))""")
    c.execute("""CREATE TABLE IF NOT EXISTS driver_locations (
        tg_id INTEGER PRIMARY KEY, lat REAL, lon REAL,
        updated_at TEXT DEFAULT (datetime('now')))""")
    conn.commit(); conn.close()

COLS_USER  = ["id","tg_id","username","full_name","phone","role","is_banned","is_online",
              "balance","total_earned","total_orders","rating","rating_count","created_at"]
COLS_ORDER = ["id","passenger_id","driver_id","order_type","from_address","to_address",
              "distance","price","status","comment","eta_minutes","created_at","accepted_at","completed_at"]

def db():
    return sqlite3.connect("taxi.db")

def get_user(tg_id):
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)); row = c.fetchone(); conn.close()
    return dict(zip(COLS_USER, row)) if row else None

def create_user(tg_id, username, full_name):
    conn = db(); c = conn.cursor()
    role = "admin" if tg_id in ADMIN_IDS else "passenger"
    c.execute("INSERT OR IGNORE INTO users (tg_id,username,full_name,role) VALUES (?,?,?,?)",
              (tg_id, username or "", full_name or "", role))
    conn.commit(); conn.close()

def update_user(tg_id, **kw):
    conn = db(); c = conn.cursor()
    for k, v in kw.items():
        c.execute(f"UPDATE users SET {k}=? WHERE tg_id=?", (v, tg_id))
    conn.commit(); conn.close()

def get_online_drivers():
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM users WHERE role='driver' AND is_online=1 AND is_banned=0")
    rows = c.fetchall(); conn.close()
    return [dict(zip(COLS_USER, r)) for r in rows]

def create_order(passenger_id, order_type, from_addr, to_addr, price, distance=0, comment="", from_lat=None, from_lon=None):
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO orders (passenger_id,order_type,from_address,to_address,price,distance,comment) VALUES (?,?,?,?,?,?,?)",
              (passenger_id, order_type, from_addr, to_addr, price, distance, comment))
    oid = c.lastrowid
    # Сохраняем координаты точки отправления в отдельной таблице если есть
    if from_lat and from_lon:
        try:
            c.execute("CREATE TABLE IF NOT EXISTS order_geo (order_id INTEGER PRIMARY KEY, lat REAL, lon REAL)")
            c.execute("INSERT OR REPLACE INTO order_geo (order_id, lat, lon) VALUES (?,?,?)", (oid, from_lat, from_lon))
        except: pass
    conn.commit(); conn.close(); return oid

def get_order(oid):
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM orders WHERE id=?", (oid,)); row = c.fetchone(); conn.close()
    return dict(zip(COLS_ORDER, row)) if row else None

def update_order(oid, **kw):
    conn = db(); c = conn.cursor()
    for k, v in kw.items():
        c.execute(f"UPDATE orders SET {k}=? WHERE id=?", (v, oid))
    conn.commit(); conn.close()

def get_user_orders(tg_id, limit=10):
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM orders WHERE passenger_id=? OR driver_id=? ORDER BY created_at DESC LIMIT ?",
              (tg_id, tg_id, limit))
    rows = c.fetchall(); conn.close()
    return [dict(zip(COLS_ORDER, r)) for r in rows]

def get_stats():
    conn = db(); c = conn.cursor(); s = {}
    for key, q in [
        ("total_users",      "SELECT COUNT(*) FROM users"),
        ("total_drivers",    "SELECT COUNT(*) FROM users WHERE role='driver'"),
        ("online_drivers",   "SELECT COUNT(*) FROM users WHERE is_online=1"),
        ("total_orders",     "SELECT COUNT(*) FROM orders"),
        ("completed_orders", "SELECT COUNT(*) FROM orders WHERE status='completed'"),
        ("banned_users",     "SELECT COUNT(*) FROM users WHERE is_banned=1"),
    ]:
        c.execute(q); s[key] = c.fetchone()[0]
    c.execute("SELECT SUM(price) FROM orders WHERE status='completed'")
    s["total_revenue"] = c.fetchone()[0] or 0
    conn.close(); return s

def search_user(query):
    conn = db(); c = conn.cursor()
    try:
        c.execute("SELECT * FROM users WHERE tg_id=?", (int(query),))
    except ValueError:
        c.execute("SELECT * FROM users WHERE username LIKE ? OR full_name LIKE ?",
                  (f"%{query}%", f"%{query}%"))
    rows = c.fetchall(); conn.close()
    return [dict(zip(COLS_USER, r)) for r in rows]

def get_all_users(limit=50):
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = c.fetchall(); conn.close()
    return [dict(zip(COLS_USER, r)) for r in rows]

# ==================== GEO HELPERS ====================

import math

def haversine_km(lat1, lon1, lat2, lon2):
    """Расстояние между двумя точками в км"""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def save_driver_location(tg_id, lat, lon):
    conn = db(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO driver_locations (tg_id, lat, lon, updated_at) VALUES (?,?,?,datetime('now'))",
              (tg_id, lat, lon))
    conn.commit(); conn.close()

def get_driver_location(tg_id):
    conn = db(); c = conn.cursor()
    c.execute("SELECT lat, lon FROM driver_locations WHERE tg_id=?", (tg_id,))
    row = c.fetchone(); conn.close()
    return (row[0], row[1]) if row else None

def get_online_drivers_sorted(from_lat=None, from_lon=None):
    """Возвращает онлайн-водителей, отсортированных по расстоянию если известны координаты"""
    drivers = get_online_drivers()
    if from_lat is None or from_lon is None or not drivers:
        return drivers
    result = []
    for d in drivers:
        loc = get_driver_location(d["tg_id"])
        if loc:
            dist = haversine_km(from_lat, from_lon, loc[0], loc[1])
            result.append((dist, d))
        else:
            result.append((9999, d))
    result.sort(key=lambda x: x[0])
    return [d for _, d in result]

# ==================== FSM ====================

class RegisterStates(StatesGroup):
    waiting_phone = State()

class AdminStates(StatesGroup):
    search_user = State()
    ban_reason  = State()
    broadcast   = State()

# ==================== HELPERS ====================

def calc_price(dist, order_type="taxi"):
    return (200 if order_type == "delivery" else BASE_PRICE) + dist * (50 if order_type == "delivery" else PRICE_PER_KM)

def eta(dist):
    return max(3, int(dist / 30 * 60))

def status_label(s):
    return {"pending":"⏳ Ожидает водителя","accepted":"✅ Принят","arrived":"🚗 Водитель подъехал",
            "completed":"🏁 Завершён","cancelled":"❌ Отменён"}.get(s, s)

def role_label(r):
    return {"passenger":"Пассажир","driver":"Водитель","admin":"Администратор"}.get(r, r)

def main_menu_text(user):
    text = (f'<tg-emoji emoji-id="5873147866364514353">🏘</tg-emoji> <b>ЕнакиевоТакси</b> — {CITY_NAME}\n\n'
            f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>{user["full_name"]}</b>\n'
            f'<tg-emoji emoji-id="5870982283724328568">⚙</tg-emoji> Роль: <b>{role_label(user["role"])}</b>\n')
    if user["role"] == "driver":
        if user["is_online"]:
            text += '<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Статус: <b>На линии</b>\n'
        else:
            text += '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Статус: <b>Не на линии</b>\n'
    text += '\nВыберите действие:'
    return text

# ==================== KEYBOARDS ====================

def kb_main(user):
    rows = []
    if user["role"] == "driver":
        if user["is_online"]:
            rows.append([InlineKeyboardButton(text="Уйти с линии",   callback_data="toggle_online", icon_custom_emoji_id="5870657884844462243")])
        else:
            rows.append([InlineKeyboardButton(text="Выйти на линию", callback_data="toggle_online", icon_custom_emoji_id="5870633910337015697")])
    rows.append([InlineKeyboardButton(text="Заказать такси / доставку", web_app=WebAppInfo(url=WEB_APP_URL), icon_custom_emoji_id="5873147866364514353")])
    rows.append([
        InlineKeyboardButton(text="Профиль",    callback_data="show_profile", icon_custom_emoji_id="5870994129244131212"),
        InlineKeyboardButton(text="Мои заказы", callback_data="my_orders",    icon_custom_emoji_id="5884479287171485878"),
    ])
    if user["role"] == "admin":
        rows.append([InlineKeyboardButton(text="Админ панель", callback_data="admin_panel", icon_custom_emoji_id="5870982283724328568")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_cancel():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="cancel_order_flow", icon_custom_emoji_id="5870657884844462243")
    ]])

def kb_confirm(oid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Подтвердить", callback_data=f"confirm_order_{oid}", icon_custom_emoji_id="5870633910337015697"),
        InlineKeyboardButton(text="Отменить",    callback_data=f"cancel_order_{oid}",  icon_custom_emoji_id="5870657884844462243"),
    ]])

def kb_accept(oid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Принять заказ", callback_data=f"accept_order_{oid}",  icon_custom_emoji_id="5870633910337015697"),
        InlineKeyboardButton(text="Отклонить",     callback_data=f"decline_order_{oid}", icon_custom_emoji_id="5870657884844462243"),
    ]])

def kb_driver_active(oid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Я подъехал!",       callback_data=f"arrived_order_{oid}",  icon_custom_emoji_id="6042011682497106307")],
        [InlineKeyboardButton(text="Завершить поездку", callback_data=f"complete_order_{oid}", icon_custom_emoji_id="5870633910337015697")],
        [InlineKeyboardButton(text="Отменить заказ",    callback_data=f"driver_cancel_{oid}",  icon_custom_emoji_id="5870657884844462243")],
    ])

def kb_driver_complete(oid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Завершить поездку", callback_data=f"complete_order_{oid}", icon_custom_emoji_id="5870633910337015697")],
        [InlineKeyboardButton(text="Отменить заказ",    callback_data=f"driver_cancel_{oid}",  icon_custom_emoji_id="5870657884844462243")],
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика",        callback_data="admin_stats",        icon_custom_emoji_id="5870921681735781843")],
        [InlineKeyboardButton(text="Пользователи",      callback_data="admin_users",        icon_custom_emoji_id="5870772616305839506")],
        [InlineKeyboardButton(text="Найти пользователя",callback_data="admin_search_user",  icon_custom_emoji_id="5870994129244131212")],
        [InlineKeyboardButton(text="Последние заказы",  callback_data="admin_orders",       icon_custom_emoji_id="5884479287171485878")],
        [InlineKeyboardButton(text="Рассылка",          callback_data="admin_broadcast",    icon_custom_emoji_id="6039422865189638057")],
        [InlineKeyboardButton(text="Главное меню",      callback_data="main_menu",          icon_custom_emoji_id="5873147866364514353")],
    ])

def kb_manage_user(target_id, user):
    rows = []
    if user["role"] != "driver":
        rows.append([InlineKeyboardButton(text="Сделать водителем",      callback_data=f"set_driver_{target_id}",    icon_custom_emoji_id="5870994129244131212")])
    if user["role"] != "passenger":
        rows.append([InlineKeyboardButton(text="Сделать пассажиром",     callback_data=f"set_passenger_{target_id}", icon_custom_emoji_id="5891207662678317861")])
    if user["role"] != "admin":
        rows.append([InlineKeyboardButton(text="Сделать администратором",callback_data=f"set_admin_{target_id}",     icon_custom_emoji_id="5870982283724328568")])
    if user["is_banned"]:
        rows.append([InlineKeyboardButton(text="Разблокировать", callback_data=f"unban_{target_id}", icon_custom_emoji_id="6037496202990194718")])
    else:
        rows.append([InlineKeyboardButton(text="Заблокировать",  callback_data=f"ban_{target_id}",   icon_custom_emoji_id="6037249452824072506")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_users", icon_custom_emoji_id="5893057118545646106")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back(cb="admin_panel"):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Назад", callback_data=cb, icon_custom_emoji_id="5893057118545646106")
    ]])

def kb_webapp_reply():
    """Reply-клавиатура с кнопкой WebApp — sendData работает ТОЛЬКО через неё на мобильном"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚕 Заказать такси / доставку", web_app=WebAppInfo(url=WEB_APP_URL))]],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def kb_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Главное меню", callback_data="main_menu", icon_custom_emoji_id="5873147866364514353")
    ]])

def kb_waiting_order(oid):
    """Кнопки когда заказ ждёт водителей"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отменить заказ", callback_data=f"cancel_order_{oid}", icon_custom_emoji_id="5870657884844462243")],
        [InlineKeyboardButton(text="Главное меню",   callback_data="main_menu",           icon_custom_emoji_id="5873147866364514353")],
    ])

# ==================== ROUTER ====================

router = Router()

# ===== START =====

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    tg_id = message.from_user.id
    create_user(tg_id, message.from_user.username, message.from_user.full_name)
    user = get_user(tg_id)

    if user["is_banned"]:
        await message.answer(
            '<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> <b>Вы заблокированы.</b>\n\nОбратитесь к администратору.',
            parse_mode=ParseMode.HTML)
        return

    await message.answer(".", reply_markup=ReplyKeyboardRemove())

    if not user["phone"]:
        await message.answer(
            '<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>Добро пожаловать в ЕнакиевоТакси!</b>\n\n'
            'Нажмите кнопку ниже, чтобы поделиться номером телефона:',
            parse_mode=ParseMode.HTML,
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
                resize_keyboard=True, one_time_keyboard=True))
        await state.set_state(RegisterStates.waiting_phone)
        return

    await message.answer(main_menu_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_main(user))

@router.message(RegisterStates.waiting_phone, F.contact)
async def reg_phone(message: Message, state: FSMContext):
    phone = message.contact.phone_number
    update_user(message.from_user.id, phone=phone)
    user = get_user(message.from_user.id)
    await state.clear()
    await message.answer("✅", reply_markup=ReplyKeyboardRemove())
    await message.answer(
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Регистрация завершена!</b>\n\n'
        f'📱 Телефон: <b>{phone}</b>\n\n' + main_menu_text(user),
        parse_mode=ParseMode.HTML, reply_markup=kb_main(user))

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user = get_user(callback.from_user.id)
    if not user or user["is_banned"]:
        await callback.answer("Нет доступа.", show_alert=True); return
    await callback.message.edit_text(main_menu_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_main(user))
    await callback.answer()

# ===== WEB APP DATA (sendData — работает везде) =====

@router.message(F.web_app_data)
async def web_app_order(message: Message, bot: Bot):
    await process_order_data(message, message.web_app_data.data, bot)

# ===== WEB APP DATA (fetch → sendMessage — Mobile fallback) =====

order_router = Router()

@order_router.message()
async def catch_taxi_order(message: Message, bot: Bot):
    if message.text and message.text.startswith("TAXI_ORDER:"):
        raw = message.text[len("TAXI_ORDER:"):]
        try:
            await message.delete()
        except Exception:
            pass
        await process_order_data(message, raw, bot)

async def process_order_data(message: Message, raw: str, bot: Bot):
    user = get_user(message.from_user.id)
    if not user or user["is_banned"]:
        return

    try:
        data = json.loads(raw)
    except Exception:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Ошибка обработки данных.</b>',
            parse_mode=ParseMode.HTML)
        return

    order_type   = data.get("order_type", "taxi")
    from_address = data.get("from_address", "").strip()
    to_address   = data.get("to_address",   "").strip()
    distance     = float(data.get("distance", 0))
    price        = float(data.get("price",    0))
    comment      = data.get("comment", "").strip()
    from_lat     = data.get("from_lat")
    from_lon     = data.get("from_lon")
    if from_lat is not None: from_lat = float(from_lat)
    if from_lon is not None: from_lon = float(from_lon)

    if not from_address or not to_address or distance <= 0:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Некорректные данные. Попробуйте снова.</b>',
            parse_mode=ParseMode.HTML, reply_markup=kb_menu())
        return

    if price <= 0:
        price = calc_price(distance, order_type)

    oid = create_order(
        passenger_id=message.from_user.id, order_type=order_type,
        from_addr=from_address, to_addr=to_address,
        price=round(price), distance=distance, comment=comment,
        from_lat=from_lat, from_lon=from_lon)

    icon = "📦" if order_type == "delivery" else "🚕"
    type_name = "Доставка" if order_type == "delivery" else "Такси"
    t = eta(distance)

    await message.answer(
        f'{icon} <b>{type_name} #{oid}</b>\n\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Откуда:</b> {from_address}\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Куда:</b> {to_address}\n'
        f'<tg-emoji emoji-id="5778479949572738874">↔️</tg-emoji> <b>Расстояние:</b> {distance:.1f} км\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Стоимость:</b> {round(price)} ₽\n'
        f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> <b>В пути примерно:</b> ~{t} мин\n'
        f'<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> <b>Комментарий:</b> {comment or "нет"}\n\n'
        f'Подтвердить заказ?',
        parse_mode=ParseMode.HTML, reply_markup=kb_confirm(oid))

# ===== DRIVER TOGGLE =====

@router.callback_query(F.data == "toggle_online")
async def cb_toggle(callback: CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user or user["role"] not in ("driver", "admin"):
        try: await callback.answer("Только для водителей.", show_alert=True)
        except: pass
        return
    new_st = 0 if user["is_online"] else 1
    update_user(callback.from_user.id, is_online=new_st)
    user = get_user(callback.from_user.id)
    try:
        if new_st:
            await callback.answer("🟢 Вы на линии! Отправьте геолокацию для точного определения расстояния до клиентов.", show_alert=True)
        else:
            await callback.answer("🔴 Вы ушли с линии.", show_alert=True)
    except: pass
    try:
        if new_st:
            await callback.message.edit_text(
                main_menu_text(user) + '\n\n<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Отправьте свою геолокацию</b> (кнопка 📎 → Геолокация), чтобы клиенты видели ближайшего водителя.',
                parse_mode=ParseMode.HTML, reply_markup=kb_main(user))
        else:
            await callback.message.edit_text(main_menu_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_main(user))
    except: pass

@router.message(F.location)
async def driver_location(message: Message):
    """Водитель отправил геолокацию — сохраняем"""
    user = get_user(message.from_user.id)
    if not user or user["role"] not in ("driver", "admin"):
        return
    lat = message.location.latitude
    lon = message.location.longitude
    save_driver_location(message.from_user.id, lat, lon)
    # Проверяем есть ли pending-заказы которые ещё не приняты
    conn2 = db(); c2 = conn2.cursor()
    c2.execute("SELECT id FROM orders WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    pending = c2.fetchone(); conn2.close()
    extra = ""
    if pending:
        extra = f'\n\n<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Есть ожидающий заказ <b>#{pending[0]}</b>! Проверьте очередь.'
    await message.answer(
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Геолокация сохранена!</b>\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> {lat:.4f}, {lon:.4f}{extra}',
        parse_mode=ParseMode.HTML, reply_markup=kb_main(user))

# ===== PROFILE =====

@router.callback_query(F.data == "show_profile")
async def cb_profile(callback: CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user: await callback.answer(); return
    text = (f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>Профиль</b>\n\n'
            f'<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> <b>Имя:</b> {user["full_name"]}\n'
            f'<tg-emoji emoji-id="5769289093221454192">🔗</tg-emoji> <b>Username:</b> @{user["username"] or "не указан"}\n'
            f'📱 <b>Телефон:</b> {user["phone"] or "не указан"}\n'
            f'<tg-emoji emoji-id="5870982283724328568">⚙</tg-emoji> <b>Роль:</b> {role_label(user["role"])}\n'
            f'<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> <b>Заказов:</b> {user["total_orders"]}\n'
            f'<tg-emoji emoji-id="5890937706803894250">📅</tg-emoji> <b>Регистрация:</b> {user["created_at"][:10]}\n')
    if user["role"] == "driver":
        text += (f'\n<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Баланс:</b> {user["balance"]:.0f} ₽\n'
                 f'<tg-emoji emoji-id="5879814368572478751">🏧</tg-emoji> <b>Заработано:</b> {user["total_earned"]:.0f} ₽\n'
                 f'⭐ <b>Рейтинг:</b> {user["rating"]:.1f}\n')
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_back("main_menu"))
    await callback.answer()

# ===== MY ORDERS =====

@router.callback_query(F.data == "my_orders")
async def cb_my_orders(callback: CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user: await callback.answer(); return
    orders = get_user_orders(callback.from_user.id)
    if not orders:
        await callback.message.edit_text(
            '<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> <b>Мои заказы</b>\n\nУ вас пока нет заказов.',
            parse_mode=ParseMode.HTML, reply_markup=kb_back("main_menu"))
        await callback.answer(); return
    text = '<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> <b>Ваши последние заказы:</b>\n\n'
    for o in orders[:8]:
        icon = "📦" if o["order_type"] == "delivery" else "🚕"
        rl = " (водитель)" if o["driver_id"] == callback.from_user.id else ""
        text += (f'{icon} <b>#{o["id"]}{rl}</b> — {status_label(o["status"])}\n'
                 f'  <i>{o["from_address"]} → {o["to_address"]}</i>\n'
                 f'  <tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> {o["price"]:.0f} ₽ | {o["created_at"][:16]}\n\n')
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_back("main_menu"))
    await callback.answer()

# ===== CONFIRM ORDER =====

@router.callback_query(F.data.startswith("confirm_order_"))
async def confirm_order(callback: CallbackQuery, bot: Bot):
    oid = int(callback.data.split("_")[-1])
    order = get_order(oid)
    if not order or order["passenger_id"] != callback.from_user.id:
        await callback.answer("Заказ не найден.", show_alert=True); return
    if order["status"] != "pending":
        await callback.answer("Заказ уже обработан.", show_alert=True); return

    icon = "📦" if order["order_type"] == "delivery" else "🚕"
    type_name = "Доставка" if order["order_type"] == "delivery" else "Такси"

    # Получаем координаты точки отправления и сортируем водителей по расстоянию
    from_lat, from_lon = None, None
    try:
        conn2 = db(); c2 = conn2.cursor()
        c2.execute("SELECT lat, lon FROM order_geo WHERE order_id=?", (oid,))
        geo_row = c2.fetchone(); conn2.close()
        if geo_row:
            from_lat, from_lon = geo_row
    except: pass
    drivers = get_online_drivers_sorted(from_lat, from_lon)

    if not drivers:
        # Водителей нет — заказ остаётся в базе, ждём
        await callback.message.edit_text(
            f'{icon} <b>{type_name} #{oid} — принят!</b>\n\n'
            f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Откуда:</b> {order["from_address"]}\n'
            f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Куда:</b> {order["to_address"]}\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Стоимость:</b> {order["price"]:.0f} ₽\n\n'
            f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> <b>Ожидаем водителей онлайн...</b>\n\n'
            f'Как только водитель появится на линии — он получит ваш заказ. Мы уведомим вас!',
            parse_mode=ParseMode.HTML, reply_markup=kb_waiting_order(oid))
        await callback.answer()
        return

    # Рассылаем заказ водителям
    driver_msg = (
        f'{icon} <b>Новый заказ #{oid}</b>\n\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Откуда:</b> {order["from_address"]}\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Куда:</b> {order["to_address"]}\n'
        f'<tg-emoji emoji-id="5778479949572738874">↔️</tg-emoji> <b>Расстояние:</b> {order["distance"]:.1f} км\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Оплата:</b> {order["price"]:.0f} ₽\n'
        f'<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> <b>Комментарий:</b> {order["comment"] or "нет"}'
    )
    for drv in drivers:
        try:
            await bot.send_message(drv["tg_id"], driver_msg, parse_mode=ParseMode.HTML, reply_markup=kb_accept(oid))
        except Exception:
            pass

    await callback.message.edit_text(
        f'{icon} <b>{type_name} #{oid} — оформлен!</b>\n\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Откуда:</b> {order["from_address"]}\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Куда:</b> {order["to_address"]}\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Стоимость:</b> {order["price"]:.0f} ₽\n\n'
        f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> Ищем водителя... Ожидайте!',
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отменить заказ", callback_data=f"cancel_order_{oid}", icon_custom_emoji_id="5870657884844462243")],
            [InlineKeyboardButton(text="Главное меню",   callback_data="main_menu",           icon_custom_emoji_id="5873147866364514353")],
        ]))
    await callback.answer()

# ===== CANCEL ORDER =====

@router.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: CallbackQuery, bot: Bot):
    oid = int(callback.data.split("_")[-1])
    order = get_order(oid)
    if not order: await callback.answer("Заказ не найден.", show_alert=True); return
    if order["status"] in ("completed","cancelled"):
        await callback.answer("Заказ уже завершён.", show_alert=True); return
    update_order(oid, status="cancelled")
    if order.get("driver_id"):
        try:
            await bot.send_message(order["driver_id"],
                f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Заказ #{oid} отменён пассажиром.</b>',
                parse_mode=ParseMode.HTML)
        except Exception: pass
    user = get_user(callback.from_user.id)
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Заказ #{oid} отменён.</b>\n\n' + main_menu_text(user),
        parse_mode=ParseMode.HTML, reply_markup=kb_main(user))
    await callback.answer()

@router.callback_query(F.data == "cancel_order_flow")
async def cancel_flow(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user = get_user(callback.from_user.id)
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Отменено.\n\n' + main_menu_text(user),
        parse_mode=ParseMode.HTML, reply_markup=kb_main(user))
    await callback.answer()

# ===== DRIVER ACTIONS =====

@router.callback_query(F.data.startswith("accept_order_"))
async def accept_order(callback: CallbackQuery, bot: Bot):
    oid = int(callback.data.split("_")[-1])
    order = get_order(oid)
    driver = get_user(callback.from_user.id)
    if not order: await callback.answer("Заказ не найден.", show_alert=True); return
    if order["status"] != "pending": await callback.answer("Заказ уже принят другим водителем.", show_alert=True); return
    if not driver or driver["role"] not in ("driver","admin"): await callback.answer("Только для водителей.", show_alert=True); return
    update_order(oid, status="accepted", driver_id=callback.from_user.id, accepted_at=datetime.now().isoformat())
    t = eta(order["distance"])
    try:
        await bot.send_message(order["passenger_id"],
            f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Водитель найден!</b>\n\n'
            f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>{driver["full_name"]}</b>\n'
            f'📱 Телефон: <b>{driver["phone"] or "не указан"}</b>\n'
            f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> Подъедет примерно через <b>{t} мин</b>',
            parse_mode=ParseMode.HTML)
    except Exception: pass
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Заказ #{oid} принят!</b>\n\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Откуда:</b> {order["from_address"]}\n'
        f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Куда:</b> {order["to_address"]}\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Сумма:</b> {order["price"]:.0f} ₽',
        parse_mode=ParseMode.HTML, reply_markup=kb_driver_active(oid))
    await callback.answer("Заказ принят!", show_alert=True)

@router.callback_query(F.data.startswith("decline_order_"))
async def decline_order(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("Заказ отклонён.")

@router.callback_query(F.data.startswith("arrived_order_"))
async def arrived_order(callback: CallbackQuery, bot: Bot):
    oid = int(callback.data.split("_")[-1])
    order = get_order(oid)
    if not order: await callback.answer(); return
    try:
        await bot.send_message(order["passenger_id"],
            '<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Водитель подъехал!</b>\n\nВыходите.',
            parse_mode=ParseMode.HTML)
    except Exception: pass
    await callback.message.edit_reply_markup(reply_markup=kb_driver_complete(oid))
    await callback.answer("Пассажир уведомлён!", show_alert=True)

@router.callback_query(F.data.startswith("complete_order_"))
async def complete_order(callback: CallbackQuery, bot: Bot):
    oid = int(callback.data.split("_")[-1])
    order = get_order(oid)
    if not order: await callback.answer(); return
    update_order(oid, status="completed", completed_at=datetime.now().isoformat())
    driver = get_user(callback.from_user.id)
    if driver:
        update_user(callback.from_user.id,
                    total_earned=driver["total_earned"] + order["price"],
                    total_orders=driver["total_orders"] + 1)
    try:
        await bot.send_message(order["passenger_id"],
            f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>Поездка завершена!</b>\n\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Сумма: <b>{order["price"]:.0f} ₽</b>\n\n'
            f'Спасибо, что пользуетесь ЕнакиевоТакси!',
            parse_mode=ParseMode.HTML, reply_markup=kb_menu())
    except Exception: pass
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> <b>Поездка #{oid} завершена!</b>\n\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Заработано: <b>{order["price"]:.0f} ₽</b>',
        parse_mode=ParseMode.HTML, reply_markup=kb_menu())
    await callback.answer("Поездка завершена!", show_alert=True)

@router.callback_query(F.data.startswith("driver_cancel_"))
async def driver_cancel(callback: CallbackQuery, bot: Bot):
    oid = int(callback.data.split("_")[-1])
    order = get_order(oid)
    if not order: await callback.answer(); return
    update_order(oid, status="cancelled")
    try:
        await bot.send_message(order["passenger_id"],
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Водитель отменил заказ.</b>\n\nПопробуйте снова.',
            parse_mode=ParseMode.HTML, reply_markup=kb_menu())
    except Exception: pass
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Заказ #{oid} отменён.</b>',
        parse_mode=ParseMode.HTML, reply_markup=kb_menu())
    await callback.answer()

# ===== ADMIN =====

@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    await callback.message.edit_text(
        '<tg-emoji emoji-id="5870982283724328568">⚙</tg-emoji> <b>Панель администратора</b>\n\nВыберите действие:',
        parse_mode=ParseMode.HTML, reply_markup=kb_admin())
    await callback.answer()

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    s = get_stats()
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="5870921681735781843">📊</tg-emoji> <b>Статистика</b>\n\n'
        f'<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> Пользователей: <b>{s["total_users"]}</b>\n'
        f'🚗 Водителей: <b>{s["total_drivers"]}</b>\n'
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Онлайн: <b>{s["online_drivers"]}</b>\n\n'
        f'<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> Заказов всего: <b>{s["total_orders"]}</b>\n'
        f'🏁 Завершено: <b>{s["completed_orders"]}</b>\n'
        f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> Выручка: <b>{s["total_revenue"]:.0f} ₽</b>\n\n'
        f'<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Заблокировано: <b>{s["banned_users"]}</b>',
        parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel"))
    await callback.answer()

@router.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    users = get_all_users(10)
    rows = [[InlineKeyboardButton(
        text=f'{u["full_name"][:22]} ({role_label(u["role"])[:4]})',
        callback_data=f'view_user_{u["tg_id"]}',
        icon_custom_emoji_id="5870994129244131212"
    )] for u in users]
    rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_panel", icon_custom_emoji_id="5893057118545646106")])
    await callback.message.edit_text(
        '<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> <b>Пользователи (последние 10)</b>',
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

@router.callback_query(F.data.startswith("view_user_"))
async def view_user(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    target_id = int(callback.data.split("_")[-1])
    u = get_user(target_id)
    if not u: await callback.answer("Не найден.", show_alert=True); return
    text = (f'<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>{u["full_name"]}</b>\n\n'
            f'ID: <code>{u["tg_id"]}</code>\n'
            f'Username: @{u["username"] or "—"}\nТелефон: {u["phone"] or "—"}\n'
            f'Роль: {role_label(u["role"])}\n'
            f'Статус: {"🔒 Заблокирован" if u["is_banned"] else ("🟢 Онлайн" if u["is_online"] else "🔴 Офлайн")}\n'
            f'Заказов: {u["total_orders"]}\nРегистрация: {u["created_at"][:10]}')
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_manage_user(target_id, u))
    await callback.answer()

@router.callback_query(F.data == "admin_search_user")
async def admin_search(callback: CallbackQuery, state: FSMContext):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    await state.set_state(AdminStates.search_user)
    await callback.message.edit_text(
        '<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> <b>Поиск</b>\n\nВведите ID, username или имя:',
        parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel"))

@router.message(AdminStates.search_user)
async def admin_search_exec(message: Message, state: FSMContext):
    admin = get_user(message.from_user.id)
    if not admin or admin["role"] != "admin": return
    results = search_user(message.text.strip())
    await state.clear()
    if not results:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> <b>Ничего не найдено.</b>',
            parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel")); return
    rows = [[InlineKeyboardButton(
        text=f'{u["full_name"][:22]} ({role_label(u["role"])[:4]})',
        callback_data=f'view_user_{u["tg_id"]}',
        icon_custom_emoji_id="5870994129244131212"
    )] for u in results[:5]]
    rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_panel", icon_custom_emoji_id="5893057118545646106")])
    await message.answer(
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Найдено: {len(results)}</b>',
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("set_driver_"))
async def set_driver(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    tid = int(callback.data.split("_")[-1]); update_user(tid, role="driver")
    await callback.answer(f'✅ {get_user(tid)["full_name"]} теперь водитель!', show_alert=True)
    await view_user(callback)

@router.callback_query(F.data.startswith("set_passenger_"))
async def set_passenger(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    tid = int(callback.data.split("_")[-1]); update_user(tid, role="passenger", is_online=0)
    await callback.answer(f'✅ {get_user(tid)["full_name"]} теперь пассажир!', show_alert=True)
    await view_user(callback)

@router.callback_query(F.data.startswith("set_admin_"))
async def set_admin(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    tid = int(callback.data.split("_")[-1]); update_user(tid, role="admin")
    await callback.answer(f'✅ {get_user(tid)["full_name"]} теперь администратор!', show_alert=True)
    await view_user(callback)

@router.callback_query(F.data.startswith("ban_"))
async def ban_user(callback: CallbackQuery, state: FSMContext):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    tid = int(callback.data.split("_")[-1]); u = get_user(tid)
    if not u: await callback.answer("Не найден.", show_alert=True); return
    await state.set_state(AdminStates.ban_reason); await state.update_data(ban_target=tid)
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> <b>Блокировка: {u["full_name"]}</b>\n\nВведите причину:',
        parse_mode=ParseMode.HTML, reply_markup=kb_back(f"view_user_{tid}"))

@router.message(AdminStates.ban_reason)
async def ban_reason(message: Message, state: FSMContext, bot: Bot):
    admin = get_user(message.from_user.id)
    if not admin or admin["role"] != "admin": return
    data = await state.get_data(); tid = data.get("ban_target"); reason = message.text
    await state.clear()
    if not tid: return
    update_user(tid, is_banned=1, is_online=0)
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO ban_log (admin_id,user_id,reason,action) VALUES (?,?,?,?)",
              (message.from_user.id, tid, reason, "ban")); conn.commit(); conn.close()
    u = get_user(tid)
    await message.answer(
        f'<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> <b>Заблокирован: {u["full_name"]}</b>\n'
        f'<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Причина: {reason}',
        parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel"))
    try:
        await bot.send_message(tid,
            f'<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> <b>Вы заблокированы.</b>\n\n'
            f'<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> Причина: {reason}',
            parse_mode=ParseMode.HTML)
    except: pass

@router.callback_query(F.data.startswith("unban_"))
async def unban_user(callback: CallbackQuery, bot: Bot):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    tid = int(callback.data.split("_")[-1]); update_user(tid, is_banned=0)
    conn = db(); c = conn.cursor()
    c.execute("INSERT INTO ban_log (admin_id,user_id,reason,action) VALUES (?,?,?,?)",
              (callback.from_user.id, tid, "Разблокирован администратором", "unban")); conn.commit(); conn.close()
    u = get_user(tid)
    await callback.answer(f'✅ {u["full_name"]} разблокирован!', show_alert=True)
    try:
        await bot.send_message(tid,
            '<tg-emoji emoji-id="6037496202990194718">🔓</tg-emoji> <b>Вы разблокированы!</b>\n\nДобро пожаловать обратно!',
            parse_mode=ParseMode.HTML)
    except: pass
    await view_user(callback)

@router.callback_query(F.data == "admin_orders")
async def admin_orders(callback: CallbackQuery):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 10")
    orders = [dict(zip(COLS_ORDER, r)) for r in c.fetchall()]; conn.close()
    text = '<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji> <b>Последние 10 заказов</b>\n\n'
    for o in orders:
        icon = "📦" if o["order_type"] == "delivery" else "🚕"
        text += (f'{icon} <b>#{o["id"]}</b> — {status_label(o["status"])}\n'
                 f'  <i>{o["from_address"]} → {o["to_address"]}</i>\n'
                 f'  <tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> {o["price"]:.0f} ₽ | {o["created_at"][:16]}\n\n')
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel"))
    await callback.answer()

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    admin = get_user(callback.from_user.id)
    if not admin or admin["role"] != "admin": await callback.answer("Нет доступа.", show_alert=True); return
    await state.set_state(AdminStates.broadcast)
    await callback.message.edit_text(
        '<tg-emoji emoji-id="6039422865189638057">📣</tg-emoji> <b>Рассылка</b>\n\nНапишите сообщение для всех пользователей:',
        parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel"))

@router.message(AdminStates.broadcast)
async def do_broadcast(message: Message, state: FSMContext, bot: Bot):
    admin = get_user(message.from_user.id)
    if not admin or admin["role"] != "admin": return
    await state.clear()
    conn = db(); c = conn.cursor()
    c.execute("SELECT tg_id FROM users WHERE is_banned=0")
    ids = [r[0] for r in c.fetchall()]; conn.close()
    sent = failed = 0
    for tid in ids:
        try:
            await bot.send_message(tid,
                f'<tg-emoji emoji-id="6039422865189638057">📣</tg-emoji> <b>Сообщение от администрации:</b>\n\n{message.text}',
                parse_mode=ParseMode.HTML)
            sent += 1; await asyncio.sleep(0.05)
        except: failed += 1
    await message.answer(
        f'<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> <b>Рассылка завершена!</b>\n\n'
        f'<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> Отправлено: <b>{sent}</b>\n'
        f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибок: <b>{failed}</b>',
        parse_mode=ParseMode.HTML, reply_markup=kb_back("admin_panel"))

# ===== HELP =====

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        f'<tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> <b>Помощь — ЕнакиевоТакси</b>\n\n'
        f'🚕 Такси: {BASE_PRICE}₽ посадка + {PRICE_PER_KM}₽/км\n'
        f'📦 Доставка: 200₽ посадка + 50₽/км\n\n'
        f'<tg-emoji emoji-id="5873147866364514353">🏘</tg-emoji> Город: {CITY_NAME} (ДНР)\n\n'
        f'По вопросам обратитесь к администратору.',
        parse_mode=ParseMode.HTML, reply_markup=kb_menu())

# ==================== HTTP API для WebApp ====================

async def handle_order(request: web.Request) -> web.Response:
    """POST /api/order — принимает заказ из WebApp (mobile-friendly)"""
    # CORS preflight
    if request.method == "OPTIONS":
        return web.Response(headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        })

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400, headers=headers)

    user_id = body.get("user_id")
    raw     = body.get("data")

    if not user_id or not raw:
        return web.json_response({"ok": False, "error": "missing user_id or data"}, status=400, headers=headers)

    try:
        user_id = int(user_id)
    except Exception:
        return web.json_response({"ok": False, "error": "bad user_id"}, status=400, headers=headers)

    bot: Bot = request.app["bot"]

    # Проверяем пользователя
    user = get_user(user_id)
    if not user:
        return web.json_response({"ok": False, "error": "user not found"}, status=404, headers=headers)
    if user["is_banned"]:
        return web.json_response({"ok": False, "error": "banned"}, status=403, headers=headers)

    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return web.json_response({"ok": False, "error": "bad data"}, status=400, headers=headers)

    order_type   = data.get("order_type", "taxi")
    from_address = data.get("from_address", "").strip()
    to_address   = data.get("to_address",   "").strip()
    distance     = float(data.get("distance", 0))
    price        = float(data.get("price",    0))
    comment      = data.get("comment", "").strip()
    from_lat     = data.get("from_lat")
    from_lon     = data.get("from_lon")
    if from_lat is not None: from_lat = float(from_lat)
    if from_lon is not None: from_lon = float(from_lon)

    if not from_address or not to_address or distance <= 0:
        return web.json_response({"ok": False, "error": "incomplete order"}, status=400, headers=headers)

    if price <= 0:
        price = calc_price(distance, order_type)

    oid = create_order(
        passenger_id=user_id, order_type=order_type,
        from_addr=from_address, to_addr=to_address,
        price=round(price), distance=distance, comment=comment,
        from_lat=from_lat, from_lon=from_lon)

    icon      = "📦" if order_type == "delivery" else "🚕"
    type_name = "Доставка" if order_type == "delivery" else "Такси"
    t         = eta(distance)

    try:
        await bot.send_message(
            user_id,
            f'{icon} <b>{type_name} #{oid}</b>\n\n'
            f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Откуда:</b> {from_address}\n'
            f'<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji> <b>Куда:</b> {to_address}\n'
            f'<tg-emoji emoji-id="5778479949572738874">↔️</tg-emoji> <b>Расстояние:</b> {distance:.1f} км\n'
            f'<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji> <b>Стоимость:</b> {round(price)} ₽\n'
            f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> <b>В пути примерно:</b> ~{t} мин\n'
            f'<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji> <b>Комментарий:</b> {comment or "нет"}\n\n'
            f'Подтвердить заказ?',
            parse_mode=ParseMode.HTML, reply_markup=kb_confirm(oid))
    except Exception as e:
        logger.error(f"send_message error: {e}")
        return web.json_response({"ok": False, "error": "send failed"}, status=500, headers=headers)

    return web.json_response({"ok": True, "order_id": oid}, headers=headers)


async def handle_options(request: web.Request) -> web.Response:
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })


# ==================== MAIN ====================

async def main():
    init_db()
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.include_router(order_router)

    # aiohttp web app
    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/api/order", handle_order)
    app.router.add_route("OPTIONS", "/api/order", handle_options)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", API_PORT)
    await site.start()
    logger.info(f"HTTP API запущен на порту {API_PORT} → {API_URL}/api/order")

    logger.info("ЕнакиевоТакси запущен!")
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())