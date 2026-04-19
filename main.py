import asyncio
import logging
import os
import re
import json
import time
import csv
import io
import math
from datetime import datetime
from io import BytesIO

import aiohttp
import asyncpg
import redis.asyncio as redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import ConnectionError, TimeoutError

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiohttp import web
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.redis import RedisStorage

# ==========================================
# ⚙️ الإعدادات الأساسية (من متغيرات البيئة حصراً)
# ==========================================
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")
PG_DSN = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8080))

SITES = {
    "alisms": {
        "name": "سيرفر ALISMS 🟢",
        "url": "https://alisms.org/stubs/handler_api.php",
        "key": os.getenv("API_KEY_ALISMS")
    },
    "grizzly": {
        "name": "سيرفر Grizzly 🐻",
        "url": "https://api.grizzlysms.com/stubs/handler_api.php",
        "key": os.getenv("API_KEY_GRIZZLY")
    }
}
SITE_FEATURES = {
    "alisms": "🟢 *مميزات سيرفر ALISMS:*\n✅ أسعار تنافسية جداً.\n✅ وصول أكواد (SMS) بسرعة فائقة.\n✅ نسبة نجاح مرتفعة في التفعيلات.",
    "grizzly": "🐻 *مميزات سيرفر Grizzly SMS:*\n✅ مخزون ضخم لمختلف الخدمات العالمية.\n✅ تغطية شاملة لأكثر من 170 دولة.\n✅ أرقام حصرية ونسبة حظر شبه معدومة."
}

# ==========================================
# 📊 القواميس الكاملة للخدمات والدول
# ==========================================
# جميع الخدمات
ALL_SERVICES = [
    ("واتساب", "wa"), ("تليجرام", "tg"), ("انستقرام", "ig"), ("جوجل", "go"),
    ("فيسبوك", "fb"), ("تيك توك", "lf"), ("تويتر (X)", "tw"), ("سناب شات", "sn"),
    ("حراج", "ha"), ("نون", "nn"), ("فايبر", "vi"), ("اوبر", "ub"),
    ("ديسكورد", "ds"), ("امازون", "am"), ("مايكروسوفت", "mm"), ("وي شات", "kf"),
    ("نتفليكس", "nf"), ("فكونتاكتي", "vk"), ("لاين", "li"), ("ياهو", "ya")
]
ALL_SERVICES_MAP = {code: name for name, code in ALL_SERVICES}

# جميع الدول
ALL_COUNTRIES = [
    ("روسيا 🇷🇺", "0"), ("أوكرانيا 🇺🇦", "1"), ("كازاخستان 🇰🇿", "2"), ("الصين 🇨🇳", "3"),
    ("الفلبين 🇵🇭", "4"), ("إندونيسيا 🇮🇩", "6"), ("ماليزيا 🇲🇾", "7"), ("فيتنام 🇻🇳", "10"),
    ("قيرغيزستان 🇰🇬", "11"), ("امريكا 🇺🇸", "187"), ("إسرائيل 🇮🇱", "13"), ("مصر 🇪🇬", "21"),
    ("اليمن 🇾🇪", "30"), ("كندا 🇨🇦", "36"), ("المغرب 🇲🇦", "37"), ("ألمانيا 🇩🇪", "43"),
    ("العراق 🇮🇶", "47"), ("الكويت 🇰🇼", "52"), ("السعودية 🇸🇦", "53"), ("الجزائر 🇩🇿", "58"),
    ("تركيا 🇹🇷", "62"), ("فرنسا 🇫🇷", "78"), ("الامارات 🇦🇪", "95"), ("عمان 🇴🇲", "110"),
    ("سوريا 🇸🇾", "118"), ("لبنان 🇱🇧", "135"), ("فلسطين 🇵🇸", "143"), ("قطر 🇶🇦", "144"),
    ("البحرين 🇧🇭", "145"), ("بريطانيا 🇬🇧", "16"), ("إسبانيا 🇪🇸", "56"), ("إيطاليا 🇮🇹", "86")
]
ALL_COUNTRIES_MAP = {code: name for name, code in ALL_COUNTRIES}

# ==========================================
# 🛠️ التهيئة الأساسية (قواعد البيانات و Redis)
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
bot_session = AiohttpSession()
bot = Bot(token=TOKEN, session=bot_session, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
retry_strategy = Retry(ExponentialBackoff(cap=2, base=0.2), retries=5)
redis_client = redis.from_url(
    REDIS_URL, decode_responses=True, health_check_interval=10,
    socket_keepalive=True, retry_on_timeout=True, retry=retry_strategy
)
redis_storage = RedisStorage(redis=redis_client)
dp = Dispatcher(storage=redis_storage)
router = Router()
dp.include_router(router)
db_pool = None

async def keep_alive_connections():
    while True:
        try:
            await redis_client.ping()
            if db_pool:
                async with db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
        except Exception as e:
            logging.warning(f"Keep-alive check failed: {e}")
        await asyncio.sleep(45)

async def init_db():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(PG_DSN, command_timeout=10, max_inactive_connection_lifetime=300)
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS history (
                    id SERIAL PRIMARY KEY, user_id BIGINT, phone TEXT,
                    service TEXT, code TEXT, price NUMERIC, date TIMESTAMP, operator TEXT, server TEXT
                );
            ''')
    except Exception as e:
        logging.error(f"DB Init Error: {e}")

async def get_user_site(state: FSMContext):
    data = await state.get_data()
    return data.get("site", "alisms")

async def api_request(site_key, params, max_retries=3):
    url = SITES[site_key]["url"]
    params["api_key"] = SITES[site_key]["key"]
    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.get(url, params=params, timeout=7) as response:
                    if response.status == 200:
                        text = await response.text()
                        try:
                            # First try to parse as JSON, common for structured responses
                            return json.loads(text)
                        except json.JSONDecodeError:
                            # If it fails, return the raw text (e.g., "STATUS_OK:12345")
                            return text
                    else:
                        logging.warning(f"API request failed for {site_key} with status {response.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.warning(f"API request attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(1)
    return None

async def get_balance(site_key):
    res = await api_request(site_key, {"action": "getBalance"})
    if isinstance(res, str) and "BALANCE" in res and ":" in res:
        try:
            return float(res.split(':')[1])
        except (ValueError, IndexError):
            return 0.0
    # Grizzly SMS returns JSON for balance
    if isinstance(res, dict) and 'balance' in res:
        try:
            return float(res['balance'])
        except ValueError:
            return 0.0
    return 0.0

# ==========================================
# 🎛️ واجهة المستخدم (الكيبورد الأساسي)
# ==========================================
def main_kb():
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text="🚀 البدء"), KeyboardButton(text="🛑 إيقاف جميع الصيادات")],
        [KeyboardButton(text="🛒 طلب رقم جديد")],
        [KeyboardButton(text="💰 فحص الرصيد"), KeyboardButton(text="💻 لوحة التحكم")],
        [KeyboardButton(text="🔄 تغيير السيرفر")],
        [KeyboardButton(text="📊 الإحصائيات"), KeyboardButton(text="📂 استخراج Excel")],
        [KeyboardButton(text="🇸🇦 طلب مباشر (222)")]
    ])

# ==========================================
# 🧭 دوال التصفح ونظام الصفحات (Pagination)
# ==========================================
def build_pagination_kb(items_list, page, per_page, prefix, action_prefix):
    total_pages = math.ceil(len(items_list) / per_page)
    start = page * per_page
    end = start + per_page
    current_items = items_list[start:end]
    kb = []
    row = []
    for name, code in current_items:
        row.append(InlineKeyboardButton(text=name, callback_data=f"{action_prefix}_{code}"))
        if len(row) == 3:  # عرض 3 أزرار في كل صف
            kb.append(row)
            row = []
    if row:
        kb.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ السابق", callback_data=f"{prefix}_{page-1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="التالي ➡️", callback_data=f"{prefix}_{page+1}"))
    if nav_row:
        kb.append(nav_row)

    return InlineKeyboardMarkup(inline_keyboard=kb)

# ==========================================
# ⚡ الأوامر الأساسية وإدارة الجلسات
# ==========================================
@router.message(F.text.in_(["/start", "🚀 البدء"]))
async def start_cmd(message: Message, state: FSMContext):
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 الشراء من ALISMS", callback_data="set_site_alisms")],
        [InlineKeyboardButton(text="🐻 الشراء من Grizzly SMS", callback_data="set_site_grizzly")]
    ])
    await message.answer("💎 *مرحباً بك في نظام الصياد VIP*\n\n👇 يرجى اختيار السيرفر الذي ترغب بالعمل عليه:", reply_markup=markup)

@router.message(F.text == "🔄 تغيير السيرفر")
async def change_site_btn(message: Message, state: FSMContext):
    await start_cmd(message, state) # Pass state to start_cmd

@router.callback_query(F.data.startswith("set_site_"))
async def change_site_callback(call: CallbackQuery, state: FSMContext):
    site_key = call.data.replace("set_site_", "")
    await state.update_data(site=site_key)
    try:
        await call.message.delete()
    except TelegramAPIError:
        pass
    await call.message.answer(f"✅ *تم التفعيل على:* `{SITES[site_key]['name']}`\n\n{SITE_FEATURES[site_key]}\n\n👇 *ابدأ من القائمة السفلية:*", reply_markup=main_kb())
    await call.answer()

@router.message(F.text == "💰 فحص الرصيد")
async def check_bal(message: Message, state: FSMContext):
    site_key = await get_user_site(state)
    msg = await message.answer(f"⏳ جارٍ فحص الرصيد في ({SITES[site_key]['name']})...")
    bal = await get_balance(site_key)
    await msg.edit_text(f"💰 رصيدك في ({SITES[site_key]['name']}): `{bal:.2f}$`")

@router.message(F.text == "💻 لوحة التحكم")
async def admin_panel_cmd(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ عذراً، هذه القائمة مخصصة للمدير فقط.")
    await message.answer("🛠️ *لوحة تحكم المدير:*\nيمكنك استخدام أزرار استخراج Excel والإحصائيات من الكيبورد السفلي لمتابعة النظام.")

@router.message(F.text == "🛑 إيقاف جميع الصيادات")
async def global_stop_hunts(message: Message):
    keys = await redis_client.keys(f"hunt:{message.chat.id}:*")
    count = 0
    if keys:
        count = len(keys)
        await redis_client.delete(*keys)
    await message.answer(f"🛑 *تم إيقاف جميع عمليات البحث ({count}) الخاصة بك بنجاح.*")

# ==========================================
# 🔍 نظام الطلب الموحد (خدمة -> دولة -> أسعار -> صيد)
# ==========================================
@router.message(F.text == "🛒 طلب رقم جديد")
async def order_new_number(message: Message, state: FSMContext):
    await state.update_data(search_srv=None, search_cnt=None)
    markup = build_pagination_kb(ALL_SERVICES, page=0, per_page=15, prefix="pg_srv", action_prefix="selsrv")
    await message.answer("🔍 *الخطوة 1: اختر الخدمة التي تريد تفعيلها:*", reply_markup=markup)

@router.callback_query(F.data.startswith("pg_srv_"))
async def paginate_services(call: CallbackQuery):
    page = int(call.data.split("_")[2])
    markup = build_pagination_kb(ALL_SERVICES, page=page, per_page=15, prefix="pg_srv", action_prefix="selsrv")
    try:
        await call.message.edit_text("🔍 *الخطوة 1: اختر الخدمة التي تريد تفعيلها:*", reply_markup=markup)
    except TelegramAPIError:
        pass
    await call.answer()

@router.callback_query(F.data.startswith("selsrv_"))
async def process_service_selection(call: CallbackQuery, state: FSMContext):
    srv_code = call.data.split("_")[1]
    await state.update_data(search_srv=srv_code)
    try:
        await call.message.edit_text("⏳ جارٍ الحذف...", reply_markup=None) # Improve UX
        await call.message.delete()
    except TelegramAPIError:
        pass
    markup = build_pagination_kb(ALL_COUNTRIES, page=0, per_page=15, prefix="pg_cnt", action_prefix="selcnt")
    await call.message.answer(f"✅ تم اختيار: `{ALL_SERVICES_MAP.get(srv_code, srv_code)}`\n\n🌍 *الخطوة 2: اختر الدولة:*", reply_markup=markup)
    await call.answer()


@router.callback_query(F.data.startswith("pg_cnt_"))
async def paginate_countries(call: CallbackQuery, state: FSMContext):
    page = int(call.data.split("_")[2])
    data = await state.get_data()
    srv_code = data.get("search_srv", "غير معروف")
    markup = build_pagination_kb(ALL_COUNTRIES, page=page, per_page=15, prefix="pg_cnt", action_prefix="selcnt")
    try:
        await call.message.edit_text(f"✅ تم اختيار: `{ALL_SERVICES_MAP.get(srv_code, srv_code)}`\n\n🌍 *الخطوة 2: اختر الدولة:*", reply_markup=markup)
    except TelegramAPIError:
        pass
    await call.answer()

@router.callback_query(F.data.startswith("selcnt_"))
async def process_country_and_prices(call: CallbackQuery, state: FSMContext):
    cnt_code = call.data.split("_")[1]
    data = await state.get_data()
    srv_code = data.get("search_srv")
    site_key = await get_user_site(state)

    if not srv_code:
        return await call.answer("حدث خطأ، يرجى طلب الرقم من البداية", show_alert=True)

    await call.message.edit_text("⏳ جاري فحص الرصيد وجلب المشغلين والأسعار من السيرفر...")
    
    bal = await get_balance(site_key)
    prices_res = await api_request(site_key, {"action": "getPrices", "service": srv_code, "country": cnt_code})

    operators_data = {}
    if isinstance(prices_res, dict):
        # Universal way to access nested data
        operators_data = prices_res.get(str(cnt_code), {}).get(srv_code, {})

    if not operators_data:
        try:
            await call.message.edit_text(f"❌ لا توجد أرقام متاحة لخدمة `{ALL_SERVICES_MAP.get(srv_code, srv_code)}` في `{ALL_COUNTRIES_MAP.get(cnt_code, cnt_code)}` حالياً.")
        except TelegramAPIError: pass
        return await call.answer()

    text = f"📊 *الأسعار المتاحة ({SITES[site_key]['name']}):*\n💰 رصيدك الحالي: `{bal:.2f}$`\n\n"
    kb = []
    
    # Sort operators by cost
    sorted_operators = sorted(operators_data.items(), key=lambda item: item[1].get('cost', float('inf')))

    for op, details in sorted_operators:
        cost = float(details.get('cost', 0.0))
        count = int(details.get('count', 0))
        text += f"📡 المشغل: `{op}` | 💰 السعر: `{cost:.2f}$` | 🔢 المتوفر: `{count}`\n"
        if bal >= cost and count > 0:
            kb.append([InlineKeyboardButton(text=f"🚀 صيد {op} ({cost:.2f}$)", callback_data=f"hunt_{srv_code}_{cnt_code}_{op}_{cost}")])

    if not kb:
        text += "\n\n❌ *رصيدك لا يكفي لشراء أي رقم من هؤلاء المشغلين أو لا توجد أرقام متاحة!*"
    
    try:
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except TelegramAPIError: pass
    await call.answer()

# ==========================================
# 🎯 الصياد الذكي (يصيد رقم واحد ويتوقف)
# ==========================================
@router.callback_query(F.data.startswith("hunt_"))
async def start_hunting_btn(call: CallbackQuery, state: FSMContext):
    parts = call.data.split("_")
    try:
        srv, cid, op, cost_str = parts[1], parts[2], parts[3], parts[4]
        cost = float(cost_str)
    except (IndexError, ValueError) as e:
        logging.error(f"Invalid hunt callback data: {call.data} | Error: {e}")
        return await call.answer("❌ بيانات الطلب غير صالحة!", show_alert=True)

    site_key = await get_user_site(state)
    
    bal = await get_balance(site_key)
    if bal < cost:
        return await call.answer("❌ رصيدك غير كافٍ!", show_alert=True)

    await call.answer("⚡ جاري تشغيل الصياد...", show_alert=False)
    asyncio.create_task(hunt_single_number(call.message.chat.id, srv, cid, op, cost, site_key))
    try:
        await call.message.delete()
    except TelegramAPIError:
        pass

@router.message(F.text == "🇸🇦 طلب مباشر (222)")
async def saudi_direct_222(message: Message, state: FSMContext):
    site_key = await get_user_site(state)
    bal = await get_balance(site_key)
    if bal <= 0: # A small cost is assumed
        return await message.answer("❌ رصيدك غير كافٍ للطلب!")
    await message.answer(f"⚡ جاري تشغيل الصياد لـ 222 (السعودية) على سيرفر {SITES[site_key]['name']}...")
    # Assume cost is low, but greater than 0. We don't know the exact cost here.
    asyncio.create_task(hunt_single_number(message.chat.id, "wa", "53", "222", 0.1, site_key))

async def hunt_single_number(chat_id, srv, cid, op, cost, site_key):
    timestamp = int(time.time())
    hunt_id = f"hunt:{chat_id}:{timestamp}"
    await redis_client.setex(hunt_id, 600, "active")  # 10-minute hunt lifespan

    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛑 إيقاف هذا الصياد", callback_data=f"stophunt_{chat_id}_{timestamp}")]])
    
    try:
        msg = await bot.send_message(chat_id, f"🚀 *الصياد يعمل الآن...*\nنبحث عن رقم لخدمة `{ALL_SERVICES_MAP.get(srv, srv)}` من المشغل `{op}`", reply_markup=markup)
    except TelegramAPIError:
        return

    delay = 0.5
    caught_data = None
    
    try:
        while await redis_client.get(hunt_id):
            params = {"action": "getNumberV2", "service": srv, "country": cid, "operator": op}
            res = await api_request(site_key, params, max_retries=1)
            
            if isinstance(res, dict) and ('number' in res or 'phoneNumber' in res):
                caught_data = res
                caught_data['op'] = op
                caught_data['price'] = cost # Use the cost passed from the button
                await redis_client.delete(hunt_id)
                break
                
            if isinstance(res, str) and ("NO_NUMBERS" in res or "NO_BALANCE" in res or "SQL_ERROR" in res):
                logging.warning(f"Hunter stopping due to API response: {res}")
                break

            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 3.0)
    except Exception as e:
        logging.error(f"Hunter loop error: {e}")
    
    try:
        await bot.delete_message(chat_id, msg.message_id)
    except TelegramAPIError:
        pass 
    
    if caught_data:
        await handle_success_hunt(chat_id, caught_data, srv, cid, site_key)
    else:
        await bot.send_message(chat_id, f"⏳ *توقف الصياد.*\nلم يتم العثور على رقم لـ `{ALL_SERVICES_MAP.get(srv, srv)}` من المشغل `{op}` في الوقت الحالي.")


async def handle_success_hunt(chat_id, data, srv, cid, site_key):
    act_id = data.get('id') or data.get('activationId')
    phone = data.get('number') or data.get('phoneNumber')
    op = data.get('op', 'unknown')
    price = data.get('price', 0.0)
    
    # Store all necessary info for potential cancellation
    session_data = f"{phone}|{srv}|{op}|{price}|{site_key}"
    await redis_client.hset(f"active_orders:{chat_id}", str(act_id), session_data)
    await redis_client.setex(f"session:{act_id}", 1200, "active") # 20 minutes active session

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ إلغاء / حظر", callback_data=f"canc_{act_id}")],
    ])
    
    text = f"🎯 *تم الصيد بنجاح! ({SITES[site_key]['name']})*\n📞 الرقم: `{phone}`\n📡 المشغل: `{op}`\n💰 السعر: `{price:.2f}$`\n⏳ ننتظر وصول الكود..."
    sent_msg = await bot.send_message(chat_id, text, reply_markup=markup)
    asyncio.create_task(auto_check_sms(chat_id, act_id, phone, srv, op, price, site_key, sent_msg.message_id))


async def auto_check_sms(chat_id, act_id, phone, srv, op, price, site_key, msg_id):
    start_time = time.time()
    try:
        while await redis_client.get(f"session:{act_id}"):
            if time.time() - start_time > 1100:  # ~18 minutes
                break
            
            res = await api_request(site_key, {"action": "getStatus", "id": act_id})
            
            if isinstance(res, str) and "STATUS_OK" in res:
                code_match = re.search(r':(\d+)', res)
                if code_match:
                    code = code_match.group(1)
                    await redis_client.delete(f"session:{act_id}") # Stop checking
                    
                    try:
                        async with db_pool.acquire() as conn:
                            await conn.execute("INSERT INTO history (user_id, phone, service, code, price, date, operator, server) VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)", chat_id, phone, srv, code, price, op, site_key)
                    except Exception as e:
                        logging.error(f"DB insert error: {e}")
                    
                    try:
                        await bot.edit_message_text(f"🔔 *تم وصول الكود!*\n📱 الرقم: `{phone}`\n✉️ الكود: `{code}`\n💸 تم خصم: `{price:.2f}$`", chat_id=chat_id, message_id=msg_id, reply_markup=None)
                    except TelegramAPIError:
                        await bot.send_message(chat_id, f"🔔 *تم وصول الكود!*\n📱 الرقم: `{phone}`\n✉️ الكود: `{code}`\n💸 تم خصم: `{price:.2f}$`")
                    return
            
            await asyncio.sleep(10) # Check every 10 seconds
    except Exception as e:
        logging.error(f"Auto check SMS error: {e}")
        
    # If the loop finishes without finding a code
    if await redis_client.get(f"session:{act_id}"):
        await redis_client.delete(f"session:{act_id}")
        await api_request(site_key, {"action": "setStatus", "status": "8", "id": act_id}) # Send cancellation
        try:
            await bot.edit_message_text(f"⌛ *انتهى الوقت ولم يصل الكود للرقم:* `{phone}`\n(تم الإلغاء ولم يتم خصم الرصيد)", chat_id=chat_id, message_id=msg_id, reply_markup=None)
        except TelegramAPIError:
            pass

@router.callback_query(F.data.startswith("canc_"))
async def cancel_number(call: CallbackQuery, state: FSMContext):
    act_id = call.data.split("_")[1]
    
    # Retrieve site_key to send cancellation to the correct server
    session_data_raw = await redis_client.hget(f"active_orders:{call.message.chat.id}", str(act_id))
    if not session_data_raw:
        try: await call.message.edit_text("❌ *تم إلغاء هذا الطلب بالفعل أو انتهت صلاحيته.*")
        except: pass
        return await call.answer("الطلب منتهي", show_alert=True)
        
    site_key = session_data_raw.split('|')[-1]

    # First, tell the API to cancel
    await api_request(site_key, {"action": "setStatus", "status": "8", "id": act_id})
    # Then, delete local session
    await redis_client.delete(f"session:{act_id}")
    await redis_client.hdel(f"active_orders:{call.message.chat.id}", str(act_id))
    
    await call.answer("✅ تم إرسال طلب الإلغاء.", show_alert=False)
    try:
        await call.message.edit_text("❌ *تم إلغاء الرقم بنجاح، لن يتم خصم الرصيد.*")
    except TelegramAPIError:
        pass

@router.callback_query(F.data.startswith("stophunt_"))
async def stop_hunter(call: CallbackQuery):
    parts = call.data.split("_")
    try:
        chat_id_str, timestamp_str = parts[1], parts[2]
        key_to_delete = f"hunt:{chat_id_str}:{timestamp_str}"
        
        # Atomically delete the key and check if it existed
        deleted_count = await redis_client.delete(key_to_delete)
        
        if deleted_count > 0:
            await call.answer("✅ تم إيقاف الصياد بنجاح.", show_alert=True)
            try:
                await call.message.edit_text("🛑 *تم إيقاف هذا الصياد بناءً على طلبك.*")
            except TelegramAPIError:
                pass
        else:
            await call.answer("ℹ️ هذا الصياد متوقف بالفعل.", show_alert=True)
            try:
                await call.message.edit_text("🛑 *هذا الصياد متوقف بالفعل.*")
            except TelegramAPIError:
                pass
    except (IndexError, ValueError):
        await call.answer("❌ خطأ في إيقاف الصياد.", show_alert=True)


# ==========================================
# 📊 الإحصائيات واستخراج Excel
# ==========================================
@router.message(F.text == "📊 الإحصائيات")
async def generate_chart(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ للمدير فقط.")
    
    msg = await message.answer("⏳ جارٍ إنشاء الرسم البياني...")
    site_key = await get_user_site(state)
    
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch("SELECT operator, count(*) as c FROM history WHERE server = $1 GROUP BY operator ORDER BY c DESC LIMIT 7", site_key)
    except Exception as e:
        logging.error(f"Stats DB error: {e}")
        return await msg.edit_text("حدث خطأ في جلب البيانات من قاعدة البيانات.")

    if not records:
        return await msg.edit_text("لا توجد بيانات كافية لإنشاء رسم بياني حالياً.")
    
    ops = [r['operator'] for r in records]
    counts = [r['c'] for r in records]
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(ops, counts, color='skyblue')
    plt.ylabel('عدد الأرقام المباعة')
    plt.title(f"أكثر المشغلين مبيعاً - {SITES[site_key]['name']}")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    
    # Add numbers on top of bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, yval, int(yval), va='bottom') # va: vertical alignment

    buf = BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    
    await message.answer_photo(BufferedInputFile(buf.read(), filename="chart.png"), caption="📊 *تحليل أداء المشغلين*")
    await msg.delete()

@router.message(F.text == "📂 استخراج Excel")
async def export_excel(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ للمدير فقط.")
    
    msg = await message.answer("⏳ جارٍ استخراج البيانات...")
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, user_id, phone, service, code, price, date, operator, server FROM history ORDER BY date DESC")
    except Exception as e:
        logging.error(f"Excel DB error: {e}")
        return await msg.edit_text("خطأ في جلب البيانات من قاعدة البيانات.")

    if not rows:
        return await msg.edit_text("لا توجد بيانات مسجلة لاستخراجها.")
        
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'User ID', 'Phone', 'Service', 'Code', 'Price', 'Date', 'Operator', 'Server'])
    for r in rows:
        writer.writerow([
            r['id'], r['user_id'], r['phone'], ALL_SERVICES_MAP.get(r['service'], r['service']), 
            r['code'], f"{r['price']:.2f}", r['date'].strftime('%Y-%m-%d %H:%M'), r['operator'], r['server']
        ])
    
    file_bytes = output.getvalue().encode('utf-8-sig')
    await message.answer_document(BufferedInputFile(file_bytes, filename="history.csv"), caption="📂 سجل العمليات الكامل.")
    await msg.delete()

# ==========================================
# 🚀 إعداد الخادم وفحص متغيرات البيئة بصرامة
# ==========================================
async def on_startup(bot: Bot):
    await init_db()
    asyncio.create_task(keep_alive_connections())
    await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)

async def on_shutdown(bot: Bot):
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()
    logging.info("Shutting down bot and connections.")

def main():
    required_env_vars = ["BOT_TOKEN", "API_KEY_ALISMS", "API_KEY_GRIZZLY", "DATABASE_URL", "REDIS_URL", "WEBHOOK_HOST", "ADMIN_ID"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logging.critical(f"❌ CRITICAL: The following environment variables are missing: {', '.join(missing_vars)}")
        exit(1)
        
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)

if __name__ == '__main__':
    main()
