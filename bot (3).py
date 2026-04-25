import asyncio
import sqlite3
import os
import math
import logging
import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")
SUPPORT_ID = int(os.getenv("SUPPORT_ID", "8534836236"))

TOKEN = os.getenv("TOKEN", "7903572178:AAG8YwkrEPvxJH9Yc6PHzMdEoCG7RfjO0k8")
GROUP_ID_RAW = os.getenv("GROUP_ID", "-1003998856432")

# Курс рубля к звёздам: сколько рублей в одной звезде.
# Меняйте через переменную окружения RUB_PER_STAR (например "1.5" или "2").
try:
    RUB_PER_STAR = float(os.getenv("RUB_PER_STAR", "1.5"))
    if RUB_PER_STAR <= 0:
        raise ValueError
except ValueError:
    RUB_PER_STAR = 1.5

if not TOKEN:
    raise SystemExit("❌ Не задана переменная окружения TOKEN.")
if not GROUP_ID_RAW:
    raise SystemExit("❌ Не задана переменная окружения GROUP_ID.")
try:
    GROUP_ID = int(GROUP_ID_RAW)
except ValueError:
    raise SystemExit("❌ GROUP_ID должен быть числом, например -1001234567890")

# ================= ENCRYPTION =================

ENC_KEY_RAW = os.getenv("ENCRYPTION_KEY", "f4cZrFpafZKbDyXQRGrBBkJkjIyl1-4Aw0jGHpYCMP0")
if not ENC_KEY_RAW:
    raise SystemExit("❌ Не задана переменная окружения ENCRYPTION_KEY.")

try:
    fernet = Fernet(ENC_KEY_RAW.encode())
except (ValueError, Exception):
    digest = hashlib.sha256(ENC_KEY_RAW.encode()).digest()
    fernet = Fernet(base64.urlsafe_b64encode(digest))

ENC_PREFIX = "enc:"


def enc(text: str) -> str:
    if text is None:
        return text
    return ENC_PREFIX + fernet.encrypt(text.encode()).decode()


def dec(text: str) -> str:
    if not text:
        return text
    if not text.startswith(ENC_PREFIX):
        return text
    try:
        return fernet.decrypt(text[len(ENC_PREFIX):].encode()).decode()
    except InvalidToken:
        return "[ошибка расшифровки]"


bot = Bot(TOKEN)
dp = Dispatcher()

# ================= DB =================

db = sqlite3.connect("bot.db", check_same_thread=False)
cur = db.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT, items TEXT, server TEXT, price INTEGER,
    login TEXT, password TEXT,
    status TEXT DEFAULT 'available'
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT, description TEXT, server TEXT, price INTEGER,
    status TEXT DEFAULT 'available'
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS currency_stock (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server TEXT,
    amount INTEGER,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, username TEXT,
    item_type TEXT DEFAULT 'account',
    account_id INTEGER,
    pay_method TEXT DEFAULT 'card',
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, username TEXT, text TEXT,
    status TEXT DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

for table in ("orders", "tickets"):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    if "created_at" not in cols:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN created_at TIMESTAMP")
            cur.execute(f"UPDATE {table} SET created_at=CURRENT_TIMESTAMP WHERE created_at IS NULL")
        except sqlite3.OperationalError as e:
            log.warning("migration %s: %s", table, e)

cur.execute("PRAGMA table_info(orders)")
cols = [row[1] for row in cur.fetchall()]
if "item_type" not in cols:
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN item_type TEXT DEFAULT 'account'")
        cur.execute("UPDATE orders SET item_type='account' WHERE item_type IS NULL")
    except sqlite3.OperationalError as e:
        log.warning("migration orders.item_type: %s", e)
if "pay_method" not in cols:
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN pay_method TEXT DEFAULT 'card'")
        cur.execute("UPDATE orders SET pay_method='card' WHERE pay_method IS NULL")
    except sqlite3.OperationalError as e:
        log.warning("migration orders.pay_method: %s", e)
for _col, _type in (("amount_rub", "INTEGER"), ("amount_virts", "INTEGER"),
                    ("server", "TEXT"), ("nick", "TEXT")):
    if _col not in cols:
        try:
            cur.execute(f"ALTER TABLE orders ADD COLUMN {_col} {_type}")
        except sqlite3.OperationalError as e:
            log.warning("migration orders.%s: %s", _col, e)

db.commit()

# ================= STATES =================

ticket_state: dict[int, object] = {}
admin_reply_state: dict[int, bool] = {}
admin_ticket_map: dict[int, int] = {}
add_state: dict[int, dict] = {}
add_prop_state: dict[int, dict] = {}
add_stock_state: dict[int, dict] = {}
delete_state: dict[int, bool] = {}
delete_prop_state: dict[int, bool] = {}
delete_stock_state: dict[int, bool] = {}
user_reply_state: dict[int, int] = {}
currency_order_state: dict[int, dict] = {}

# ================= TEXTS =================

ABOUT_TEXT = (
    "🤯 Магазин по покупке аккаунтов и игровой валюты в игре Black Russia\n\n"
    "🌏 Вирты в наличии от 1 до 91 сервера.\n"
    "⚡️ Быстрая выдача игровой валюты.\n"
    "🌈 Бонусы при каждой покупке.\n"
    "💥 Быстрая и отзывчивая поддержка.\n\n"
    "🥳 Наши отзывы — https://t.me/otziviRossSh0p"
)

# ================= HELPERS =================


def user_label(user: types.User) -> str:
    return f"@{user.username}" if user.username else f"id:{user.id}"


def user_greeting(user: types.User) -> str:
    if user.username:
        return f"@{user.username}"
    name = (user.full_name or "").strip()
    return name if name else f"id:{user.id}"


def is_admin_chat(chat_id: int) -> bool:
    return chat_id == GROUP_ID


def rub_to_stars(rub: int) -> int:
    return max(1, math.ceil(rub / RUB_PER_STAR))


def parse_rub(text: str) -> int | None:
    s = text.replace(" ", "").replace(",", ".").strip()
    try:
        v = float(s)
    except ValueError:
        return None
    if v < 0:
        return None
    return int(v)


def parse_virts(text: str) -> int | None:
    s = text.replace(" ", "").replace(",", ".").strip()
    if not s:
        return None
    if s.count(".") >= 2:
        s2 = s.replace(".", "")
        if not s2.isdigit():
            return None
        return int(s2)
    if "." in s:
        left, right = s.split(".")
        if not left.isdigit() or not right.isdigit():
            return None
        if len(right) == 3 and left != "0":
            return int(left + right)
        try:
            return int(float(s) * 1_000_000)
        except ValueError:
            return None
    if not s.isdigit():
        return None
    n = int(s)
    if n <= 1000:
        return n * 1_000_000
    return n


def fmt_virts(n: int) -> str:
    return f"{n:,}".replace(",", ".")


# ================= KEYBOARDS =================

def user_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Магазин аккаунтов", callback_data="shop")],
        [InlineKeyboardButton(text="🏘 Магазин имущества", callback_data="shop_prop")],
        [InlineKeyboardButton(text="💱 Наличие валюты", callback_data="stock")],
        [InlineKeyboardButton(text="💼 Продать аккаунт", callback_data="sell")],
        [InlineKeyboardButton(text="💰 Купить валюту", callback_data="buy_money")],
        [InlineKeyboardButton(text="💸 Продать валюту", callback_data="sell_money")],
        [InlineKeyboardButton(text="🏠 Продать имущество", callback_data="property")],
        [InlineKeyboardButton(text="⭐ Отзывы", callback_data="review")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="about")],
    ])


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎫 Тикеты", callback_data="tickets_list")],
        [InlineKeyboardButton(text="📜 Лог тикетов", callback_data="tickets_log")],
        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_acc")],
        [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data="del_acc")],
        [InlineKeyboardButton(text="➕ Добавить имущество", callback_data="add_prop")],
        [InlineKeyboardButton(text="🗑 Удалить имущество", callback_data="del_prop")],
        [InlineKeyboardButton(text="➕ Добавить валюту в наличии", callback_data="add_stock")],
        [InlineKeyboardButton(text="🗑 Удалить валюту из наличия", callback_data="del_stock")],
    ])


def pay_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить картой", callback_data=f"paid_{order_id}")],
        [InlineKeyboardButton(text="⭐ Оплатить звёздами", callback_data=f"stars_{order_id}")],
        [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cancel_{order_id}")],
    ])


def buy_money_choice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💴 Написать в виртах", callback_data="buy_money_virts")],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_menu")],
    ])


def back_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_menu")],
    ])


def ticket_kb(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✉️ Ответить", callback_data=f"reply_{tid}"),
            InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"close_{tid}"),
        ]
    ])


def user_reply_kb(tid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Ответить", callback_data=f"ureply_{tid}")]
    ])


# ================= START =================

@dp.message(Command("start"))
async def start(m: types.Message):
    greeting = user_greeting(m.from_user)
    await m.answer(
        f"Приветствуем тебя ({greeting}) ты попал на самого лучшего бота "
        f"по покупке игрового имущества на любых серверах ❤️",
        reply_markup=user_kb(),
    )


@dp.message(Command("panel"))
async def panel(m: types.Message):
    if not is_admin_chat(m.chat.id):
        return
    await m.answer("⚙️ Админ панель", reply_markup=admin_kb())


# ================= ABOUT =================

@dp.callback_query(F.data == "about")
async def about(c: types.CallbackQuery):
    await c.answer()
    await c.message.answer(ABOUT_TEXT, reply_markup=back_menu_kb(), disable_web_page_preview=True)


# ================= SHOP: ACCOUNTS =================

@dp.callback_query(F.data == "shop")
async def shop(c: types.CallbackQuery):
    await c.answer()
    cur.execute("SELECT id, level, items, server, price FROM accounts WHERE status='available'")
    rows = cur.fetchall()
    if not rows:
        return await c.message.answer("❌ Нет доступных аккаунтов")
    for a in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Купить за {a[4]}₽", callback_data=f"buy_{a[0]}")]
        ])
        await c.message.answer(
            f"📦 АККАУНТ #{a[0]}\n🎮 {a[1]}\n💰 {a[2]}\n🌍 {a[3]}\n💵 {a[4]}₽",
            reply_markup=kb,
        )


# ================= SHOP: PROPERTIES =================

@dp.callback_query(F.data == "shop_prop")
async def shop_prop(c: types.CallbackQuery):
    await c.answer()
    cur.execute("SELECT id, name, description, server, price FROM properties WHERE status='available'")
    rows = cur.fetchall()
    if not rows:
        return await c.message.answer("❌ Нет доступного имущества")
    for p in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Купить за {p[4]}₽", callback_data=f"buyp_{p[0]}")]
        ])
        await c.message.answer(
            f"🏘 ИМУЩЕСТВО #{p[0]}\n🏷 {p[1]}\n📝 {p[2]}\n🌍 {p[3]}\n💵 {p[4]}₽",
            reply_markup=kb,
        )


# ================= STOCK =================

@dp.callback_query(F.data == "stock")
async def stock(c: types.CallbackQuery):
    await c.answer()
    cur.execute("SELECT id, server, amount, note FROM currency_stock ORDER BY id DESC")
    rows = cur.fetchall()
    if not rows:
        return await c.message.answer(
            "❌ Сейчас валюты в наличии нет.\nВы можете оставить заявку через «💰 Купить валюту»."
        )
    for s in rows:
        sid, server, amount, note = s
        note_line = f"\n📝 {note}" if note else ""
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Купить", callback_data="buy_money")]
        ])
        await c.message.answer(
            f"💱 НАЛИЧИЕ #{sid}\n🌍 Сервер: {server}\n💴 В наличии: {fmt_virts(int(amount))} виртов{note_line}",
            reply_markup=kb,
        )


# ================= BUY: ACCOUNTS =================

@dp.callback_query(F.data.startswith("buy_") & ~F.data.startswith("buy_money") & ~F.data.startswith("buyp_"))
async def buy(c: types.CallbackQuery):
    await c.answer()
    parts = c.data.split("_")
    if len(parts) != 2 or not parts[1].isdigit():
        return
    acc_id = int(parts[1])
    u = c.from_user
    cur.execute("SELECT status FROM accounts WHERE id=?", (acc_id,))
    row = cur.fetchone()
    if not row or row[0] != "available":
        return await c.message.answer("❌ Аккаунт уже недоступен")
    cur.execute("SELECT id FROM orders WHERE user_id=? AND status='pending'", (u.id,))
    if cur.fetchone():
        return await c.message.answer("❌ У вас уже есть активный заказ")
    cur.execute(
        "INSERT INTO orders (user_id, username, item_type, account_id) VALUES (?, ?, 'account', ?)",
        (u.id, user_label(u), acc_id),
    )
    db.commit()
    order_id = cur.lastrowid
    await c.message.answer(
        f"💳 Заказ #{order_id} создан. Выберите способ оплаты:",
        reply_markup=pay_kb(order_id),
    )


# ================= BUY: PROPERTIES =================

@dp.callback_query(F.data.startswith("buyp_"))
async def buy_property(c: types.CallbackQuery):
    await c.answer()
    parts = c.data.split("_")
    if len(parts) != 2 or not parts[1].isdigit():
        return
    prop_id = int(parts[1])
    u = c.from_user
    cur.execute("SELECT status FROM properties WHERE id=?", (prop_id,))
    row = cur.fetchone()
    if not row or row[0] != "available":
        return await c.message.answer("❌ Имущество уже недоступно")
    cur.execute("SELECT id FROM orders WHERE user_id=? AND status='pending'", (u.id,))
    if cur.fetchone():
        return await c.message.answer("❌ У вас уже есть активный заказ")
    cur.execute(
        "INSERT INTO orders (user_id, username, item_type, account_id) VALUES (?, ?, 'property', ?)",
        (u.id, user_label(u), prop_id),
    )
    db.commit()
    order_id = cur.lastrowid
    await c.message.answer(
        f"💳 Заказ #{order_id} (имущество) создан. Выберите способ оплаты:",
        reply_markup=pay_kb(order_id),
    )


# ================= ORDER FLOW =================

def get_order_item(order_id: int):
    cur.execute(
        "SELECT item_type, account_id, amount_rub, amount_virts, server, nick "
        "FROM orders WHERE id=?",
        (order_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    item_type, item_id, amount_rub, amount_virts, server, nick = row
    if item_type == "property":
        cur.execute("SELECT name, price FROM properties WHERE id=?", (item_id,))
        prop = cur.fetchone()
        if not prop:
            return None
        return ("property", item_id, int(prop[1]), f"Имущество: {prop[0]}")
    elif item_type == "currency":
        rub = int(amount_rub or 0)
        virts = int(amount_virts or 0)
        return (
            "currency", order_id, rub,
            f"Валюта: {fmt_virts(virts)} виртов | сервер {server} | ник {nick}",
        )
    else:
        cur.execute("SELECT level, server, price FROM accounts WHERE id=?", (item_id,))
        acc = cur.fetchone()
        if not acc:
            return None
        return ("account", item_id, int(acc[2]), f"Аккаунт: {acc[0]} ({acc[1]})")


def order_details_text(order_id: int) -> str:
    """Подробное описание содержимого заказа для админа."""
    cur.execute(
        "SELECT item_type, account_id, amount_rub, amount_virts, server, nick "
        "FROM orders WHERE id=?",
        (order_id,),
    )
    row = cur.fetchone()
    if not row:
        return ""
    item_type, item_id, amount_rub, amount_virts, server, nick = row
    if item_type == "property":
        cur.execute("SELECT name, server, price FROM properties WHERE id=?", (item_id,))
        prop = cur.fetchone()
        if prop:
            return f"\n🏘 Имущество #{item_id}: {prop[0]} • Сервер {prop[1]} • {prop[2]}₽"
    elif item_type == "currency":
        return (
            f"\n💴 Валюта: {fmt_virts(int(amount_virts or 0))} виртов (~{amount_rub}₽)"
            f"\n🌍 Сервер: {server} • 👤 Ник: {nick}"
        )
    elif item_type == "account":
        cur.execute("SELECT level, items, server, price FROM accounts WHERE id=?", (item_id,))
        acc = cur.fetchone()
        if acc:
            return (
                f"\n📦 Аккаунт #{item_id}: {acc[0]}"
                f"\n💰 Имущество: {acc[1]}"
                f"\n🌍 Сервер: {acc[2]} • 💵 {acc[3]}₽"
            )
    return ""


@dp.callback_query(F.data.startswith("cancel_"))
async def cancel_order(c: types.CallbackQuery):
    await c.answer()
    order_id = int(c.data.split("_")[1])
    cur.execute("SELECT user_id, item_type, account_id, status FROM orders WHERE id=?", (order_id,))
    row = cur.fetchone()
    if not row:
        return await c.message.answer("❌ Заказ не найден")
    user_id, item_type, item_id, status = row
    if user_id != c.from_user.id:
        return await c.message.answer("⛔ Это не ваш заказ")
    if status != "pending":
        return await c.message.answer("❌ Заказ уже нельзя отменить")
    cur.execute("UPDATE orders SET status='cancelled' WHERE id=?", (order_id,))
    if item_type == "property":
        cur.execute("UPDATE properties SET status='available' WHERE id=? AND status!='sold'", (item_id,))
    elif item_type == "account":
        cur.execute("UPDATE accounts SET status='available' WHERE id=? AND status!='sold'", (item_id,))
    db.commit()
    try:
        await c.message.edit_text(f"❌ Заказ #{order_id} отменён")
    except Exception:
        await c.message.answer(f"❌ Заказ #{order_id} отменён")
    try:
        await bot.send_message(GROUP_ID, f"❌ ОТМЕНА ЗАКАЗА\n🆔 #{order_id}\n👤 {user_label(c.from_user)}")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("paid_"))
async def paid(c: types.CallbackQuery):
    await c.answer()
    order_id = int(c.data.split("_")[1])
    cur.execute("UPDATE orders SET status='paid', pay_method='card' WHERE id=?", (order_id,))
    db.commit()
    details = order_details_text(order_id)
    await bot.send_message(
        GROUP_ID,
        f"💰 ПОДТВЕРЖДЕНИЕ ОПЛАТЫ (карта)\n"
        f"🆔 Заказ #{order_id}\n"
        f"👤 {user_label(c.from_user)}"
        f"{details}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить выдачу", callback_data=f"confirm_{order_id}")]
        ]),
    )
    try:
        await c.message.edit_text("⏳ Ожидайте подтверждения администратора")
    except Exception:
        await c.message.answer("⏳ Ожидайте подтверждения администратора")


# ================= STARS PAYMENT =================

@dp.callback_query(F.data.startswith("stars_"))
async def pay_with_stars(c: types.CallbackQuery):
    await c.answer()
    order_id = int(c.data.split("_")[1])
    cur.execute("SELECT user_id, status FROM orders WHERE id=?", (order_id,))
    row = cur.fetchone()
    if not row:
        return await c.message.answer("❌ Заказ не найден")
    if row[0] != c.from_user.id:
        return await c.message.answer("⛔ Это не ваш заказ")
    if row[1] != "pending":
        return await c.message.answer("❌ Заказ уже не ожидает оплаты")

    info = get_order_item(order_id)
    if not info:
        return await c.message.answer("❌ Не удалось найти товар по заказу")
    item_type, item_id, price_rub, title = info
    stars = rub_to_stars(price_rub)

    try:
        await bot.send_invoice(
            chat_id=c.from_user.id,
            title=f"Заказ #{order_id}",
            description=f"{title}\nСумма: {price_rub}₽ ≈ {stars}⭐",
            payload=f"order_{order_id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"Заказ #{order_id}", amount=stars)],
        )
    except Exception as e:
        log.exception("send_invoice stars failed: %s", e)
        await c.message.answer("⚠️ Не удалось создать счёт в звёздах. Попробуйте оплатить картой.")


@dp.pre_checkout_query()
async def pre_checkout(q: types.PreCheckoutQuery):
    try:
        order_id = int(q.invoice_payload.split("_")[1])
        cur.execute("SELECT status FROM orders WHERE id=?", (order_id,))
        row = cur.fetchone()
        if not row or row[0] != "pending":
            return await bot.answer_pre_checkout_query(
                q.id, ok=False, error_message="Заказ уже не активен"
            )
    except Exception:
        return await bot.answer_pre_checkout_query(
            q.id, ok=False, error_message="Некорректный заказ"
        )
    await bot.answer_pre_checkout_query(q.id, ok=True)


# ================= CONFIRM (admin) =================

@dp.callback_query(F.data.startswith("confirm_"))
async def confirm(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    order_id = int(c.data.split("_")[1])
    cur.execute("SELECT user_id, item_type, account_id FROM orders WHERE id=?", (order_id,))
    row = cur.fetchone()
    if not row:
        return await c.message.answer("❌ Заказ не найден")
    user_id, item_type, item_id = row

    if item_type == "property":
        cur.execute("SELECT name, description, server FROM properties WHERE id=?", (item_id,))
        prop = cur.fetchone()
        if not prop:
            return await c.message.answer("❌ Имущество не найдено")
        name, descr, server = prop
        cur.execute("UPDATE orders SET status='done' WHERE id=?", (order_id,))
        cur.execute("UPDATE properties SET status='sold' WHERE id=?", (item_id,))
        db.commit()
        try:
            await bot.send_message(
                user_id,
                f"✅ ИМУЩЕСТВО ВЫДАНО\n\n🏷 {name}\n📝 {descr}\n🌍 {server}\n\n"
                f"📩 Ожидайте передачи в игре, администратор свяжется с вами."
            )
        except Exception as e:
            log.exception("send to user failed: %s", e)
            await c.message.answer(f"⚠️ Не удалось отправить сообщение пользователю {user_id}")
    else:
        cur.execute("SELECT login, password FROM accounts WHERE id=?", (item_id,))
        acc = cur.fetchone()
        if not acc:
            return await c.message.answer("❌ Аккаунт не найден")
        login, password = dec(acc[0]), dec(acc[1])
        cur.execute("UPDATE orders SET status='done' WHERE id=?", (order_id,))
        cur.execute("UPDATE accounts SET status='sold' WHERE id=?", (item_id,))
        db.commit()
        try:
            await bot.send_message(user_id, f"✅ ЗАКАЗ ВЫДАН\n\n👤 Логин: {login}\n🔐 Пароль: {password}")
        except Exception as e:
            log.exception("send to user failed: %s", e)
            await c.message.answer(f"⚠️ Не удалось отправить сообщение пользователю {user_id}")

    try:
        await c.message.edit_text(f"✔ Заказ #{order_id} подтверждён и выдан")
    except Exception:
        await c.message.answer(f"✔ Заказ #{order_id} подтверждён и выдан")


# ================= FORMS =================

@dp.callback_query(F.data == "sell")
async def sell(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "sell_account"
    await c.message.answer("💼 Опишите аккаунт, который хотите продать (уровень, имущество, сервер, цена):")


@dp.callback_query(F.data == "buy_money")
async def buy_money(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "buy_money"
    await c.message.answer(
        "Цена за 1.000.000 — 30₽.\n"
        "✍️ Введите сумму в реальных рублях, на которую хотите купить вирты.\n\n"
        "⚠️ Минимальная сумма покупки — 30 ₽.",
        reply_markup=buy_money_choice_kb(),
    )


@dp.callback_query(F.data == "buy_money_virts")
async def buy_money_virts(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "buy_money_virts"
    await c.message.answer(
        "Цена за 1.000.000 — 30₽.\n"
        "✍️ Введите количество валюты в любом удобном формате:\n\n"
        "500.000 — укажите: 0.5 или 500 000\n"
        "1.000.000 — укажите: 1 или 1 000 000\n\n"
        "⚠️ Минимальная сумма для покупки — 1.000.000 виртов.",
        reply_markup=back_menu_kb(),
    )


@dp.callback_query(F.data == "back_menu")
async def back_menu(c: types.CallbackQuery):
    await c.answer()
    ticket_state.pop(c.from_user.id, None)
    user_reply_state.pop(c.from_user.id, None)
    await c.message.answer("👋 Главное меню", reply_markup=user_kb())


@dp.callback_query(F.data == "sell_money")
async def sell_money(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "sell_money"
    await c.message.answer("💸 Введите сумму для продажи валюты. Мы скупаем вирты по 20 рублей за 1кк вирт")


@dp.callback_query(F.data == "property")
async def property_cb(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "property"
    await c.message.answer("🏠 Опишите имущество, которое хотите продать:")


@dp.callback_query(F.data == "review")
async def review(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "review"
    await c.message.answer("⭐ Напишите ваш отзыв:")


@dp.callback_query(F.data == "support")
async def support(c: types.CallbackQuery):
    await c.answer()
    ticket_state[c.from_user.id] = "support"
    await c.message.answer("✍️ Напишите ваш вопрос, он будет отправлен в поддержку:")


GROUP_FORMS = {
    "sell_account":     ("💼 ПРОДАЖА АККАУНТА", "📌"),
    "buy_money":        ("💰 ЗАКАЗ ВАЛЮТЫ (₽)",  "💵 Сумма:"),
    "buy_money_virts":  ("💴 ЗАКАЗ ВАЛЮТЫ (виртов)", "💵 Кол-во:"),
    "sell_money":       ("💸 ПРОДАЖА ВАЛЮТЫ",   "💵 Сумма:"),
    "property":         ("🏠 ПРОДАЖА ИМУЩЕСТВА", "📌"),
    "review":           ("⭐ ОТЗЫВ",            "📝"),
    "support":          ("🆘 ПОДДЕРЖКА",        "📌"),
}


# ================= ROUTER =================

@dp.message()
async def router(m: types.Message):
    # ===== Успешный платёж звёздами — автовыдача без подтверждения =====
    if m.successful_payment:
        sp = m.successful_payment
        try:
            order_id = int(sp.invoice_payload.split("_")[1])
        except Exception:
            return
        stars_paid = sp.total_amount

        cur.execute("SELECT user_id, item_type, account_id, status FROM orders WHERE id=?", (order_id,))
        order_row = cur.fetchone()
        if not order_row:
            return await m.answer("⚠️ Заказ не найден, свяжитесь с поддержкой.")
        order_user_id, item_type, item_id, order_status = order_row

        if order_status == "done":
            return await m.answer(f"ℹ️ Заказ #{order_id} уже выдан ранее.")

        cur.execute(
            "UPDATE orders SET status='paid', pay_method='stars' WHERE id=?",
            (order_id,),
        )
        db.commit()

        if item_type == "property":
            cur.execute("SELECT name, description, server FROM properties WHERE id=?", (item_id,))
            prop = cur.fetchone()
            if not prop:
                await m.answer("⚠️ Имущество не найдено, свяжитесь с поддержкой.")
                return
            name, descr, server = prop
            cur.execute("UPDATE orders SET status='done' WHERE id=?", (order_id,))
            cur.execute("UPDATE properties SET status='sold' WHERE id=?", (item_id,))
            db.commit()
            await m.answer(
                f"✅ ОПЛАТА ЗВЁЗДАМИ ПОЛУЧЕНА\n"
                f"🆔 Заказ #{order_id} • 💫 {stars_paid} ⭐\n\n"
                f"🏷 {name}\n📝 {descr}\n🌍 {server}\n\n"
                f"📩 Ожидайте передачи в игре, администратор свяжется с вами."
            )
            try:
                await bot.send_message(
                    GROUP_ID,
                    f"⭐ ОПЛАЧЕНО ЗВЁЗДАМИ (имущество, авто-выдача)\n"
                    f"🆔 Заказ #{order_id}\n"
                    f"👤 {user_label(m.from_user)}\n"
                    f"💫 {stars_paid} ⭐\n"
                    f"🏷 {name} ({server})\n\n"
                    f"⚠️ Передайте имущество в игре."
                )
            except Exception as e:
                log.exception("admin notify stars(property) failed: %s", e)
        else:
            cur.execute("SELECT login, password FROM accounts WHERE id=?", (item_id,))
            acc = cur.fetchone()
            if not acc:
                await m.answer("⚠️ Аккаунт не найден, свяжитесь с поддержкой.")
                return
            login, password = dec(acc[0]), dec(acc[1])
            cur.execute("UPDATE orders SET status='done' WHERE id=?", (order_id,))
            cur.execute("UPDATE accounts SET status='sold' WHERE id=?", (item_id,))
            db.commit()
            await m.answer(
                f"✅ ОПЛАТА ЗВЁЗДАМИ ПОЛУЧЕНА — заказ #{order_id} выдан!\n"
                f"💫 {stars_paid} ⭐\n\n"
                f"👤 Логин: {login}\n🔐 Пароль: {password}"
            )
            try:
                await bot.send_message(
                    GROUP_ID,
                    f"⭐ ОПЛАЧЕНО ЗВЁЗДАМИ (аккаунт, авто-выдача)\n"
                    f"🆔 Заказ #{order_id}\n"
                    f"👤 {user_label(m.from_user)}\n"
                    f"💫 {stars_paid} ⭐\n"
                    f"📦 Аккаунт #{item_id}"
                )
            except Exception as e:
                log.exception("admin notify stars(account) failed: %s", e)
        return

    uid = m.from_user.id
    text = m.text or ""

    # ===== Добавление аккаунта =====
    if uid in add_state:
        s = add_state[uid]
        if "level" not in s:
            s["level"] = text
            return await m.answer("💰 Имущество:")
        if "items" not in s:
            s["items"] = text
            return await m.answer("🌍 Сервер:")
        if "server" not in s:
            s["server"] = text
            return await m.answer("💵 Цена (число):")
        if "price" not in s:
            if not text.isdigit():
                return await m.answer("❌ Введите цену числом")
            s["price"] = int(text)
            return await m.answer("👤 Логин:")
        if "login" not in s:
            s["login"] = text
            return await m.answer("🔐 Пароль:")
        s["password"] = text
        cur.execute("""
            INSERT INTO accounts (level, items, server, price, login, password)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (s["level"], s["items"], s["server"], s["price"], enc(s["login"]), enc(s["password"])))
        db.commit()
        add_state.pop(uid, None)
        return await m.answer("✅ Аккаунт добавлен (логин и пароль зашифрованы)")

    # ===== Добавление имущества =====
    if uid in add_prop_state:
        s = add_prop_state[uid]
        if "name" not in s:
            s["name"] = text
            return await m.answer("📝 Описание (характеристики, адрес и т.п.):")
        if "description" not in s:
            s["description"] = text
            return await m.answer("🌍 Сервер:")
        if "server" not in s:
            s["server"] = text
            return await m.answer("💵 Цена (число):")
        if "price" not in s:
            if not text.isdigit():
                return await m.answer("❌ Введите цену числом")
            s["price"] = int(text)
        cur.execute("""
            INSERT INTO properties (name, description, server, price)
            VALUES (?, ?, ?, ?)
        """, (s["name"], s["description"], s["server"], s["price"]))
        db.commit()
        add_prop_state.pop(uid, None)
        return await m.answer("✅ Имущество добавлено")

    # ===== Добавление валюты в наличии =====
    if uid in add_stock_state:
        s = add_stock_state[uid]
        if "server" not in s:
            s["server"] = text
            return await m.answer(
                "💴 Сколько виртов в наличии?\n"
                "Можно: 1, 1.000.000, 1 000 000, 2.5 (где 2.5 = 2.500.000)"
            )
        if "amount" not in s:
            amount = parse_virts(text)
            if amount is None or amount <= 0:
                return await m.answer("❌ Не понял количество. Попробуйте ещё раз.")
            s["amount"] = amount
            return await m.answer("📝 Примечание (или «-» чтобы пропустить):")
        note = text.strip()
        if note == "-":
            note = ""
        cur.execute(
            "INSERT INTO currency_stock (server, amount, note) VALUES (?, ?, ?)",
            (s["server"], s["amount"], note),
        )
        db.commit()
        add_stock_state.pop(uid, None)
        return await m.answer(
            f"✅ Добавлено в наличие: сервер {s['server']}, {fmt_virts(s['amount'])} виртов"
        )

    # ===== Удаление аккаунта =====
    if uid in delete_state:
        if not text.isdigit():
            return await m.answer("❌ ID должен быть числом")
        acc_id = int(text)
        cur.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
        db.commit()
        delete_state.pop(uid, None)
        return await m.answer(f"🗑 Удалён аккаунт #{acc_id}")

    # ===== Удаление имущества =====
    if uid in delete_prop_state:
        if not text.isdigit():
            return await m.answer("❌ ID должен быть числом")
        prop_id = int(text)
        cur.execute("DELETE FROM properties WHERE id=?", (prop_id,))
        db.commit()
        delete_prop_state.pop(uid, None)
        return await m.answer(f"🗑 Удалено имущество #{prop_id}")

    # ===== Удаление валюты из наличия =====
    if uid in delete_stock_state:
        if not text.isdigit():
            return await m.answer("❌ ID должен быть числом")
        sid = int(text)
        cur.execute("DELETE FROM currency_stock WHERE id=?", (sid,))
        db.commit()
        delete_stock_state.pop(uid, None)
        return await m.answer(f"🗑 Удалена позиция наличия #{sid}")

    # ===== Ответ админа/поддержки на тикет =====
    if uid in admin_reply_state:
        admin_reply_state.pop(uid, None)
        tid = admin_ticket_map.pop(uid, None)
        if tid is None:
            return
        cur.execute("SELECT user_id FROM tickets WHERE id=?", (tid,))
        row = cur.fetchone()
        if not row:
            return await m.answer("❌ Тикет не найден")
        try:
            await bot.send_message(
                row[0],
                f"📩 Ответ поддержки (заявка #{tid}):\n\n{text}",
                reply_markup=user_reply_kb(tid),
            )
            await m.answer("✅ Отправлено")
        except Exception as e:
            log.exception("reply failed: %s", e)
            await m.answer("⚠️ Не удалось отправить ответ")
        return

    # ===== Ответ пользователя по своей заявке =====
    if uid in user_reply_state:
        tid = user_reply_state.pop(uid)
        cur.execute("SELECT id, status FROM tickets WHERE id=?", (tid,))
        row = cur.fetchone()
        if not row:
            return await m.answer("❌ Заявка не найдена")
        if row[1] == "closed":
            return await m.answer("🔒 Заявка уже закрыта")
        try:
            await bot.send_message(
                SUPPORT_ID,
                f"💬 ОТВЕТ ПО ЗАЯВКЕ #{tid}\n"
                f"👤 {user_label(m.from_user)}\n"
                f"🆔 {m.from_user.id}\n\n"
                f"{text}",
                reply_markup=ticket_kb(tid),
            )
            await m.answer("✅ Сообщение отправлено в поддержку")
        except Exception as e:
            log.exception("user reply failed: %s", e)
            await m.answer("⚠️ Не удалось отправить сообщение")
        return

    if m.chat.type != "private":
        return
    if not text:
        return

    # ===== Заявки =====
    if uid in ticket_state:
        kind = str(ticket_state[uid])
        body = text

        if kind == "support":
            ticket_state.pop(uid, None)
            cur.execute(
                "INSERT INTO tickets (user_id, username, text) VALUES (?, ?, ?)",
                (uid, user_label(m.from_user), f"[support] {text}"),
            )
            db.commit()
            tid = cur.lastrowid
            try:
                await bot.send_message(
                    SUPPORT_ID,
                    f"🆘 НОВОЕ СООБЩЕНИЕ В ПОДДЕРЖКУ #{tid}\n"
                    f"👤 {user_label(m.from_user)}\n"
                    f"🆔 {m.from_user.id}\n\n"
                    f"{text}",
                    reply_markup=ticket_kb(tid),
                )
            except Exception as e:
                log.exception("support send error: %s", e)
                return await m.answer("❌ Ошибка отправки")
            return await m.answer(f"✅ Заявка #{tid} отправлена в поддержку")

        if kind == "buy_money":
            rub = parse_rub(text)
            if rub is None:
                return await m.answer(
                    "❌ Нужно ввести сумму числом (например: 30, 90, 150).\n"
                    "Минимальная сумма — 30 ₽."
                )
            if rub < 30:
                return await m.answer(
                    f"⚠️ Вы ввели {rub} ₽. Минимальная сумма покупки — 30 ₽."
                )
            virts = int(rub / 30 * 1_000_000)
            body = f"{rub} ₽ (~{fmt_virts(virts)} виртов)"

        elif kind == "buy_money_virts":
            virts = parse_virts(text)
            if virts is None:
                return await m.answer(
                    "❌ Не понял количество. Примеры: 1, 1.000.000, 1 000 000, 2.5.\n"
                    "Минимальная сумма — 1.000.000 виртов."
                )
            if virts < 1_000_000:
                return await m.answer(
                    f"⚠️ Вы указали {fmt_virts(virts)} виртов. Минимум — 1.000.000."
                )
            rub = round(virts / 1_000_000 * 30, 2)
            body = f"{fmt_virts(virts)} виртов (~{rub} ₽)"

        ticket_state.pop(uid, None)
        title, label = GROUP_FORMS.get(kind, ("📩 ЗАЯВКА", "📌"))
        cur.execute(
            "INSERT INTO tickets (user_id, username, text) VALUES (?, ?, ?)",
            (uid, user_label(m.from_user), f"[{kind}] {body}"),
        )
        db.commit()
        tid = cur.lastrowid
        await bot.send_message(
            GROUP_ID,
            f"{title} #{tid}\n👤 {user_label(m.from_user)}\n{label} {body}",
            reply_markup=ticket_kb(tid),
        )
        return await m.answer(f"✅ Заявка #{tid} отправлена")


# ================= TICKETS =================

@dp.callback_query(F.data == "tickets_list")
async def tickets_list(c: types.CallbackQuery):
    await c.answer()
    cur.execute(
        "SELECT id, username, text, status FROM tickets WHERE status='open' ORDER BY id DESC LIMIT 10"
    )
    rows = cur.fetchall()
    if not rows:
        return await c.message.answer("📭 Нет открытых тикетов")
    for tid, username, text, status in rows:
        await c.message.answer(
            f"🎫 #{tid}\n👤 {username}\n📌 {text}\n📊 {status}",
            reply_markup=ticket_kb(tid),
        )


@dp.callback_query(F.data == "tickets_log")
async def tickets_log(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    cur.execute(
        "SELECT id, username, text, status, created_at FROM tickets ORDER BY id DESC LIMIT 30"
    )
    rows = cur.fetchall()
    if not rows:
        return await c.message.answer("📭 История тикетов пуста")
    open_n = sum(1 for r in rows if r[3] == "open")
    closed_n = sum(1 for r in rows if r[3] == "closed")
    lines = [f"📜 ЛОГ ТИКЕТОВ (последние {len(rows)})\n🟢 Открытых: {open_n}   🔒 Закрытых: {closed_n}\n"]
    for tid, username, text, status, created_at in rows:
        icon = "🟢" if status == "open" else "🔒"
        date = (created_at or "")[:16]
        snippet = (text or "")[:80]
        lines.append(f"{icon} #{tid} • {date}\n👤 {username}\n📌 {snippet}\n")
    msg = "\n".join(lines)
    chunk = ""
    for line in msg.split("\n"):
        if len(chunk) + len(line) + 1 > 3800:
            await c.message.answer(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        await c.message.answer(chunk)


# ================= REPLY / CLOSE =================

@dp.callback_query(F.data.startswith("reply_"))
async def reply_start(c: types.CallbackQuery):
    await c.answer()
    tid = int(c.data.split("_")[1])
    admin_reply_state[c.from_user.id] = True
    admin_ticket_map[c.from_user.id] = tid
    await c.message.answer(f"✍️ Введите ответ для тикета #{tid}:")


@dp.callback_query(F.data.startswith("ureply_"))
async def user_reply_start(c: types.CallbackQuery):
    await c.answer()
    tid = int(c.data.split("_")[1])
    user_reply_state[c.from_user.id] = tid
    await c.message.answer(f"✍️ Напишите ваш ответ по заявке #{tid}:")


@dp.callback_query(F.data.startswith("close_"))
async def close_ticket(c: types.CallbackQuery):
    await c.answer()
    tid = int(c.data.split("_")[1])
    cur.execute("SELECT user_id FROM tickets WHERE id=?", (tid,))
    row = cur.fetchone()
    cur.execute("UPDATE tickets SET status='closed' WHERE id=?", (tid,))
    db.commit()
    try:
        await c.message.edit_text(f"🔒 Тикет #{tid} закрыт")
    except Exception:
        await c.message.answer(f"🔒 Тикет #{tid} закрыт")
    if row:
        try:
            await bot.send_message(row[0], f"🔒 Ваша заявка #{tid} закрыта поддержкой.")
        except Exception:
            pass


# ================= ADMIN ACTIONS =================

@dp.callback_query(F.data == "add_acc")
async def add_acc(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    add_state[c.from_user.id] = {}
    await c.message.answer("🎮 Уровень:")


@dp.callback_query(F.data == "del_acc")
async def del_acc(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    delete_state[c.from_user.id] = True
    await c.message.answer("🗑 Введите ID аккаунта:")


@dp.callback_query(F.data == "add_prop")
async def add_prop(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    add_prop_state[c.from_user.id] = {}
    await c.message.answer("🏷 Название имущества:")


@dp.callback_query(F.data == "del_prop")
async def del_prop(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    delete_prop_state[c.from_user.id] = True
    await c.message.answer("🗑 Введите ID имущества:")


@dp.callback_query(F.data == "add_stock")
async def add_stock(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    add_stock_state[c.from_user.id] = {}
    await c.message.answer("🌍 Номер сервера (например: 5):")


@dp.callback_query(F.data == "del_stock")
async def del_stock(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    delete_stock_state[c.from_user.id] = True
    await c.message.answer("🗑 Введите ID позиции наличия:")


# ================= RUN =================

async def main():
    try:
        await dp.start_polling(bot)
    finally:
        db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
