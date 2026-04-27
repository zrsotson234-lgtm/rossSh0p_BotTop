import asyncio
import signal
import sqlite3
import os
import math
import logging
import base64
import hashlib

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from cryptography.fernet import Fernet, InvalidToken

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bot")
SUPPORT_ID = int(os.getenv("SUPPORT_ID"))

TOKEN = os.getenv("TOKEN")
GROUP_ID_RAW = os.getenv("GROUP_ID")

# Курс рубля к звёздам: сколько рублей в одной звезде.
# Рыночный курс: 1.2 руб = 1 звезда (1.2к к 1).
# Меняйте через переменную окружения RUB_PER_STAR.
try:
    RUB_PER_STAR = float(os.getenv("RUB_PER_STAR", "1.2"))
    if RUB_PER_STAR <= 0:
        raise ValueError
except ValueError:
    RUB_PER_STAR = 1.2

if not TOKEN:
    raise SystemExit("❌ Не задана переменная окружения TOKEN.")
if not GROUP_ID_RAW:
    raise SystemExit("❌ Не задана переменная окружения GROUP_ID.")
try:
    GROUP_ID = int(GROUP_ID_RAW)
except ValueError:
    raise SystemExit("❌ GROUP_ID должен быть числом, например -1001234567890")

# ================= ENCRYPTION =================

ENC_KEY_RAW = os.getenv("ENCRYPTION_KEY")
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

DB_PATH = os.getenv("DB_PATH", "bot.db")
db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False)
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

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    blocked INTEGER DEFAULT 0
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
# Состояние для ответа администратора покупателю по конкретному заказу
admin_order_msg_state: dict[int, int] = {}  # admin_id -> order_id
add_state: dict[int, dict] = {}
add_prop_state: dict[int, dict] = {}
add_stock_state: dict[int, dict] = {}
delete_state: dict[int, bool] = {}
delete_prop_state: dict[int, bool] = {}
delete_stock_state: dict[int, bool] = {}
user_reply_state: dict[int, int] = {}
currency_order_state: dict[int, dict] = {}
broadcast_state: set[int] = set()
search_acc_state: dict[int, bool] = {}
search_prop_state: dict[int, bool] = {}
# Защита от дублей: хранит payload уже обработанных успешных платежей
processed_payments: set[str] = set()

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

def start_kb() -> ReplyKeyboardMarkup:
    """Постоянная кнопка внизу экрана в личном чате."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Главное меню")]],
        resize_keyboard=True,
        is_persistent=True,
    )


def user_kb() -> InlineKeyboardMarkup:    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Магазин аккаунтов", callback_data="shop"),
         InlineKeyboardButton(text="🔍 Поиск аккаунта", callback_data="search_acc")],
        [InlineKeyboardButton(text="🏘 Магазин имущества", callback_data="shop_prop"),
         InlineKeyboardButton(text="🔍 Поиск имущества", callback_data="search_prop")],
        [InlineKeyboardButton(text="💱 Наличие валюты", callback_data="stock")],
        [InlineKeyboardButton(text="💼 Продать аккаунт", callback_data="sell"),
         InlineKeyboardButton(text="🏠 Продать имущество", callback_data="property")],
        [InlineKeyboardButton(text="💰 Купить валюту", callback_data="buy_money"),
         InlineKeyboardButton(text="💸 Продать валюту", callback_data="sell_money")],
        [InlineKeyboardButton(text="⭐ Отзывы", callback_data="review"),
         InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="about")],
    ])


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🎫 Тикеты", callback_data="tickets_list"),
         InlineKeyboardButton(text="📜 Лог тикетов", callback_data="tickets_log")],
        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_acc"),
         InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data="del_acc")],
        [InlineKeyboardButton(text="➕ Добавить имущество", callback_data="add_prop"),
         InlineKeyboardButton(text="🗑 Удалить имущество", callback_data="del_prop")],
        [InlineKeyboardButton(text="➕ Добавить валюту в наличии", callback_data="add_stock"),
         InlineKeyboardButton(text="🗑 Удалить из наличия", callback_data="del_stock")],
        [InlineKeyboardButton(text="📢 Рассылка всем", callback_data="broadcast")],
    ])


def pay_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить картой", callback_data=f"paid_{order_id}")],
        [InlineKeyboardButton(text="⭐ Оплатить звёздами", callback_data=f"stars_{order_id}")],
        [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cancel_{order_id}")],
    ])


def cancel_order_kb(order_id: int) -> InlineKeyboardMarkup:
    """Кнопка отмены для сообщений с уже созданным заказом."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cancel_{order_id}")],
        [InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_menu")],
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
    try:
        cur.execute(
            "INSERT INTO users (user_id, username) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "username=excluded.username, last_seen=CURRENT_TIMESTAMP, blocked=0",
            (m.from_user.id, user_label(m.from_user)),
        )
        db.commit()
    except Exception as e:
        log.warning("save user on /start failed: %s", e)

    greeting = user_greeting(m.from_user)
    # Сначала показываем постоянную кнопку внизу
    await m.answer(
        f"Приветствуем тебя ({greeting}) ты попал на самого лучшего бота "
        f"по покупке игрового имущества на любых серверах ❤️",
        reply_markup=start_kb(),
    )
    # Затем inline-меню
    await m.answer("👇 Выберите действие:", reply_markup=user_kb())


@dp.message(Command("panel"))
async def panel(m: types.Message):
    if not is_admin_chat(m.chat.id):
        return
    await m.answer("⚙️ Админ панель", reply_markup=admin_kb())


@dp.callback_query(F.data == "broadcast")
async def broadcast_start(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    cur.execute("SELECT COUNT(*) FROM users WHERE blocked=0")
    total = cur.fetchone()[0]
    broadcast_state.add(c.from_user.id)
    await c.message.answer(
        f"📢 РАССЫЛКА\n\n"
        f"Получателей: {total} чел.\n\n"
        f"Отправьте мне следующим сообщением текст рассылки.\n"
        f"Чтобы отменить — напишите /cancel.",
    )


async def _do_broadcast(text: str, admin_chat_id: int, admin_user_id: int):
    cur.execute("SELECT user_id FROM users WHERE blocked=0")
    user_ids = [row[0] for row in cur.fetchall()]
    total = len(user_ids)
    sent, failed, blocked = 0, 0, 0

    progress_msg = await bot.send_message(
        admin_chat_id, f"📤 Отправка началась... 0/{total}"
    )

    for i, uid in enumerate(user_ids, 1):
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception as e:
            err = str(e).lower()
            if "blocked" in err or "deactivated" in err or "chat not found" in err:
                blocked += 1
                try:
                    cur.execute("UPDATE users SET blocked=1 WHERE user_id=?", (uid,))
                    db.commit()
                except Exception:
                    pass
            else:
                failed += 1
                log.warning("broadcast to %s failed: %s", uid, e)

        # Telegram лимит ~30 msg/сек, держим запас
        await asyncio.sleep(0.05)

        # обновлять прогресс каждые 25 отправок
        if i % 25 == 0 or i == total:
            try:
                await progress_msg.edit_text(
                    f"📤 Отправка... {i}/{total}\n"
                    f"✅ {sent}  ⛔ заблокировали: {blocked}  ⚠️ ошибок: {failed}"
                )
            except Exception:
                pass

    await bot.send_message(
        admin_chat_id,
        f"✅ РАССЫЛКА ЗАВЕРШЕНА\n\n"
        f"📊 Всего получателей: {total}\n"
        f"✅ Доставлено: {sent}\n"
        f"⛔ Заблокировали бота: {blocked}\n"
        f"⚠️ Прочих ошибок: {failed}",
    )


# ================= ABOUT =================

@dp.callback_query(F.data == "about")
async def about(c: types.CallbackQuery):
    await c.answer()
    await c.message.answer(ABOUT_TEXT, reply_markup=back_menu_kb(), disable_web_page_preview=True)


# ================= ADMIN STATS =================

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")

    # Пользователи
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE blocked=0")
    active_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE date(first_seen) = date('now')")
    new_today = cur.fetchone()[0]

    # Заказы
    cur.execute("SELECT COUNT(*) FROM orders WHERE status='done'")
    done_orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM orders WHERE status='pending'")
    pending_orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM orders WHERE status='cancelled'")
    cancelled_orders = cur.fetchone()[0]

    # Доходы по типам
    cur.execute("SELECT COALESCE(SUM(price),0) FROM accounts WHERE status='sold'")
    income_accounts = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(price),0) FROM properties WHERE status='sold'")
    income_props = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(amount_rub),0) FROM orders WHERE status='done' AND item_type='currency'")
    income_currency = cur.fetchone()[0]
    total_income = income_accounts + income_props + income_currency

    # Способы оплаты
    cur.execute("SELECT COUNT(*) FROM orders WHERE status='done' AND pay_method='stars'")
    paid_stars = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM orders WHERE status='done' AND pay_method='card'")
    paid_card = cur.fetchone()[0]

    # Тикеты
    cur.execute("SELECT COUNT(*) FROM tickets WHERE status='open'")
    open_tickets = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tickets")
    total_tickets = cur.fetchone()[0]

    # Товары в наличии
    cur.execute("SELECT COUNT(*) FROM accounts WHERE status='available'")
    avail_accs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM properties WHERE status='available'")
    avail_props = cur.fetchone()[0]

    # Заказы за сегодня
    cur.execute("SELECT COUNT(*) FROM orders WHERE status='done' AND date(created_at)=date('now')")
    done_today = cur.fetchone()[0]
    cur.execute(
        "SELECT COALESCE(SUM(price),0) FROM accounts "
        "JOIN orders ON orders.account_id=accounts.id AND orders.item_type='account' "
        "WHERE orders.status='done' AND date(orders.created_at)=date('now')"
    )
    income_today = cur.fetchone()[0] or 0

    text = (
        "📊 СТАТИСТИКА МАГАЗИНА\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👥 ПОЛЬЗОВАТЕЛИ\n"
        f"  Всего: {total_users} | Активных: {active_users}\n"
        f"  Новых сегодня: {new_today}\n\n"
        "📦 ТОВАРЫ В НАЛИЧИИ\n"
        f"  Аккаунты: {avail_accs} шт.\n"
        f"  Имущество: {avail_props} шт.\n\n"
        "🛒 ЗАКАЗЫ\n"
        f"  Выполнено: {done_orders} | Ожидают: {pending_orders} | Отменено: {cancelled_orders}\n"
        f"  Выполнено сегодня: {done_today}\n\n"
        "💰 ДОХОДЫ\n"
        f"  Аккаунты: {income_accounts:,}₽\n"
        f"  Имущество: {income_props:,}₽\n"
        f"  Валюта: {income_currency:,}₽\n"
        f"  ИТОГО: {total_income:,}₽\n"
        f"  Сегодня: ~{income_today:,}₽\n\n"
        "💳 СПОСОБЫ ОПЛАТЫ\n"
        f"  Картой: {paid_card} | Звёздами: {paid_stars}\n\n"
        "🎫 ТИКЕТЫ\n"
        f"  Открытых: {open_tickets} | Всего: {total_tickets}\n"
    )
    await c.message.answer(text)


# ================= SEARCH =================

@dp.callback_query(F.data == "search_acc")
async def search_acc_start(c: types.CallbackQuery):
    await c.answer()
    search_acc_state[c.from_user.id] = True
    await c.message.answer(
        "🔍 Поиск аккаунтов\n\n"
        "Введите ключевое слово для поиска по уровню, имуществу или серверу.\n"
        "Или введите диапазон цены: например «1000-5000»\n\n"
        "Примеры: «200 lvl», «дом», «сервер 5», «1000-3000»",
        reply_markup=back_menu_kb(),
    )


@dp.callback_query(F.data == "search_prop")
async def search_prop_start(c: types.CallbackQuery):
    await c.answer()
    search_prop_state[c.from_user.id] = True
    await c.message.answer(
        "🔍 Поиск имущества\n\n"
        "Введите ключевое слово по названию, описанию или серверу.\n"
        "Или введите диапазон цены: например «5000-20000»",
        reply_markup=back_menu_kb(),
    )


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
    cur.execute("SELECT level, items, server, price FROM accounts WHERE id=?", (acc_id,))
    acc_info = cur.fetchone()
    await c.message.answer(
        f"🛒 ЗАКАЗ #{order_id} СОЗДАН\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Аккаунт #{acc_id}\n"
        f"🎮 Уровень: {acc_info[0]}\n"
        f"💰 Имущество: {acc_info[1]}\n"
        f"🌍 Сервер: {acc_info[2]}\n"
        f"💵 Цена: {acc_info[3]}₽\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Выберите способ оплаты или отмените заказ:",
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
    cur.execute("SELECT name, description, server, price FROM properties WHERE id=?", (prop_id,))
    prop_info = cur.fetchone()
    await c.message.answer(
        f"🛒 ЗАКАЗ #{order_id} СОЗДАН\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏘 Имущество #{prop_id}\n"
        f"🏷 Название: {prop_info[0]}\n"
        f"📝 Описание: {prop_info[1]}\n"
        f"🌍 Сервер: {prop_info[2]}\n"
        f"💵 Цена: {prop_info[3]}₽\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Выберите способ оплаты или отмените заказ:",
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
        return await c.message.answer("❌ Заказ уже нельзя отменить (он уже оплачен или выдан)")
    cur.execute("UPDATE orders SET status='cancelled' WHERE id=?", (order_id,))
    if item_type == "property":
        cur.execute("UPDATE properties SET status='available' WHERE id=? AND status!='sold'", (item_id,))
    elif item_type == "account":
        cur.execute("UPDATE accounts SET status='available' WHERE id=? AND status!='sold'", (item_id,))
    db.commit()

    type_label = {"account": "📦 Аккаунт", "property": "🏘 Имущество", "currency": "💴 Валюта"}.get(item_type, "Товар")
    cancel_text = (
        f"❌ ЗАКАЗ #{order_id} ОТМЕНЁН\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{type_label} возвращён в каталог.\n\n"
        f"Вы можете выбрать другой товар в главном меню."
    )
    try:
        await c.message.edit_text(cancel_text, reply_markup=back_menu_kb())
    except Exception:
        await c.message.answer(cancel_text, reply_markup=back_menu_kb())
    try:
        await bot.send_message(
            GROUP_ID,
            f"❌ ОТМЕНА ЗАКАЗА\n"
            f"🆔 #{order_id} • {type_label}\n"
            f"👤 {user_label(c.from_user)} (id: {c.from_user.id})"
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("paid_"))
async def paid(c: types.CallbackQuery):
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
    cur.execute("UPDATE orders SET status='paid', pay_method='card' WHERE id=?", (order_id,))
    db.commit()
    details = order_details_text(order_id)
    stars_hint = rub_to_stars(0)  # просто для инфо
    await bot.send_message(
        GROUP_ID,
        f"💳 ОПЛАТА КАРТОЙ — требует подтверждения\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Заказ #{order_id}\n"
        f"👤 {user_label(c.from_user)} (id: {c.from_user.id})"
        f"{details}\n\n"
        f"⚠️ Проверьте поступление средств и подтвердите выдачу.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить выдачу", callback_data=f"confirm_{order_id}")],
            [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"admin_cancel_{order_id}")],
            [InlineKeyboardButton(text="✉️ Написать покупателю", callback_data=f"admin_msg_{order_id}")],
        ]),
    )
    try:
        await c.message.edit_text(
            "⏳ Заявка на оплату картой отправлена!\n\n"
            "Администратор проверит платёж и выдаст товар.\n"
            "Обычно это занимает до 15 минут."
        )
    except Exception:
        await c.message.answer(
            "⏳ Заявка на оплату картой отправлена!\n\n"
            "Администратор проверит платёж и выдаст товар."
        )


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
            description=f"{title}\n💰 Сумма: {price_rub}₽ ≈ {stars}⭐\n📊 Курс: {RUB_PER_STAR}₽ = 1⭐",
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
                f"🎉 ЗАКАЗ ВЫПОЛНЕН!\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏘 {name}\n"
                f"📝 {descr}\n"
                f"🌍 Сервер: {server}\n\n"
                f"📩 Администратор свяжется с вами для передачи имущества в игре.\n"
                f"⭐ Спасибо за покупку! Оставьте отзыв — нам важно ваше мнение."
            )
        except Exception as e:
            log.exception("send to user failed: %s", e)
            await c.message.answer(f"⚠️ Не удалось отправить сообщение пользователю {user_id}")
    elif item_type == "currency":
        cur.execute(
            "SELECT amount_rub, amount_virts, server, nick FROM orders WHERE id=?",
            (order_id,),
        )
        cdata = cur.fetchone()
        if not cdata:
            return await c.message.answer("❌ Данные о валюте не найдены")
        amount_rub, amount_virts, server, nick = cdata
        cur.execute("UPDATE orders SET status='done' WHERE id=?", (order_id,))
        db.commit()
        try:
            await bot.send_message(
                user_id,
                f"🎉 ВАЛЮТА ВЫДАНА!\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💴 {fmt_virts(int(amount_virts or 0))} виртов (~{amount_rub}₽)\n"
                f"🌍 Сервер: {server}\n"
                f"👤 Ник: {nick}\n\n"
                f"📩 Администратор свяжется с вами для передачи в игре.\n"
                f"⭐ Спасибо за покупку! Оставьте отзыв — нам важно ваше мнение."
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
            await bot.send_message(
                user_id,
                f"🎉 АККАУНТ ВЫДАН!\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 Логин: `{login}`\n"
                f"🔐 Пароль: `{password}`\n\n"
                f"⚠️ Сразу смените пароль после входа!\n"
                f"⭐ Спасибо за покупку! Оставьте отзыв — нам важно ваше мнение.",
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("send to user failed: %s", e)
            await c.message.answer(f"⚠️ Не удалось отправить сообщение пользователю {user_id}")

    try:
        await c.message.edit_text(f"✔ Заказ #{order_id} подтверждён и выдан")
    except Exception:
        await c.message.answer(f"✔ Заказ #{order_id} подтверждён и выдан")


# ================= ADMIN: ОТМЕНА ЗАКАЗА =================

@dp.callback_query(F.data.startswith("admin_cancel_"))
async def admin_cancel_order(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    order_id = int(c.data.split("_")[2])
    cur.execute("SELECT user_id, item_type, account_id, status FROM orders WHERE id=?", (order_id,))
    row = cur.fetchone()
    if not row:
        return await c.message.answer("❌ Заказ не найден")
    user_id, item_type, item_id, status = row
    if status in ("done", "cancelled"):
        return await c.message.answer(f"❌ Заказ #{order_id} уже {'выдан' if status == 'done' else 'отменён'}")
    # Возвращаем товар в каталог
    cur.execute("UPDATE orders SET status='cancelled' WHERE id=?", (order_id,))
    if item_type == "property":
        cur.execute("UPDATE properties SET status='available' WHERE id=? AND status!='sold'", (item_id,))
    elif item_type == "account":
        cur.execute("UPDATE accounts SET status='available' WHERE id=? AND status!='sold'", (item_id,))
    db.commit()
    type_label = {"account": "📦 Аккаунт", "property": "🏘 Имущество", "currency": "💴 Валюта"}.get(item_type, "Товар")
    # Уведомляем покупателя
    try:
        await bot.send_message(
            user_id,
            f"❌ ЗАКАЗ #{order_id} ОТМЕНЁН АДМИНИСТРАТОРОМ\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{type_label} возвращён в каталог.\n\n"
            f"Если вы уже отправили деньги — напишите в поддержку для решения вопроса.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🆘 Написать в поддержку", callback_data="support")]
            ]),
        )
    except Exception as e:
        log.warning("Не удалось уведомить покупателя об отмене: %s", e)
    try:
        await c.message.edit_text(
            f"❌ Заказ #{order_id} отменён администратором @{c.from_user.username or c.from_user.id}\n"
            f"{type_label} возвращён в каталог."
        )
    except Exception:
        await c.message.answer(f"❌ Заказ #{order_id} отменён, {type_label.lower()} возвращён в каталог.")


# ================= ADMIN: СООБЩЕНИЕ ПОКУПАТЕЛЮ =================

@dp.callback_query(F.data.startswith("admin_msg_"))
async def admin_msg_start(c: types.CallbackQuery):
    await c.answer()
    if not is_admin_chat(c.message.chat.id):
        return await c.message.answer("⛔ Только в админ-чате")
    order_id = int(c.data.split("_")[2])
    cur.execute("SELECT user_id, status FROM orders WHERE id=?", (order_id,))
    row = cur.fetchone()
    if not row:
        return await c.message.answer("❌ Заказ не найден")
    admin_order_msg_state[c.from_user.id] = order_id
    await c.message.answer(
        f"✉️ Введите сообщение для покупателя по заказу #{order_id}:\n"
        f"(оно будет отправлено напрямую в личку)\n\n"
        f"Для отмены напишите /cancel"
    )


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
    currency_order_state.pop(c.from_user.id, None)
    search_acc_state.pop(c.from_user.id, None)
    search_prop_state.pop(c.from_user.id, None)
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

@dp.message(F.text == "🚀 Главное меню")
async def btn_main_menu(m: types.Message):
    """Обработчик постоянной кнопки «🚀 Главное меню» внизу экрана."""
    if m.chat.type != "private":
        return
    uid = m.from_user.id
    # Сбрасываем все активные состояния
    ticket_state.pop(uid, None)
    currency_order_state.pop(uid, None)
    search_acc_state.pop(uid, None)
    search_prop_state.pop(uid, None)
    admin_order_msg_state.pop(uid, None)
    await m.answer("👇 Выберите действие:", reply_markup=user_kb())


@dp.message(Command("cancel"))
async def cancel_any(m: types.Message):
    uid = m.from_user.id
    cleared = []
    if uid in broadcast_state:
        broadcast_state.discard(uid)
        cleared.append("рассылка")
    if uid in ticket_state:
        ticket_state.pop(uid, None)
        cleared.append("заявка")
    if uid in currency_order_state:
        currency_order_state.pop(uid, None)
        cleared.append("заказ валюты")
    if uid in search_acc_state:
        search_acc_state.pop(uid, None)
        cleared.append("поиск аккаунта")
    if uid in search_prop_state:
        search_prop_state.pop(uid, None)
        cleared.append("поиск имущества")
    if uid in admin_order_msg_state:
        admin_order_msg_state.pop(uid, None)
        cleared.append("сообщение покупателю")
    if cleared:
        await m.answer(f"❌ Отменено: {', '.join(cleared)}")
    else:
        await m.answer("Нечего отменять.")


@dp.message()
async def router(m: types.Message):
    # ===== РАССЫЛКА: админ ввёл текст после нажатия "📢 Рассылка" =====
    if (
        m.from_user
        and m.from_user.id in broadcast_state
        and is_admin_chat(m.chat.id)
        and m.text
        and not m.text.startswith("/")
    ):
        broadcast_state.discard(m.from_user.id)
        text = m.text
        await m.answer("🚀 Запускаю рассылку...")
        asyncio.create_task(_do_broadcast(text, m.chat.id, m.from_user.id))
        return

    # ===== Успешный платёж звёздами — автовыдача без подтверждения =====
    if m.successful_payment:
        sp = m.successful_payment
        try:
            order_id = int(sp.invoice_payload.split("_")[1])
        except Exception:
            return
        # Защита от дублей
        dedup_key = f"{m.from_user.id}:{sp.invoice_payload}:{sp.telegram_payment_charge_id}"
        if dedup_key in processed_payments:
            log.warning("Duplicate payment ignored: %s", dedup_key)
            return
        processed_payments.add(dedup_key)
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
                    f"💫 {stars_paid} ⭐"
                    f"{order_details_text(order_id)}\n\n"
                    f"⚠️ Передайте имущество в игре."
                )
            except Exception as e:
                log.exception("admin notify stars(property) failed: %s", e)
        elif item_type == "currency":
            cur.execute(
                "SELECT amount_rub, amount_virts, server, nick FROM orders WHERE id=?",
                (order_id,),
            )
            cdata = cur.fetchone()
            if not cdata:
                await m.answer("⚠️ Данные о валюте не найдены, свяжитесь с поддержкой.")
                return
            amount_rub, amount_virts, server, nick = cdata
            cur.execute("UPDATE orders SET status='done' WHERE id=?", (order_id,))
            db.commit()
            await m.answer(
                f"✅ ОПЛАТА ЗВЁЗДАМИ ПОЛУЧЕНА\n"
                f"🆔 Заказ #{order_id} • 💫 {stars_paid} ⭐\n\n"
                f"💴 {fmt_virts(int(amount_virts or 0))} виртов (~{amount_rub}₽)\n"
                f"🌍 Сервер: {server} • 👤 Ник: {nick}\n\n"
                f"📩 Ожидайте передачи валюты в игре, администратор свяжется с вами."
            )
            try:
                await bot.send_message(
                    GROUP_ID,
                    f"⭐ ОПЛАЧЕНО ЗВЁЗДАМИ (валюта, авто-выдача)\n"
                    f"🆔 Заказ #{order_id}\n"
                    f"👤 {user_label(m.from_user)}\n"
                    f"💫 {stars_paid} ⭐"
                    f"{order_details_text(order_id)}\n\n"
                    f"⚠️ Передайте валюту в игре."
                )
            except Exception as e:
                log.exception("admin notify stars(currency) failed: %s", e)
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
                f"🎉 АККАУНТ ВЫДАН — оплата звёздами!\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💫 Заказ #{order_id} • {stars_paid} ⭐\n\n"
                f"👤 Логин: `{login}`\n"
                f"🔐 Пароль: `{password}`\n\n"
                f"⚠️ Сразу смените пароль после входа!\n"
                f"⭐ Спасибо за покупку! Оставьте отзыв.",
                parse_mode="Markdown",
            )
            try:
                await bot.send_message(
                    GROUP_ID,
                    f"⭐ ОПЛАЧЕНО ЗВЁЗДАМИ (аккаунт, авто-выдача)\n"
                    f"🆔 Заказ #{order_id}\n"
                    f"👤 {user_label(m.from_user)}\n"
                    f"💫 {stars_paid} ⭐"
                    f"{order_details_text(order_id)}"
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

    # ===== Сообщение администратора покупателю по заказу =====
    if uid in admin_order_msg_state:
        order_id = admin_order_msg_state.pop(uid)
        cur.execute("SELECT user_id, item_type FROM orders WHERE id=?", (order_id,))
        row = cur.fetchone()
        if not row:
            return await m.answer("❌ Заказ не найден")
        buyer_id = row[0]
        try:
            await bot.send_message(
                buyer_id,
                f"📩 Сообщение от администратора по заказу #{order_id}:\n\n"
                f"{text}\n\n"
                f"Для ответа напишите в поддержку:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🆘 Написать в поддержку", callback_data="support")]
                ]),
            )
            await m.answer(f"✅ Сообщение отправлено покупателю (заказ #{order_id})")
        except Exception as e:
            log.exception("admin_order_msg failed: %s", e)
            await m.answer(f"⚠️ Не удалось отправить сообщение покупателю (id: {buyer_id})")
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

    # ===== Поиск аккаунтов =====
    if uid in search_acc_state:
        search_acc_state.pop(uid, None)
        query = text.strip()
        # Проверяем диапазон цен
        price_match = None
        if "-" in query:
            parts = query.split("-", 1)
            try:
                price_min, price_max = int(parts[0].strip()), int(parts[1].strip())
                price_match = (price_min, price_max)
            except ValueError:
                pass
        if price_match:
            pmin, pmax = price_match
            cur.execute(
                "SELECT id, level, items, server, price FROM accounts "
                "WHERE status='available' AND price BETWEEN ? AND ? ORDER BY price",
                (pmin, pmax),
            )
        else:
            like = f"%{query}%"
            cur.execute(
                "SELECT id, level, items, server, price FROM accounts "
                "WHERE status='available' AND (level LIKE ? OR items LIKE ? OR server LIKE ?)"
                " ORDER BY price",
                (like, like, like),
            )
        rows = cur.fetchall()
        if not rows:
            return await m.answer("🔍 Ничего не найдено по вашему запросу.", reply_markup=user_kb())
        await m.answer(f"🔍 Найдено аккаунтов: {len(rows)}")
        for a in rows[:10]:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"💳 Купить за {a[4]}₽", callback_data=f"buy_{a[0]}")]
            ])
            await m.answer(
                f"📦 АККАУНТ #{a[0]}\n🎮 {a[1]}\n💰 {a[2]}\n🌍 {a[3]}\n💵 {a[4]}₽",
                reply_markup=kb,
            )
        return

    # ===== Поиск имущества =====
    if uid in search_prop_state:
        search_prop_state.pop(uid, None)
        query = text.strip()
        price_match = None
        if "-" in query:
            parts = query.split("-", 1)
            try:
                price_min, price_max = int(parts[0].strip()), int(parts[1].strip())
                price_match = (price_min, price_max)
            except ValueError:
                pass
        if price_match:
            pmin, pmax = price_match
            cur.execute(
                "SELECT id, name, description, server, price FROM properties "
                "WHERE status='available' AND price BETWEEN ? AND ? ORDER BY price",
                (pmin, pmax),
            )
        else:
            like = f"%{query}%"
            cur.execute(
                "SELECT id, name, description, server, price FROM properties "
                "WHERE status='available' AND (name LIKE ? OR description LIKE ? OR server LIKE ?)"
                " ORDER BY price",
                (like, like, like),
            )
        rows = cur.fetchall()
        if not rows:
            return await m.answer("🔍 Ничего не найдено по вашему запросу.", reply_markup=user_kb())
        await m.answer(f"🔍 Найдено имущества: {len(rows)}")
        for p in rows[:10]:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"💳 Купить за {p[4]}₽", callback_data=f"buyp_{p[0]}")]
            ])
            await m.answer(
                f"🏘 ИМУЩЕСТВО #{p[0]}\n🏷 {p[1]}\n📝 {p[2]}\n🌍 {p[3]}\n💵 {p[4]}₽",
                reply_markup=kb,
            )
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
            currency_order_state[uid] = {"rub": int(rub), "virts": virts}
            ticket_state[uid] = "buy_money_server"
            return await m.answer(
                f"💴 Вы покупаете: {fmt_virts(virts)} виртов (~{rub}₽)\n\n"
                f"📍 Введите номер сервера и ник в игре через запятую\n"
                f"(например: 5, MyNickName)",
                reply_markup=back_menu_kb(),
            )

        if kind == "buy_money_virts":
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
            rub = max(1, math.ceil(virts / 1_000_000 * 30))
            currency_order_state[uid] = {"rub": int(rub), "virts": virts}
            ticket_state[uid] = "buy_money_server"
            return await m.answer(
                f"💴 Вы покупаете: {fmt_virts(virts)} виртов (~{rub}₽)\n\n"
                f"📍 Введите номер сервера и ник в игре через запятую\n"
                f"(например: 5, MyNickName)",
                reply_markup=back_menu_kb(),
            )

        if kind == "buy_money_server":
            parts = [p.strip() for p in text.split(",", 1)]
            if len(parts) != 2 or not parts[0] or not parts[1]:
                return await m.answer(
                    "❌ Формат: сервер, ник (например: 5, MyNick)"
                )
            server, nick = parts
            data = currency_order_state.pop(uid, None)
            if not data:
                ticket_state.pop(uid, None)
                return await m.answer(
                    "⚠️ Сессия истекла. Начните заново через «💰 Купить валюту»."
                )
            cur.execute(
                "SELECT id FROM orders WHERE user_id=? AND status='pending'", (uid,)
            )
            if cur.fetchone():
                ticket_state.pop(uid, None)
                return await m.answer(
                    "❌ У вас уже есть активный заказ. Завершите или отмените его."
                )
            cur.execute(
                "INSERT INTO orders (user_id, username, item_type, "
                "amount_rub, amount_virts, server, nick) "
                "VALUES (?, ?, 'currency', ?, ?, ?, ?)",
                (uid, user_label(m.from_user), data["rub"], data["virts"], server, nick),
            )
            db.commit()
            order_id = cur.lastrowid
            ticket_state.pop(uid, None)
            return await m.answer(
                f"🛒 ЗАКАЗ #{order_id} СОЗДАН\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💴 Валюта: {fmt_virts(data['virts'])} виртов\n"
                f"💵 Сумма: ~{data['rub']}₽\n"
                f"🌍 Сервер: {server}\n"
                f"👤 Ник: {nick}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"Выберите способ оплаты или отмените заказ:",
                reply_markup=pay_kb(order_id),
            )

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

def _mask(value: str, keep: int = 4) -> str:
    if not value:
        return "<empty>"
    if len(value) <= keep:
        return "*" * len(value)
    return value[:keep] + "*" * (len(value) - keep)


async def main():
    log.info("=" * 50)
    log.info("Запуск бота Black Russia Shop")
    log.info("TOKEN: %s", _mask(TOKEN))
    log.info("GROUP_ID: %s", GROUP_ID)
    log.info("SUPPORT_ID: %s", SUPPORT_ID)
    log.info("DB_PATH: %s", DB_PATH)
    log.info("RUB_PER_STAR: %s", RUB_PER_STAR)
    log.info("ENCRYPTION_KEY: %s", _mask(ENC_KEY_RAW))
    log.info("=" * 50)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(*_):
        log.info("Получен сигнал остановки, завершаюсь корректно...")
        stop_event.set()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass  # Windows / некоторые окружения

    polling_task = asyncio.create_task(_polling_with_retry())
    stop_task = asyncio.create_task(stop_event.wait())

    try:
        done, _ = await asyncio.wait(
            {polling_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done and not polling_task.done():
            polling_task.cancel()
            try:
                await polling_task
            except (asyncio.CancelledError, Exception):
                pass
    finally:
        try:
            await dp.stop_polling()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        try:
            db.commit()
            db.close()
        except Exception:
            pass
        log.info("Бот остановлен.")


async def _polling_with_retry():
    # 1) Сбрасываем webhook (если был) и зависшие апдейты — частая
    #    причина "бот молчит" после переезда на хост.
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        log.info("Webhook сброшен, зависшие апдейты очищены.")
    except Exception as e:
        log.warning("Не удалось сбросить webhook: %s", e)

    # 2) Проверяем, что токен реально валиден и бот доступен.
    try:
        me = await bot.get_me()
        log.info("✅ Бот @%s (id=%s) подключён к Telegram.", me.username, me.id)
    except Exception as e:
        log.exception("❌ Не удалось получить информацию о боте: %s", e)
        log.error("Проверьте TOKEN — возможно он неверный или отозван.")
        raise

    # 3) Уведомляем админ-группу о запуске — если уведомление дошло,
    #    значит бот точно живой и токен правильный.
    try:
        await bot.send_message(
            GROUP_ID,
            f"🟢 Бот @{me.username} запущен и готов к работе.",
        )
    except Exception as e:
        log.warning("Не удалось уведомить админ-группу о запуске: %s", e)

    backoff = 5
    while True:
        try:
            await dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types(),
                handle_signals=False,
            )
            return  # штатное завершение
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Polling упал: %s. Перезапуск через %s сек...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем (Ctrl+C).")
