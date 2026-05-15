import logging
import re
import secrets
import string
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.base import StorageKey
import pymysql
import asyncio
from aiogram.types import InputFile
from aiogram import F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from aiogram import Router
from datetime import datetime, timedelta, date
import pandas as pd
from io import BytesIO
import os
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

API_TOKEN = os.getenv('API_TOKEN')

ADMIN_ID_1=os.getenv('ADMIN_ID_1')
ADMIN_ID_2=os.getenv('ADMIN_ID_2')

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

ph_router = Router()

dp.include_router(ph_router)

logging.basicConfig(level=logging.INFO)


class CompleteOrderStates(StatesGroup):
    result_photos = State()
    awaiting_revision = State()


class CreateOrderStates(StatesGroup):
    description = State()
    photos = State()
    confirm = State()


class LoginStates(StatesGroup):
    login = State()
    password = State()


class OtpStates(StatesGroup):
    code = State()


INVITES_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS expert_invites (
    code        VARCHAR(16) PRIMARY KEY,
    created_by  BIGINT       NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_by_tg  BIGINT       NULL,
    used_at     TIMESTAMP    NULL
)
"""


def generate_otp(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def is_admin(tg_id) -> bool:
    sid = str(tg_id)
    return sid == str(ADMIN_ID_1) or sid == str(ADMIN_ID_2)


def ensure_invites_table() -> None:
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute(INVITES_TABLE_DDL)
            connection.commit()
        connection.close()
    except Exception as e:
        logging.error(f"Не удалось создать таблицу expert_invites: {e}")


class RevisionStates(StatesGroup):
    revision_comment = State()
    revision_photos = State()
    revision_awaiting = State()


class DeclineOrderStates(StatesGroup):
    reason = State()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🌛 Войти в систему")],
            [types.KeyboardButton(text="🔑 У меня есть код приглашения")],
        ],
        resize_keyboard=True
    )

    await message.answer(
        "✌ Добро пожаловать в сервис помощи экспертам!\nВыберите действие:",
        reply_markup=keyboard
    )


@dp.message(F.text == "Удалить заявку")
async def delete_order_start(message: types.Message):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT o.id, o.description
                           FROM orders o
                           WHERE o.expert_id = (SELECT id FROM users_expert WHERE tg_id = %s)
                             AND o.status = 'Ожидает исполнителя'
                           """, (message.from_user.id,))

            orders = cursor.fetchall()

            if not orders:
                await message.answer("❌ Нет активных заявок для отмены")
                return

            keyboard = InlineKeyboardMarkup(inline_keyboard=[])
            for order in orders:
                keyboard.inline_keyboard.append([
                    InlineKeyboardButton(
                        text=f"#{order[0]} - {order[1][:30]}",
                        callback_data=f"cancel_order_{order[0]}"
                    )
                ])

            await message.answer(
                "Выберите заявку для отмены:",
                reply_markup=keyboard
            )

    except Exception as e:
        logging.error(f"Ошибка получения заявок: {e}")
        await message.answer("⚠️ Ошибка при загрузке заявок")
    finally:
        connection.close()


@dp.callback_query(lambda c: c.data.startswith("cancel_order_"))
async def confirm_cancel_order(callback: types.CallbackQuery):
    order_id = int(callback.data.split("_")[-1])

    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_cancel_{order_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])

    await callback.message.edit_text(
        f"Вы уверены, что хотите отменить заявку #{order_id}?",
        reply_markup=confirm_keyboard
    )


@dp.callback_query(lambda c: c.data.startswith("confirm_cancel_"))
async def process_cancel_order(callback: types.CallbackQuery):
    order_id = int(callback.data.split("_")[-1])
    user_id = callback.from_user.id

    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                           UPDATE orders
                           SET status = 'Отменено'
                           WHERE id = %s
                             AND expert_id = (SELECT id FROM users_expert WHERE tg_id = %s)
                           """, (order_id, user_id))

            cursor.execute("""
                           SELECT up.tg_id, om.message_id
                           FROM order_messages om
                                    LEFT JOIN users_ph up ON om.ph_id = up.id
                           WHERE order_id = %s
                           """, (order_id,))

            messages = cursor.fetchall()

            for ph_id, message_id in messages:
                try:
                    await bot.edit_message_text(
                        chat_id=ph_id,
                        message_id=message_id,
                        text=f"🚫 Заявка #{order_id} отменена экспертом",
                        reply_markup=None
                    )
                except Exception as e:
                    logging.error(f"Ошибка обновления сообщения: {e}")

            connection.commit()

            await callback.message.edit_text(f"✅ Заявка #{order_id} успешно отменена!")

    except Exception as e:
        logging.error(f"Ошибка отмены заявки: {e}")
        await callback.message.edit_text("⚠️ Ошибка при отмене заявки")
    finally:
        connection.close()


@dp.callback_query(lambda c: c.data == "cancel_action")
async def cancel_action(callback: types.CallbackQuery):
    await callback.message.edit_text("❌ Действие отменено")


@dp.message(lambda message: message.text == "🌛 Войти в систему")
async def login_start(message: types.Message, state: FSMContext):
    await state.set_state(LoginStates.login)
    await message.answer("Введите ваш логин:", reply_markup=types.ReplyKeyboardRemove())


@dp.message(LoginStates.login)
async def process_login_input(message: types.Message, state: FSMContext):
    await state.update_data(login=message.text)
    await state.set_state(LoginStates.password)
    await message.answer("Введите ваш пароль:")


@dp.message(LoginStates.password)
async def process_password_input(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    login = user_data['login']
    password = message.text

    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore
        cursor = connection.cursor()

        cursor.execute(
            "SELECT name, surname FROM users_expert WHERE login = %s AND password = %s",
            (login, password)
        )
        expert = cursor.fetchone()
        if expert:
            keyboard = types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="Создать заявку")],
                    [types.KeyboardButton(text="Удалить заявку")]
                ],
                resize_keyboard=True
            )
            await message.answer(f"Добро пожаловать, {expert[0]} {expert[1]}!", reply_markup=keyboard)
            await state.clear()
            return

        cursor.execute(
            "SELECT name FROM users_ph WHERE login = %s AND password = %s",
            (login, password)
        )
        ph = cursor.fetchone()
        if ph:
            await message.answer(f"Добро пожаловать, {ph[0]}!")
            await state.clear()
            return

        await message.answer("❌ Неверный логин или пароль")

    except Exception as e:
        logging.error(f"Ошибка при входе: {e}")
        await message.answer("⚠️ Произошла ошибка. Попробуйте позже.")
    finally:
        cursor.close()
        connection.close()
        await state.clear()


@dp.message(F.text == "🔑 У меня есть код приглашения")
async def otp_start(message: types.Message, state: FSMContext):
    await state.set_state(OtpStates.code)
    await message.answer(
        "Введите одноразовый код приглашения:",
        reply_markup=types.ReplyKeyboardRemove(),
    )


@dp.message(OtpStates.code)
async def otp_process(message: types.Message, state: FSMContext):
    code = (message.text or "").strip().upper()
    tg_id = message.from_user.id
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM users_expert WHERE tg_id = %s",
                (tg_id,),
            )
            if cursor.fetchone():
                await message.answer("Вы уже зарегистрированы как эксперт.")
                await state.clear()
                return

            cursor.execute(
                "SELECT code FROM expert_invites WHERE code = %s AND used_by_tg IS NULL",
                (code,),
            )
            if not cursor.fetchone():
                await message.answer("❌ Код недействителен или уже использован.")
                await state.clear()
                return

            name = (message.from_user.first_name or "Эксперт")[:64]
            surname = (message.from_user.last_name or "")[:64]
            login = f"tg_{tg_id}"

            cursor.execute(
                """INSERT INTO users_expert (tg_id, name, surname, login, password, banned)
                   VALUES (%s, %s, %s, %s, %s, 0)""",
                (tg_id, name, surname, login, ""),
            )
            cursor.execute(
                """UPDATE expert_invites
                   SET used_by_tg = %s, used_at = NOW()
                   WHERE code = %s""",
                (tg_id, code),
            )
            connection.commit()

        keyboard = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="Создать заявку")],
                [types.KeyboardButton(text="Удалить заявку")],
            ],
            resize_keyboard=True,
        )
        await message.answer(
            f"✅ Готово! Добро пожаловать, {name}!",
            reply_markup=keyboard,
        )
    except Exception as e:
        logging.error(f"Ошибка OTP-регистрации: {e}")
        await message.answer("⚠️ Произошла ошибка. Попробуйте позже.")
    finally:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass
        await state.clear()


@dp.message(Command("invite"))
async def cmd_invite(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    code = generate_otp(8)
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO expert_invites (code, created_by) VALUES (%s, %s)",
                (code, message.from_user.id),
            )
            connection.commit()
        await message.answer(
            f"🔑 Код приглашения для нового эксперта:\n\n`{code}`\n\n"
            "Перешлите его новому эксперту. Код одноразовый.",
            parse_mode="Markdown",
        )
    except Exception as e:
        logging.error(f"Ошибка /invite: {e}")
        await message.answer("⚠️ Не удалось создать код.")
    finally:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass


@dp.message(Command("invites"))
async def cmd_invites(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT code, created_at FROM expert_invites "
                "WHERE used_by_tg IS NULL ORDER BY created_at DESC"
            )
            rows = cursor.fetchall()
        if not rows:
            await message.answer("Активных кодов нет.")
            return
        lines = [f"`{code}` — {created_at}" for code, created_at in rows]
        await message.answer(
            "Активные коды:\n" + "\n".join(lines),
            parse_mode="Markdown",
        )
    except Exception as e:
        logging.error(f"Ошибка /invites: {e}")
        await message.answer("⚠️ Не удалось получить список.")
    finally:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass


@dp.message(Command("revoke"))
async def cmd_revoke(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /revoke <код>")
        return
    code = parts[1].strip().upper()
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM expert_invites WHERE code = %s AND used_by_tg IS NULL",
                (code,),
            )
            deleted = cursor.rowcount
            connection.commit()
        if deleted:
            await message.answer(f"✅ Код `{code}` удалён.", parse_mode="Markdown")
        else:
            await message.answer("Код не найден или уже использован.")
    except Exception as e:
        logging.error(f"Ошибка /revoke: {e}")
        await message.answer("⚠️ Не удалось удалить код.")
    finally:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass


@dp.message(F.text == "Создать заявку")
async def create_order_start(message: types.Message, state: FSMContext):
    connection = pymysql.connect(**DB_CONFIG)  # type: ignore
    cursor = connection.cursor()
    cursor.execute(
        "SELECT tg_id, banned FROM users_expert WHERE tg_id = %s",
        (message.from_user.id),  # type: ignore
    )
    ban = cursor.fetchone()[1]  # type: ignore
    print(ban)
    if ban == 0:  # type: ignore
        await state.set_state(CreateOrderStates.description)
        await message.answer("Введите описание заявки:", reply_markup=types.ReplyKeyboardRemove())
    else:  # type: ignore
        await message.answer("❌ ВЫ ЗАБЛОКИРОВАНЫ! ❌", reply_markup=types.ReplyKeyboardRemove())


@dp.message(CreateOrderStates.description)
async def process_order_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text)
    await state.set_state(CreateOrderStates.photos)
    await message.answer("Прикрепите фотографии (максимум 6). После каждой фото можно завершить:",
                         reply_markup=types.ReplyKeyboardMarkup(
                             keyboard=[[types.KeyboardButton(text="Завершить добавление фото")]],
                             resize_keyboard=True
                         ))


@dp.message(CreateOrderStates.photos, F.photo)
async def process_order_photos(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    photos = user_data.get('photos', [])

    if len(photos) >= 6:
        await message.answer("Достигнут максимум 6 фотографий!")
        return

    photos.append(message.photo[-1].file_id)  # type: ignore
    await state.update_data(photos=photos)

    if len(photos) < 6:
        await message.answer(f"Добавлено фото {len(photos)}/6. Отправьте ещё или нажмите 'Завершить'")
    else:
        await message.answer("Максимум достигнут. Создаем заявку...")
        await save_order_data(message, state)


@dp.message(CreateOrderStates.photos, F.text == "Завершить добавление фото")
async def finish_photos(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    if not user_data.get('photos'):
        await message.answer("Нужно добавить хотя бы одно фото!")
        return
    await save_order_data(message, state)


def format_order_card(order_id, expert_id, description, status_line):
    return (
        f"📄 Заявка #{order_id}\n"
        f"👤 Создатель: #клиент{expert_id}\n"
        f"Описание: {description}\n"
        f"Статус: {status_line}"
    )


async def save_order_data(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    expert_id = await get_expert_id(message.from_user.id)  # type: ignore

    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Создать заявку")],
            [types.KeyboardButton(text="Удалить заявку")]
        ],
        resize_keyboard=True
    )

    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore
        cursor = connection.cursor()

        cursor.execute(
            """INSERT INTO orders
                   (expert_id, description, status)
               VALUES (%s, %s, 'Ожидает исполнителя')""",
            (expert_id, user_data['description'])
        )
        order_id = cursor.lastrowid

        for photo_id in user_data.get('photos', []):
            cursor.execute(
                """INSERT INTO order_photos
                       (order_id, photo_url)
                   VALUES (%s, %s)""",
                (order_id, photo_id)
            )

        connection.commit()

        await send_order_to_ph(order_id, expert_id, user_data['description'], user_data.get('photos', []))

        await message.answer("✅ Заявка успешно создана!", reply_markup=keyboard)

    except Exception as e:
        logging.error(f"Ошибка создания заявки: {e}")
        await message.answer("❌ Ошибка при создании заявки")
    finally:
        cursor.close()
        connection.close()
        await state.clear()


async def get_expert_id(user_id):
    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore
        cursor = connection.cursor()
        cursor.execute(
            "SELECT id FROM users_expert WHERE tg_id = %s",
            (user_id,)
        )
        return cursor.fetchone()[0]  # type: ignore
    except Exception as e:
        logging.error(f"Ошибка получения expert_id: {e}")
        return None


async def get_ph_id(user_id):
    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore
        cursor = connection.cursor()
        cursor.execute(
            "SELECT id FROM users_ph WHERE tg_id = %s",
            (user_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Ошибка получения ph_id: {e}")
        return None


@ph_router.callback_query(lambda c: c.data.startswith("take_order_"))
async def take_order(callback: types.CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split("_")[-1])  # type: ignore
    ph_id = await get_ph_id(callback.from_user.id)

    if not ph_id:
        await callback.answer("❌ Вы не зарегистрированы как исполнитель!", show_alert=True)
        return

    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore

        cursor2 = connection.cursor()
        with connection.cursor() as cursor2:
            cursor2.execute("""
                            SELECT COUNT(*)
                            FROM orders
                            WHERE ph_id = %s
                              AND status = 'В работе'
                            """, (ph_id,))
            if cursor2.fetchone()[0] > 0:  # type: ignore
                await callback.answer("У вас уже есть заявка в работе!", show_alert=True)
                return

        cursor3 = connection.cursor()
        cursor3.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
        status = cursor3.fetchone()[0]

        if status == "Отменено":
            await callback.answer("⚠️ Заявка была отменена экспертом!", show_alert=True)
            return

        cursor = connection.cursor()
        cursor.execute(
            "SELECT status, description, expert_id FROM orders WHERE id = %s",
            (order_id,)
        )
        order_data = cursor.fetchone()
        if not order_data:
            await callback.answer("⚠️ Заявка не найдена!")
            return

        status, description, expert_id = order_data

        if status != "Ожидает исполнителя":
            await callback.answer("⚠️ Заявка уже взята в работу!", show_alert=True)
            return

        cursor.execute(
            """UPDATE orders
               SET status = 'В работе',
                   ph_id  = %s
               WHERE id = %s""",
            (ph_id, order_id)
        )

        cursor.execute(
            "SELECT up.tg_id, om.message_id FROM users_ph up LEFT JOIN order_messages om ON om.ph_id = up.id WHERE om.order_id = %s",
            (order_id))
        all_messages = cursor.fetchall()
        print(all_messages)

        cursor.execute(
            "SELECT up.name FROM orders o INNER JOIN users_ph up on up.id = o.ph_id WHERE o.id = %s", (order_id))
        ph_name = cursor.fetchone()[0]

        for target_ph_id, message_id in all_messages:
            try:
                new_text = format_order_card(order_id, expert_id, description, f"В работе у {ph_name}")

                await bot.edit_message_text(
                    chat_id=target_ph_id,
                    message_id=message_id,
                    text=new_text,
                    reply_markup=None
                )
            except Exception as e:
                logging.error(f"Ошибка обновления сообщения {message_id}: {e}")

        connection.commit()

        await state.set_state(CompleteOrderStates.result_photos)
        await state.update_data(
            order_id=order_id,
            expert_id=expert_id,
            ph_id=ph_id,
            photos=[]
        )

        await bot.send_message(
            chat_id=callback.from_user.id,
            text="📤 Отправьте до ТРЁХ фотографий результата выполнения заказа:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Завершить отправку фото")]],
                resize_keyboard=True
            )
        )

        await callback.answer("✅ Заявка взята в работу!", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка: {e}")
        await callback.answer("❌ Ошибка!")
    finally:
        cursor2.close()
        cursor.close()
        connection.close()


@ph_router.message(CompleteOrderStates.result_photos, F.photo)
async def process_result_photos(message: types.Message, state: FSMContext):
    user_data = await state.get_data()

    ph_id = await get_ph_id(message.from_user.id)  # type: ignore
    if ph_id != user_data.get('ph_id'):
        await message.answer("❌ Это действие доступно только исполнителю заявки")
        return

    photos = user_data.get('photos', [])

    if len(photos) >= 3:
        await message.answer("⚠️ Максимум 3 фото! Нажмите 'Завершить отправку фото'")
        return

    photos.append(message.photo[-1].file_id)  # type: ignore
    await state.update_data(photos=photos)

    if len(photos) < 3:
        await message.answer(f"✅ Фото {len(photos)}/3 принято. Отправьте еще или нажмите кнопку завершения.")
    else:
        await message.answer("✅ Принято 3 фото. Нажмите 'Завершить отправку фото' для завершения заявки.")


@ph_router.message(CompleteOrderStates.result_photos, F.text == "Завершить отправку фото")
async def finish_photos_upload(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    photos = user_data.get('photos', [])

    if not photos:
        await message.answer("❌ Нужно отправить хотя бы одно фото!")
        return

    order_id = user_data['order_id']
    expert_id = user_data['expert_id']
    ph_id = user_data['ph_id']
    photo_count = len(photos)

    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore
        cursor = connection.cursor()

        cursor.execute(
            "UPDATE orders SET status = 'Завершено', result_photo = %s WHERE id = %s",
            (photo_count, order_id)
        )

        cursor.execute(
            "SELECT up.tg_id, om.message_id FROM users_ph up LEFT JOIN order_messages om ON om.ph_id = up.id WHERE om.order_id = %s",
            (order_id))
        all_messages = cursor.fetchall()
        cursor.execute("SELECT description FROM orders WHERE id = %s", (order_id,))
        description = cursor.fetchone()[0]  # type: ignore

        for target_ph_id, message_id in all_messages:
            try:
                new_text = format_order_card(order_id, expert_id, description, "Выполнена")
                await bot.edit_message_text(
                    chat_id=target_ph_id,
                    message_id=message_id,
                    text=new_text,
                    reply_markup=None
                )
            except Exception as e:
                logging.error(f"Ошибка обновления сообщения {message_id}: {e}")

        cursor.execute("SELECT tg_id FROM users_expert WHERE id = %s", (expert_id,))
        expert_tg_id = cursor.fetchone()[0]  # type: ignore

        media = [types.InputMediaPhoto(media=photo) for photo in photos]

        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="Отправить на доработку?", callback_data='yes')
        )

        if media:
            media[0].caption = f"Результат по заявке #{order_id}"
            await bot.send_media_group(
                chat_id=expert_tg_id,
                media=media
            )
            await bot.send_message(
                chat_id=expert_tg_id,
                text=f"Отправить на доработку? #{order_id}",
                reply_markup=builder.as_markup()
            )
            await bot.send_media_group(chat_id=ADMIN_ID_1, media=media)
            await bot.send_media_group(chat_id=ADMIN_ID_2, media=media)

        await message.answer("✅ Результат успешно отправлен эксперту!", reply_markup=ReplyKeyboardRemove())
        connection.commit()

    except Exception as e:
        logging.error(f"Ошибка: {e}")
        await message.answer("❌ Произошла ошибка при обработке")
    finally:
        cursor.close()
        connection.close()
        await state.clear()


async def send_order_to_ph(order_id, expert_id, description, photos):
    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore
        cursor = connection.cursor()

        cursor.execute("SELECT id, tg_id FROM users_ph")
        performers = cursor.fetchall()

        for ph_id, tg_id in performers:
            try:
                message_text = (
                    f"📄 Новая заявка #{order_id}\n"
                    f"👤 Создатель: #клиент{expert_id}\n"
                    f"Описание: {description}\n"
                    f"Статус: Ожидает исполнителя"
                )
                markup = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Взять в работу", callback_data=f"take_order_{order_id}"),
                    InlineKeyboardButton(text="Отказать", callback_data=f"retake_order_{order_id}")
                ]])

                msg = await bot.send_message(tg_id, message_text, reply_markup=markup)

                for photo_id in photos:
                    await bot.send_photo(tg_id, photo=photo_id)

                cursor.execute(
                    "INSERT INTO order_messages (order_id, ph_id, message_id) VALUES (%s, %s, %s)",
                    (order_id, ph_id, msg.message_id)
                )
                connection.commit()

            except Exception as e:
                logging.error(f"Ошибка отправки исполнителю {ph_id}: {e}")

    except Exception as e:
        logging.error(f"Ошибка рассылки заявок: {e}")
    finally:
        cursor.close()
        connection.close()


@dp.message(Command("rep"))
async def generate_report(message: types.Message):
    # if message.from_user.id != ADMIN_ID_1 and message.from_user.id != ADMIN_ID_2:  # type: ignore
    #     await message.answer("❌ Доступ запрещен!")
    #     return
    #     коммент до выяснения обстоятельств((

    file_name = None
    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore

        experts_query = """
                        SELECT ue.id, 
                               ue.name, 
                               ue.surname, 
                               ue.adress_oto, 
                               DATE_FORMAT(o.created_at, '%Y-%m') as month,
                COUNT(o.id) as total_orders,
                ph.order_price * SUM(o.result_photo) as total_amount,
                ue.tg_id as TelegramId
                        FROM orders o
                            JOIN users_expert ue 
                        ON o.expert_id = ue.id
                            JOIN users_ph ph ON o.ph_id = ph.id
                        WHERE o.status = 'Завершено'
                        GROUP BY ue.id, month
                        ORDER BY ue.id, month 
                        """
        experts_df = pd.read_sql(experts_query, connection)  # type: ignore

        ph_query = """
                   SELECT ph.id, 
                          ph.name, 
                          DATE_FORMAT(o.created_at, '%Y-%m') as month,
                COUNT(o.id) as total_orders,
                ph.order_price * SUM(o.result_photo) as total_amount
                   FROM orders o
                       JOIN users_ph ph 
                   ON o.ph_id = ph.id
                   WHERE o.status = 'Завершено'
                   GROUP BY ph.id, month
                   ORDER BY ph.id, month 
                   """
        ph_df = pd.read_sql(ph_query, connection)  # type: ignore

        experts_query2 = """
                         SELECT ue.id, 
                                ue.name, 
                                ue.surname, 
                                ue.adress_oto, 
                                DATE_FORMAT(o.created_at, '%Y-%m-%d') as DAY,
                COUNT(o.id) as total_orders,
                ph.order_price * SUM(o.result_photo) as total_amount,
                ue.tg_id as TelegramId
                         FROM orders o
                             JOIN users_expert ue 
                         ON o.expert_id = ue.id
                             JOIN users_ph ph ON o.ph_id = ph.id
                         WHERE o.status = 'Завершено'
                         GROUP BY ue.id, DAY
                         ORDER BY ue.id, DAY 
                         """
        experts_df2 = pd.read_sql(experts_query2, connection)  # type: ignore

        ph_query2 = """
                    SELECT ph.id, 
                           ph.name, 
                           DATE_FORMAT(o.created_at, '%Y-%m-%d') as DAY,
                COUNT(o.id) as total_orders,
                ph.order_price * SUM(o.result_photo) as total_amount
                    FROM orders o
                        JOIN users_ph ph 
                    ON o.ph_id = ph.id
                    WHERE o.status = 'Завершено'
                    GROUP BY ph.id, DAY
                    ORDER BY ph.id, DAY 
                    """
        ph_df2 = pd.read_sql(ph_query2, connection)  # type: ignore

        file_name = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(file_name) as writer:
            experts_df.to_excel(writer, sheet_name='Эксперты_по_мес', index=False)
            ph_df.to_excel(writer, sheet_name='Исполнители_по_мес', index=False)
            experts_df2.to_excel(writer, sheet_name='Эксперты_по_дням', index=False)
            ph_df2.to_excel(writer, sheet_name='Исполнители_по_дням', index=False)

        with open(file_name, "rb") as file:
            await bot.send_document(
                chat_id=message.chat.id,
                document=types.BufferedInputFile(file.read(), filename=file_name),
                caption="📊 Отчет по завершенным заявкам"
            )

        await message.answer("✅ Отчет успешно сгенерирован и отправлен!")

    except Exception as e:
        logging.error(f"Ошибка генерации отчета: {e}")
        await message.answer("❌ Ошибка при генерации отчета")
    finally:
        if connection:
            connection.close()
        if file_name and os.path.exists(file_name):
            os.remove(file_name)


@dp.message(Command("repexp"))
async def generate_report2(message: types.Message):
    if message.from_user.id != ADMIN_ID_1 and message.from_user.id != ADMIN_ID_2:  # type: ignore
        await message.answer("❌ Доступ запрещен!")
        return

    argument = message.text.split()[1:]  # type: ignore
    file_name = None
    try:
        connection = pymysql.connect(**DB_CONFIG)  # type: ignore

        experts_query = f"""
            SELECT 
                ue.id,
                ue.name,
                ue.surname,
                ue.adress_oto,
                DATE_FORMAT(o.created_at, '%Y-%m') as month,
                COUNT(o.id) as total_orders,
                SUM(ph.order_price) as total_amount,
                ue.tg_id as TelegramId
            FROM orders o
            JOIN users_expert ue ON o.expert_id = ue.id
            JOIN users_ph ph ON o.ph_id = ph.id
            WHERE o.status = 'Завершено'
            AND ue.tg_id = '{argument[0]}'
            GROUP BY ue.id, month
            ORDER BY ue.id, month
        """
        experts_df = pd.read_sql(experts_query, connection)  # type: ignore

        file_name = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(file_name) as writer:
            experts_df.to_excel(writer, sheet_name='Эксперт', index=False)

        with open(file_name, "rb") as file:
            await bot.send_document(
                chat_id=message.chat.id,
                document=types.BufferedInputFile(file.read(), filename=file_name),
                caption="📊 Отчет по завершенным заявкам"
            )

        await message.answer("✅ Отчет успешно сгенерирован и отправлен!")

    except Exception as e:
        logging.error(f"Ошибка генерации отчета: {e}")
        await message.answer("❌ Ошибка при генерации отчета")
    finally:
        if connection:
            connection.close()
        if file_name and os.path.exists(file_name):
            os.remove(file_name)


@dp.callback_query(lambda c: c.data == "yes")
async def request_revision(callback: types.CallbackQuery, state: FSMContext):
    text = callback.message.text
    m = re.search(r"#(\d+)", text or "")
    order_id = int(m.group(1)) if m else None
    print(order_id, text)
    if not order_id:
        await callback.answer("❌ Ошибка: не найден номер заявки")
        return

    await state.set_state(RevisionStates.revision_comment)
    await state.update_data(order_id=order_id)

    await callback.message.answer("📝 Введите комментарий для доработки:")


@dp.message(RevisionStates.revision_comment)
async def process_revision_comment(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    order_id = user_data['order_id']
    comment = message.text
    logging.info(f"Expert submitted revision comment for order #{order_id}: {comment}")

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        cursor.execute(
            "UPDATE orders SET status = 'На доработке', revision_comment = %s WHERE id = %s",
            (comment, order_id)
        )

        cursor.execute(
            "SELECT ph_id FROM orders WHERE id = %s",
            (order_id,)
        )
        ph_id = cursor.fetchone()[0]
        logging.info(f"Found PH ID for order #{order_id}: {ph_id}")

        cursor.execute(
            "SELECT tg_id FROM users_ph WHERE id = %s",
            (ph_id,)
        )
        ph_tg_id = cursor.fetchone()[0]
        logging.info(f"Found PH Telegram ID: {ph_tg_id}")

        connection.commit()

        cursor.execute(
            "INSERT INTO revision_states (order_id, ph_id, state) VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE state = VALUES(state)",
            (order_id, ph_id, "RevisionStates:revision_photos")
        )
        connection.commit()

        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(
            text="Отправить фото доработки",
            callback_data=f"activate_revision_{order_id}"
        ))

        await bot.send_message(
            ph_tg_id,
            f"📝 Получен комментарий по заявке #{order_id}:\n\n{comment}\n\n"
            "Нажмите кнопку ниже, чтобы начать отправку фотографий с исправлениями:",
            reply_markup=builder.as_markup()
        )

        await message.answer("✅ Комментарий отправлен исполнителю!")
        logging.info(f"Revision comment sent to PH {ph_tg_id} for order #{order_id}")

    except Exception as e:
        logging.error(f"Ошибка при обработке комментария: {e}", exc_info=True)
        await message.answer("❌ Ошибка при отправке комментария")
    finally:
        cursor.close()
        connection.close()
        await state.clear()


@dp.message(RevisionStates.revision_photos, F.photo)
async def process_revision_photos(message: types.Message, state: FSMContext):
    logging.info(f"Received photo from PH {message.from_user.id} in revision_photos state")

    user_data = await state.get_data()
    photos = user_data.get('photos', [])
    logging.info(f"Current photos in state: {len(photos)}")

    if len(photos) >= 3:
        await message.answer("⚠️ Максимум 3 фото! Нажмите 'Завершить отправку фото'")
        return

    photos.append(message.photo[-1].file_id)
    await state.update_data(photos=photos)
    logging.info(f"Photo added. Total photos: {len(photos)}")

    if len(photos) < 3:
        await message.answer(f"✅ Фото {len(photos)}/3 принято. Отправьте еще или нажмите кнопку завершения.")
    else:
        await message.answer("✅ Принято 3 фото. Нажмите 'Завершить отправку фото' для отправки.")


@dp.message(RevisionStates.revision_photos, F.text == "Завершить отправку фото")
async def finish_revision_photos(message: types.Message, state: FSMContext):
    logging.info(f"PH {message.from_user.id} finished photo upload for revision")

    user_data = await state.get_data()
    photos = user_data.get('photos', [])
    order_id = user_data['order_id']
    logging.info(f"Processing revision photos for order #{order_id}. Photo count: {len(photos)}")

    if not photos:
        await message.answer("❌ Нужно отправить хотя бы одно фото!")
        return

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Обновляем статус заявки
        cursor.execute(
            "UPDATE orders SET status = 'Ожидает проверки' WHERE id = %s",
            (order_id,)
        )

        # Удаляем запись о состоянии
        cursor.execute(
            "DELETE FROM revision_states WHERE order_id = %s",
            (order_id,)
        )

        # Получаем данные эксперта
        cursor.execute(
            "SELECT expert_id FROM orders WHERE id = %s",
            (order_id,)
        )
        expert_id = cursor.fetchone()[0]
        logging.info(f"Found expert ID: {expert_id}")

        cursor.execute(
            "SELECT tg_id FROM users_expert WHERE id = %s",
            (expert_id,)
        )
        expert_tg_id = cursor.fetchone()[0]
        logging.info(f"Found expert Telegram ID: {expert_tg_id}")

        connection.commit()

        # Отправляем фотографии эксперту
        media = [types.InputMediaPhoto(media=photo) for photo in photos]
        if media:
            media[0].caption = f"Результат доработки по заявке #{order_id}"
            await bot.send_media_group(chat_id=expert_tg_id, media=media)

            # Создаем клавиатуру для эксперта
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text="✅ Принять", callback_data=f'accept_{order_id}'),
                InlineKeyboardButton(text="🔄 На доработку", callback_data=f'revision_{order_id}')
            )

            await bot.send_message(
                chat_id=expert_tg_id,
                text=f"Заявка #{order_id} готова к проверке:",
                reply_markup=builder.as_markup()
            )
            logging.info(f"Revision results sent to expert {expert_tg_id}")

        await message.answer("✅ Результат доработки отправлен эксперту!", reply_markup=ReplyKeyboardRemove())

    except Exception as e:
        logging.error(f"Ошибка при завершении доработки: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при отправке")
    finally:
        cursor.close()
        connection.close()
        await state.clear()
        logging.info(f"State cleared for PH {message.from_user.id}")


@dp.callback_query(lambda c: c.data.startswith("accept_"))
async def accept_revision(callback: types.CallbackQuery):
    order_id = int(callback.data.split('_')[1])

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE orders SET status = 'Завершено' WHERE id = %s",
            (order_id,)
        )
        connection.commit()

        cursor.execute(
            "SELECT ph.tg_id FROM orders o LEFT JOIN users_ph ph ON ph.id = o.ph_id WHERE o.id = %s",
            (order_id,)
        )
        ph_tg_id = cursor.fetchone()[0]
        await bot.send_message(chat_id=ph_tg_id, text=f"✅ Эксперт принял доработку по заявке #{order_id}!")
        await callback.message.edit_text(f"✅ Заявка #{order_id} принята!")

    except Exception as e:
        logging.error(f"Ошибка при принятии заявки: {e}")
        await callback.answer("❌ Ошибка!")
    finally:
        cursor.close()
        connection.close()


@dp.callback_query(lambda c: c.data.startswith("revision_"))
async def request_new_revision(callback: types.CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split('_')[1])
    await state.set_state(RevisionStates.revision_comment)
    await state.update_data(order_id=order_id)
    await callback.message.answer("📝 Введите новый комментарий для доработки:")


async def send_result_to_expert(expert_tg_id, photos, order_id):
    media = [types.InputMediaPhoto(media=photo) for photo in photos]
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Принять", callback_data=f'accept_{order_id}'),
        InlineKeyboardButton(text="🔄 На доработку", callback_data=f'revision_{order_id}')
    )

    if media:
        media[0].caption = f"Результат по заявке #{order_id}"
        await bot.send_media_group(chat_id=expert_tg_id, media=media)
        await bot.send_message(
            chat_id=expert_tg_id,
            text=f"Проверьте результат по заявке #{order_id}:",
            reply_markup=builder.as_markup()
        )


@dp.callback_query(lambda c: c.data.startswith("activate_revision_"))
async def activate_revision_state(callback: types.CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split('_')[-1])
    logging.info(f"Activating revision state for order #{order_id}")

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute(
            "SELECT state FROM revision_states WHERE order_id = %s",
            (order_id,)
        )
        state_data = cursor.fetchone()

        if state_data and state_data[0] == "RevisionStates:revision_photos":
            await state.set_state(RevisionStates.revision_photos)
            await state.update_data(order_id=order_id, photos=[])

            await callback.message.answer(
                "✅ Готово! Теперь вы можете отправлять фотографии доработки.\n\n"
                "Отправьте до трёх фотографий с исправлениями:",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="Завершить отправку фото")],
                        [KeyboardButton(text="❌ Отменить доработку")]
                    ],
                    resize_keyboard=True
                )
            )
            await callback.answer()
        else:
            await callback.answer("❌ Состояние не найдено или устарело", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка активации состояния: {e}")
        await callback.answer("❌ Ошибка активации", show_alert=True)
    finally:
        connection.close()


@ph_router.callback_query(lambda c: c.data.startswith("retake_order_"))
async def decline_order_start(callback: types.CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split("_")[-1])

    connection = pymysql.connect(**DB_CONFIG)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
        status = cursor.fetchone()[0]

        if status != "Ожидает исполнителя":
            await callback.answer("⚠️ Заявка уже взята в работу!", show_alert=True)
            return
    finally:
        cursor.close()
        connection.close()

    await state.set_state(DeclineOrderStates.reason)
    await state.update_data(order_id=order_id)

    await callback.message.answer(
        "📝 Укажите причину отказа от заявки:",
        reply_markup=ReplyKeyboardRemove()
    )
    await callback.answer()


@ph_router.message(DeclineOrderStates.reason)
async def process_decline_reason(message: types.Message, state: FSMContext):
    reason = message.text
    user_data = await state.get_data()
    order_id = user_data['order_id']
    ph_id = await get_ph_id(message.from_user.id)

    if not ph_id:
        await message.answer("❌ Ошибка: не найден исполнитель")
        await state.clear()
        return

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        cursor.execute(
            "UPDATE orders SET status = 'Отменено', decline_reason = %s WHERE id = %s",
            (reason, order_id)
        )
        cursor.execute(
            "SELECT expert_id FROM orders WHERE id = %s",
            (order_id,)
        )
        expert_id = cursor.fetchone()[0]

        cursor.execute(
            "SELECT tg_id FROM users_expert WHERE id = %s",
            (expert_id,)
        )
        expert_tg_id = cursor.fetchone()[0]

        connection.commit()

        await bot.send_message(
            expert_tg_id,
            f"🚫 Ваша заявка #{order_id} отклонена исполнителем\n\n"
            f"Причина: {reason}"
        )

        cursor.execute(
            "SELECT up.tg_id, om.message_id FROM users_ph up LEFT JOIN order_messages om ON om.ph_id = up.id WHERE om.order_id = %s",
            (order_id))
        all_messages = cursor.fetchall()
        cursor.execute("SELECT description FROM orders WHERE id = %s", (order_id,))
        description = cursor.fetchone()[0]  # type: ignore

        for target_ph_id, message_id in all_messages:
            try:
                new_text = format_order_card(order_id, expert_id, description, "Отклонена исполнителем")
                await bot.edit_message_text(
                    chat_id=target_ph_id,
                    message_id=message_id,
                    text=new_text,
                    reply_markup=None
                )
            except Exception as e:
                logging.error(f"Ошибка обновления сообщения {message_id}: {e}")

        await message.answer("✅ Заявка успешно отклонена")

    except Exception as e:
        logging.error(f"Ошибка при отказе от заявки: {e}")
        await message.answer("❌ Ошибка при обработке отказа")
    finally:
        cursor.close()
        connection.close()
        await state.clear()


@dp.message(F.text == "Моя статистика")
async def show_ph_statistics(message: types.Message):
    ph_id = await get_ph_id(message.from_user.id)
    if not ph_id:
        await message.answer("❌ Вы не зарегистрированы как исполнитель!")
        return

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        cursor.execute("SELECT order_price FROM users_ph WHERE id = %s", (ph_id,))
        result = cursor.fetchone()
        if not result:
            await message.answer("❌ Ошибка получения данных исполнителя.")
            return
        order_price = result[0]

        # Получаем все значения и обрабатываем None
        cursor.execute("""
                       SELECT SUM(result_photo)
                       FROM orders
                       WHERE ph_id = %s
                         AND status = 'Завершено'
                       """, (ph_id,))
        completed_count_result = cursor.fetchone()
        completed_count = completed_count_result[0] or 0  # Преобразуем None в 0

        cursor.execute("""
                       SELECT COUNT(*)
                       FROM orders
                       WHERE ph_id = %s
                         AND status = 'На доработке'
                       """, (ph_id,))
        revision_requested_count_result = cursor.fetchone()
        revision_requested_count = revision_requested_count_result[0] or 0

        cursor.execute("""
                       SELECT COUNT(*)
                       FROM orders
                       WHERE ph_id = %s
                         AND status = 'В работе'
                       """, (ph_id,))
        in_progress_count_result = cursor.fetchone()
        in_progress_count = in_progress_count_result[0] or 0

        cursor.execute("""
                       SELECT COUNT(*)
                       FROM orders
                       WHERE ph_id = %s
                         AND status = 'Ожидает проверки'
                       """, (ph_id,))
        revision_pending_approval_count_result = cursor.fetchone()
        revision_pending_approval_count = revision_pending_approval_count_result[0] or 0

        today_start = datetime.combine(date.today(), datetime.min.time())
        today_end = datetime.combine(date.today(), datetime.max.time())

        cursor.execute("""
                       SELECT SUM(result_photo)
                       FROM orders
                       WHERE ph_id = %s
                         AND status = 'Завершено'
                         AND created_at >= %s
                         AND created_at <= %s
                       """, (ph_id, today_start, today_end))
        completed_today_count_result = cursor.fetchone()
        completed_today_count = completed_today_count_result[0] or 0

        earnings_today = completed_today_count * order_price

        stats_message = (
            f"📋 <b>Ваша статистика:</b>\n"
            f"├ Заявок в работе: <b>{in_progress_count}</b>\n"
            f"├ Заявок выполнено (всего): <b>{completed_count}</b>\n"
            f"├ Заявок на доработке: <b>{revision_requested_count}</b>\n"
            f"├ Ожидает проверки (после доработки): <b>{revision_pending_approval_count}</b>\n"
            f"├ Выполнено за сегодня: <b>{completed_today_count}</b>\n"
            f"└ Предполагаемый заработок за сегодня: <b>{earnings_today}</b> руб."
        )

        await message.answer(stats_message, parse_mode='HTML')

    except Exception as e:
        logging.error(f"Ошибка получения статистики исполнителя: {e}")
        await message.answer("❌ Произошла ошибка при получении статистики.")
    finally:
        try:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'connection' in locals() and connection:
                connection.close()
        except Exception:
            pass


@dp.message(RevisionStates.revision_photos, F.text == "❌ Отменить доработку")
async def cancel_revision(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    order_id = user_data.get('order_id')

    logging.info(f"PH {message.from_user.id} cancelled revision for order #{order_id}")

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        cursor.execute(
            "DELETE FROM revision_states WHERE order_id = %s",
            (order_id,)
        )

        connection.commit()

        await message.answer(
            "❌ Доработка отменена. Вы можете вернуться к ней позже через уведомление от эксперта.",
            reply_markup=ReplyKeyboardRemove()
        )

    except Exception as e:
        logging.error(f"Ошибка при отмене доработки: {e}")
        await message.answer("❌ Произошла ошибка при отмене доработки")
    finally:
        cursor.close()
        connection.close()
        await state.clear()

async def main():
    ensure_invites_table()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())