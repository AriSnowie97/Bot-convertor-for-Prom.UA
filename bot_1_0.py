import os
import io
import asyncio
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
import xml.etree.ElementTree as ET


# =============================
# ⚙️ Настройки
# =============================
TOKEN = os.getenv("TELEGRAM_TOKEN", "PASTE_TOKEN_HERE")
CURRENCY = "UAH"

# Маппинг колонок (все в lower-case)
SRC = {
    "id": ["product code", "main sku", "код", "артикул"],
    "name": ["name", "назва", "название"],
    "category": ["category", "категорія", "категория"],
    "subcategory": ["subcategory", "підкатегорія", "подкатегория"],
    "qty": ["quantity", "кількість", "количество"],
    "price": ["special price", "price", "ціна", "цена"],
    "description": ["description", "опис", "описание"],
    "photos": [
        "main photo", "photo1", "photo2", "photo3", "photo4", "photo5",
        "photo6", "photo7", "photo8", "photo 9", "photo 10", "photo 11", "photo 12",
        "посилання_зображення", "изображение"
    ],
    "weight": ["weight", "kg", "вага,кг", "вага", "вес"],
    "length": ["length", "довжина,см", "длина,см"],
    "width": ["width", "ширина,см"],
    "height": ["height", "висота,см", "высота,см"],
    "params_primary": [
        ("size of the set", "Розмір комплекту"),
        ("size extra", "Додатковий розмір"),
        ("sheet size", "Розмір простирадла"),
        ("size of the pillowcase", "Розмір наволочки"),
        ("dimensions of the duvet cover", "Розмір підковдри"),
        ("fabric type", "Тип тканини"),
        ("composition", "Склад"),
        ("density", "Щільність"),
        ("color", "Колір"),
        ("country of manufacture", "Країна виробник"),
        ("brand registration country", "Країна реєстрації бренду"),
        ("producer", "Виробник"),
        ("sheet", "Простирадло"),
        ("pillowcase", "Наволочка"),
        ("a feature of pillowcases", "Особливість наволочок"),
        ("sheet with elastic band", "Простирадло на резинці"),
        ("gift packaging", "Подарункова упаковка"),
        ("available layout options", "Доступні комплектації"),
        ("fabrics \"a\" (top of the quilt)", "Тканина A (верх ковдри)"),
        ("fabrics \"b\" (bed sheet)", "Тканина B (простирадло)"),
        ("bonus", "Бонус")
    ],
}

PROM_XLS_COLS = [
    "Код_товару",
    "Назва_позиції",
    "Назва_позиції_укр",
    "Опис",
    "Опис_укр",
    "Ціна",
    "Валюта",
    "Кількість",
    "Посилання_зображення",
    "Вага,кг",
    "Ширина,см",
    "Висота,см",
    "Довжина,см",
] + [
    col
    for i in range(1, 16)
    for col in (
        f"Назва_Характеристики_{i}",
        f"Одиниця_виміру_Характеристики_{i}",
        f"Значення_Характеристики_{i}"
    )
]


# =============================
# 🧩 Утилиты
# =============================

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Приводим имена колонок к нижнему регистру без пробелов."""
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def find_first_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def build_image_list(row: pd.Series) -> List[str]:
    urls: List[str] = []
    for col in SRC["photos"]:
        if col in row and pd.notna(row[col]):
            parts = [p.strip() for p in str(row[col]).split(",") if str(p).strip()]
            urls.extend(parts)
    return list(dict.fromkeys(urls))[:10]


# =============================
# 🔄 Утилиты с логами
# =============================

def pick_price(row: pd.Series) -> Optional[float]:
    """Пытаемся найти цену и конвертировать в float. Логируем ошибки."""
    for c in SRC["price"]:
        if c in row and pd.notna(row[c]):
            val = str(row[c]).replace(",", ".").strip()
            try:
                price = float(val)
                print(f"[DEBUG] Цена найдена в колонке '{c}': {price}")
                return price
            except ValueError:
                print(f"[DEBUG] Не удалось конвертировать '{val}' в float в колонке '{c}'")
    print("[DEBUG] Цена не найдена")
    return None


def build_image_list(row: pd.Series) -> List[str]:
    """Собираем до 10 уникальных изображений, логируем пропущенные значения."""
    urls: List[str] = []
    for col in SRC["photos"]:
        if col in row and pd.notna(row[col]):
            parts = [p.strip() for p in str(row[col]).split(",") if str(p).strip()]
            urls.extend(parts)
    urls = list(dict.fromkeys(urls))[:10]
    if not urls:
        print(f"[DEBUG] Изображения не найдены в строке с _id={row.get('id', 'неизвестно')}")
    return urls


def merge_ru_ua(df_ru: pd.DataFrame, df_ua: pd.DataFrame) -> pd.DataFrame:
    """Объединяем RU и UA с проверкой колонок и логами."""
    df_ru = normalize_df(df_ru)
    df_ua = normalize_df(df_ua)

    col_id_ru = find_first_col(df_ru, SRC["id"])
    col_id_ua = find_first_col(df_ua, SRC["id"])
    if not col_id_ru or not col_id_ua:
        print("[ERROR] Не найдены идентификаторы в одном из файлов!")
        return df_ru

    col_name_ru = find_first_col(df_ru, SRC["name"]) or "название"
    col_name_ua = find_first_col(df_ua, SRC["name"]) or "назва"

    col_descr_ru = find_first_col(df_ru, SRC["description"]) or "описание"
    col_descr_ua = find_first_col(df_ua, SRC["description"]) or "опис"

    df_ru["_id"] = df_ru[col_id_ru].astype(str).str.strip()
    df_ua["_id"] = df_ua[col_id_ua].astype(str).str.strip()

    ua = df_ua[["_id", col_name_ua, col_descr_ua]].rename(
        columns={col_name_ua: "Назва_позиції_укр", col_descr_ua: "Опис_укр"}
    )

    merged = df_ru.merge(ua, on="_id", how="left")
    merged["Назва_позиції"] = merged[col_name_ru].astype(str)
    merged["Опис"] = merged[col_descr_ru].astype(str)

    print(f"[DEBUG] Объединено {len(merged)} строк")
    return merged



# =============================
# Excel и YML
# =============================

def to_prom_excel(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []
    col_id = find_first_col(df, SRC["id"]) or "код"
    col_qty = find_first_col(df, SRC["qty"]) or "quantity"

    for _, r in df.iterrows():
        pid = str(r.get(col_id, "")).strip()
        if not pid:
            continue

        item: Dict[str, Optional[str]] = {c: "" for c in PROM_XLS_COLS}
        item["Код_товару"] = pid[:25]
        item["Назва_позиції"] = r.get("Назва_позиції", "")
        item["Назва_позиції_укр"] = r.get("Назва_позиції_укр", "")
        item["Опис"] = r.get("Опис", "")
        item["Опис_укр"] = r.get("Опис_укр", "")
        item["Ціна"] = pick_price(r) or ""
        item["Валюта"] = CURRENCY
        item["Кількість"] = r.get(col_qty, "")
        item["Посилання_зображення"] = ", ".join(build_image_list(r))

        # характеристики
        triplets: List[Tuple[str, str, str]] = []
        for eng, uk in SRC["params_primary"]:
            if eng in r and pd.notna(r[eng]):
                val = str(r[eng]).strip()
                if val:
                    triplets.append((uk, "", val))
        for i, (nm, unit, val) in enumerate(triplets[:15], start=1):
            item[f"Назва_Характеристики_{i}"] = nm
            item[f"Одиниця_виміру_Характеристики_{i}"] = unit
            item[f"Значення_Характеристики_{i}"] = val

        rows.append(item)

    return pd.DataFrame(rows, columns=PROM_XLS_COLS)


def to_prom_yml(df: pd.DataFrame) -> str:
    col_id = find_first_col(df, SRC["id"]) or "код"
    col_qty = find_first_col(df, SRC["qty"]) or "quantity"

    yml = ET.Element("yml_catalog")
    shop = ET.SubElement(yml, "shop")
    offers = ET.SubElement(shop, "offers")

    for _, r in df.iterrows():
        base_id = str(r.get(col_id, "")).strip()
        if not base_id:
            continue

        offer = ET.SubElement(offers, "offer", id=base_id, available="true")
        ET.SubElement(offer, "name").text = str(r.get("Назва_позиції", ""))
        ET.SubElement(offer, "name_ua").text = str(r.get("Назва_позиції_укр", ""))
        ET.SubElement(offer, "description").text = str(r.get("Опис", ""))
        ET.SubElement(offer, "description_ua").text = str(r.get("Опис_укр", ""))

        price = pick_price(r)
        if price:
            ET.SubElement(offer, "price").text = str(price)
        ET.SubElement(offer, "currencyId").text = CURRENCY
        qty = r.get(col_qty, None)
        if pd.notna(qty):
            ET.SubElement(offer, "quantity_in_stock").text = str(qty)

        for url in build_image_list(r):
            ET.SubElement(offer, "picture").text = url

    xml_bytes = ET.tostring(yml, encoding="utf-8")
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xml_bytes.decode("utf-8")


# =============================
# 🤖 Бот
# =============================
@dataclass
class Session:
    desired_format: Optional[str] = None
    files: List[pd.DataFrame] = None


class PromBot:
    def __init__(self, token: str):
        self.bot = Bot(token)
        self.dp = Dispatcher()
        self.sessions: Dict[int, Session] = {}
        self._register_handlers()

    def _fmt_kb(self) -> InlineKeyboardBuilder:
        kb = InlineKeyboardBuilder()
        kb.button(text="📄 XLSX", callback_data="fmt:xlsx")
        kb.button(text="🧾 YML", callback_data="fmt:yml")
        kb.adjust(2)
        return kb

    def _ensure_session(self, uid: int) -> Session:
        if uid not in self.sessions:
            self.sessions[uid] = Session(desired_format=None, files=[])
        return self.sessions[uid]

    def _register_handlers(self):
        dp = self.dp

        @dp.message(Command("start"))
        async def start(message: Message):
            sess = self._ensure_session(message.from_user.id)
            sess.files.clear()
            await message.answer(
                "🚀 Привет! Отправь два Excel-файла (RU и UA).\n"
                "Выбери формат результата:",
                reply_markup=self._fmt_kb().as_markup(),
            )

        @dp.callback_query(F.data.startswith("fmt:"))
        async def pick_format(cb: CallbackQuery):
            fmt = cb.data.split(":", 1)[1]
            sess = self._ensure_session(cb.from_user.id)
            sess.desired_format = fmt
            sess.files.clear()
            await cb.message.answer(f"✅ Формат выбран: {fmt.upper()}. Загрузи два Excel-файла (RU и UA).")
            await cb.answer()

        @dp.message(F.document)
        async def receive_file(message: Message):
            sess = self._ensure_session(message.from_user.id)
            if not sess.desired_format:
                await message.answer("⚠️ Сначала выбери формат (XLSX или YML).")
                return

            file = await self.bot.get_file(message.document.file_id)
            buf = await self.bot.download_file(file.file_path)

            try:
                df = pd.read_excel(io.BytesIO(buf.read()))
                df = normalize_df(df)
                sess.files.append(df)
            except Exception as e:
                await message.answer(f"❌ Ошибка чтения Excel: {e}")
                return

            if len(sess.files) < 2:
                await message.answer("📎 Файл получен. Жду второй Excel...")
                return

            df_ru, df_ua = sess.files
            merged = merge_ru_ua(df_ru, df_ua)

            if sess.desired_format == "xlsx":
                out_df = to_prom_excel(merged)
                out_path = f"prom_{message.from_user.id}.xlsx"
                out_df.to_excel(out_path, index=False)
                await message.answer_document(FSInputFile(out_path), caption="📄 Готово: XLSX для Prom.ua")
                os.remove(out_path)
            else:
                xml_text = to_prom_yml(merged)
                out_path = f"prom_{message.from_user.id}.yml"
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(xml_text)
                await message.answer_document(FSInputFile(out_path), caption="🧾 Готово: YML для Prom.ua")
                os.remove(out_path)

            sess.files.clear()

    async def run(self):
        print("[DEV] ✅ Bot started")
        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    bot = PromBot(TOKEN)
    asyncio.run(bot.run())
