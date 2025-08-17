import os
import io
import asyncio
import pandas as pd
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Dict, Optional
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =============================
# Настройки
# =============================
TOKEN = os.getenv("TELEGRAM_TOKEN", "8437309662:AAEpCVFP75iE23G725ydP-L_PtZRjhWC9ws")
CURRENCY = "UAH"
LOG_FILE = "bot_log.txt"
PREVIEW_ROWS = 5

SRC = {
    "id": ["product code", "main sku", "код", "артикул"],
    "name": ["name", "назва", "название"],
    "qty": ["quantity", "кількість", "количество"],
    "price": ["special price", "price", "ціна", "цена"],
    "description": ["description", "опис", "описание"],
    "photos": ["main photo", "photo1", "photo2", "photo3", "photo4",
               "photo5", "photo6", "photo7", "photo8", "photo9", "photo10",
               "посилання_зображення", "изображение"],
    "params_primary": [
        ("size of the set", "Розмір комплекту"), ("size extra", "Додатковий розмір"),
        ("sheet size", "Розмір простирадла"), ("size of the pillowcase", "Розмір наволочки"),
        ("dimensions of the duvet cover", "Розмір підковдри"), ("fabric type", "Тип тканини"),
        ("composition", "Склад"), ("density", "Щільність"), ("color", "Колір"),
        ("country of manufacture", "Країна виробник"), ("brand registration country", "Країна реєстрації бренду"),
        ("producer", "Виробник"), ("sheet", "Простирадло"), ("pillowcase", "Наволочка"),
        ("a feature of pillowcases", "Особливість наволочок"), ("sheet with elastic band", "Простирадло на резинці"),
        ("gift packaging", "Подарункова упаковка"), ("available layout options", "Доступні комплектації"),
        ("fabrics \"a\" (top of the quilt)", "Тканина A (верх ковдри)"),
        ("fabrics \"b\" (bed sheet)", "Тканина B (простирадло)"), ("bonus", "Бонус")
    ]
}

PROM_XLS_COLS = [
    "Код_товару", "Назва_позиції", "Назва_позиції_укр", "Опис", "Опис_укр",
    "Ціна", "Валюта", "Кількість", "Посилання_зображення"
] + [
    f"{prefix}_{i}" for i in range(1, 21) for prefix in (
        "Назва_Характеристики", "Одиниця_виміру_Характеристики", "Значення_Характеристики"
    )
]
# =============================
# Утилиты
# =============================
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
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
            urls.extend([p.strip() for p in str(row[col]).split(",") if p.strip()])
    return list(dict.fromkeys(urls))[:10]

def pick_price(row: pd.Series) -> Optional[float]:
    for c in SRC["price"]:
        if c in row and pd.notna(row[c]):
            try:
                return float(str(row[c]).replace(",", ".").strip())
            except Exception:
                return None
    return None

def merge_ru_ua(df_ru: pd.DataFrame, df_ua: pd.DataFrame) -> pd.DataFrame:
    df_ru = normalize_df(df_ru)
    df_ua = normalize_df(df_ua)
    col_id_ru = find_first_col(df_ru, SRC["id"]) or "код"
    col_id_ua = find_first_col(df_ua, SRC["id"]) or "код"
    col_name_ru = find_first_col(df_ru, SRC["name"]) or "название"
    col_name_ua = find_first_col(df_ua, SRC["name"]) or "назва"
    df_ru["_id"] = df_ru[col_id_ru].astype(str).str.strip()
    df_ua["_id"] = df_ua[col_id_ua].astype(str).str.strip()
    ua = df_ua[['_id', col_name_ua]].rename(columns={col_name_ua: 'Назва_позиції_укр'})
    merged = df_ru.merge(ua, on='_id', how='left')
    merged['Назва_позиції'] = merged[col_name_ru].astype(str)
    merged['Код_товару'] = merged['_id']
    return merged

def to_prom_excel(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []
    col_qty = find_first_col(df, SRC['qty']) or 'quantity'
    for _, r in df.iterrows():
        item: Dict[str, Optional[str]] = {c: '' for c in PROM_XLS_COLS}
        item['Код_товару'] = r.get('Код_товару', '')
        item['Назва_позиції'] = r.get('Назва_позиції', '')
        item['Назва_позиції_укр'] = r.get('Назва_позиції_укр', '')
        item['Ціна'] = pick_price(r) or ''
        item['Валюта'] = CURRENCY
        item['Кількість'] = r.get(col_qty, '')
        item['Посилання_зображення'] = ', '.join(build_image_list(r))
        triplets: List[tuple] = []
        for eng, uk in SRC['params_primary']:
            if eng in r and pd.notna(r[eng]):
                val = str(r[eng]).strip()
                if val:
                    triplets.append((uk, '', val))
        for i, (nm, unit, val) in enumerate(triplets, start=1):
            item[f'Назва_Характеристики_{i}'] = nm
            item[f'Одиниця_виміру_Характеристики_{i}'] = unit
            item[f'Значення_Характеристики_{i}'] = val
        rows.append(item)
    return pd.DataFrame(rows)

def to_prom_yml(df: pd.DataFrame) -> str:
    def xml_safe(val: str) -> str:
        return str(val).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    col_qty = find_first_col(df, SRC['qty']) or 'quantity'
    yml = ET.Element('yml_catalog')
    shop = ET.SubElement(yml, 'shop')
    offers = ET.SubElement(shop, 'offers')
    for _, r in df.iterrows():
        base_id = str(r.get('Код_товару', '')).strip()
        if not base_id: continue
        offer = ET.SubElement(offers, 'offer', id=base_id, available='true')
        name = r.get('Назва_позиції', '')
        name_ua = r.get('Назва_позиції_укр', '')
        price = pick_price(r)
        qty = r.get(col_qty, None)
        images = build_image_list(r)
        if name: ET.SubElement(offer, 'name').text = xml_safe(name)
        if name_ua: ET.SubElement(offer, 'name_ua').text = xml_safe(name_ua)
        if price is not None: ET.SubElement(offer, 'price').text = str(price)
        ET.SubElement(offer, 'currencyId').text = CURRENCY
        if pd.notna(qty): ET.SubElement(offer, 'quantity_in_stock').text = str(qty)
        for url in images: ET.SubElement(offer, 'picture').text = xml_safe(url)
        for eng, uk in SRC['params_primary']:
            if eng in r and pd.notna(r[eng]):
                val = str(r[eng]).strip()
                if val: ET.SubElement(offer, 'param', name=uk).text = xml_safe(val)
    xml_bytes = ET.tostring(yml, encoding='utf-8')
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_bytes.decode('utf-8')

def log_error(msg: str):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')
    print('[LOG]', msg)

@dataclass
class Session:
    desired_format: Optional[str] = None
    files: List[pd.DataFrame] = None
# =============================
# Бот
# =============================
class PromBot:
    def __init__(self, token: str):
        self.bot = Bot(token)
        self.dp = Dispatcher()
        self.sessions: Dict[int, Session] = {}
        self._register_handlers()

    def _fmt_kb(self) -> InlineKeyboardBuilder:
        kb = InlineKeyboardBuilder()
        kb.button(text='📄 XLSX', callback_data='fmt:xlsx')
        kb.button(text='🧾 YML', callback_data='fmt:yml')
        kb.adjust(2)
        return kb

    def _ensure_session(self, uid: int) -> Session:
        if uid not in self.sessions:
            self.sessions[uid] = Session(desired_format=None, files=[])
        return self.sessions[uid]

    def _register_handlers(self):
        dp = self.dp

        # === Основные команды ===
        @dp.message(Command('start'))
        async def start(message: Message):
            sess = self._ensure_session(message.from_user.id)
            sess.files.clear()
            await message.answer(
                '🚀 Привет! Отправь два Excel-файла (RU и UA).\nВыбери формат результата:',
                reply_markup=self._fmt_kb().as_markup()
            )

        @dp.message(Command('info'))
        async def info(message: Message):
            await message.answer(
                'ℹ️ Бот позволяет объединять файлы RU/UA, генерировать XLSX/YML с характеристиками, фото и ценой.\n'
                'Команды: /start, /info, /clear, /preview, /validate, /onlyphoto, /onlyprice, /infoaboutparsing, /showlog, /stop'
            )

        @dp.message(Command('clear'))
        async def clear_files(message: Message):
            sess = self._ensure_session(message.from_user.id)
            sess.files.clear()
            await message.answer('🗑️ Старые файлы очищены.')

        @dp.message(Command('preview'))
        async def preview(message: Message):
            sess = self._ensure_session(message.from_user.id)
            if not sess.files:
                await message.answer('⚠️ Файлы ещё не загружены.')
                return
            previews = []
            for i, df in enumerate(sess.files, start=1):
                previews.append(f'Файл {i}:\n{df.head(PREVIEW_ROWS).to_string()}')
            await message.answer('\n\n'.join(previews))

        @dp.message(Command('validate'))
        async def validate(message: Message):
            sess = self._ensure_session(message.from_user.id)
            missing = []
            required_cols = ['id', 'name', 'price']
            for df in sess.files:
                for col in required_cols:
                    if not find_first_col(df, SRC[col]):
                        missing.append(col)
            if missing:
                await message.answer(f'⚠️ Отсутствуют колонки: {set(missing)}')
            else:
                await message.answer('✅ Все необходимые колонки присутствуют.')

        @dp.message(Command('onlyphoto'))
        async def onlyphoto(message: Message):
            sess = self._ensure_session(message.from_user.id)
            if not sess.files:
                await message.answer('⚠️ Файлы ещё не загружены.')
                return
            df = pd.concat(sess.files)
            out_df = df[['Код_товару'] + SRC['photos'][:10]]
            out_path = f'photos_{message.from_user.id}.xlsx'
            out_df.to_excel(out_path, index=False)
            await message.answer_document(FSInputFile(out_path), caption='📸 Только фото')
            os.remove(out_path)

        @dp.message(Command('onlyprice'))
        async def onlyprice(message: Message):
            sess = self._ensure_session(message.from_user.id)
            if not sess.files:
                await message.answer('⚠️ Файлы ещё не загружены.')
                return
            df = pd.concat(sess.files)
            out_df = df[['Код_товару']]
            out_df['Цена'] = df.apply(pick_price, axis=1)
            out_path = f'price_{message.from_user.id}.xlsx'
            out_df.to_excel(out_path, index=False)
            await message.answer_document(FSInputFile(out_path), caption='💰 Только цены')
            os.remove(out_path)

        @dp.message(Command('infoaboutparsing'))
        async def infoaboutparsing(message: Message):
            sess = self._ensure_session(message.from_user.id)
            total_rows = sum(len(df) for df in sess.files) if sess.files else 0
            await message.answer(f'📊 Всего позиций в текущих файлах: {total_rows}')

        @dp.message(Command('showlog'))
        async def showlog(message: Message):
            if not os.path.exists(LOG_FILE):
                await message.answer('ℹ️ Лог-файл пуст.')
                return
            with open(LOG_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
            await message.answer(f'📝 Лог:\n{content[-2000:]}')

        @dp.message(Command('stop'))
        async def stop(message: Message):
            await message.answer('⚠️ Остановка бота (только для разработчика)')
            await self.bot.session.close()
            exit(0)

        # === Обработка выбора формата ===
        @dp.callback_query(F.data.startswith('fmt:'))
        async def pick_format(cb: CallbackQuery):
            fmt = cb.data.split(':', 1)[1]
            sess = self._ensure_session(cb.from_user.id)
            sess.desired_format = fmt
            sess.files.clear()
            await cb.message.answer(f'✅ Формат выбран: {fmt.upper()}. Загрузи два Excel-файла (RU и UA).')
            await cb.answer()

        # === Обработка загрузки файлов ===
        @dp.message(F.document)
        async def receive_file(message: Message):
            sess = self._ensure_session(message.from_user.id)
            if not sess.desired_format:
                await message.answer('⚠️ Сначала выбери формат (XLSX или YML).')
                return
            try:
                file = await self.bot.get_file(message.document.file_id)
                buf = await self.bot.download_file(file.file_path)
                df = pd.read_excel(io.BytesIO(buf.read()))
                df = normalize_df(df)
                sess.files.append(df)
                await message.answer(f'📎 Файл получен. Всего файлов: {len(sess.files)}')
            except Exception as e:
                await message.answer(f'❌ Ошибка чтения Excel: {e}')
                log_error(f'Ошибка чтения Excel для {message.from_user.id}: {e}')
                return
            if len(sess.files) < 2:
                await message.answer('⏳ Жду второй файл...')
                return
            try:
                df_ru, df_ua = sess.files[:2]
                merged = merge_ru_ua(df_ru, df_ua)
                if sess.desired_format == 'xlsx':
                    out_df = to_prom_excel(merged)
                    out_path = f'prom_{message.from_user.id}.xlsx'
                    out_df.to_excel(out_path, index=False)
                    await message.answer_document(FSInputFile(out_path), caption='📄 XLSX готово')
                else:
                    xml_text = to_prom_yml(merged)
                    out_path = f'prom_{message.from_user.id}.yml'
                    with open(out_path, 'w', encoding='utf-8') as f:
                        f.write(xml_text)
                    await message.answer_document(FSInputFile(out_path), caption='🧾 YML готово')
                sess.files.clear()
                os.remove(out_path)
            except Exception as e:
                await message.answer(f'❌ Ошибка генерации файла: {e}')
                log_error(f'Ошибка генерации файла для {message.from_user.id}: {e}')

    def run(self):
        asyncio.run(self.dp.start_polling(self.bot))
if __name__ == '__main__':
    bot = PromBot(TOKEN)
    bot.run()
