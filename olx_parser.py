from bs4 import BeautifulSoup
import aiohttp
import json
import asyncio

import sqlite3

from typing import Dict, List, Union


OLX_HOST = "https://www.olx.uz"
BASE_URL = f"{OLX_HOST}/nedvizhimost/kvartiry/prodazha/tashkent/?currency=USD"
CARD_CLASS_ID = "css-rc5s2u"
PROPERTIES_CLASS_ID = "css-1r0si1e"
PRICE_CLASS_ID = "css-47bkj9"
IMAGES_CLASS_ID = "css-1bmvjcs"

ORDER_BY_PRICE_DESC = "ORDER_BY_PRICE_DESC"
ORDER_BY_PRICE_ASC = "ORDER_BY_PRICE_ASC"
ORDER_BY_NEW = "ORDER_BY_NEW"
ORDER_BY_RELEVANCE = "ORDER_BY_RELEVANCE"

REPAIR_TYPE_CUSTOM = "REPAIR_TYPE_CUSTOM"
REPAIR_TYPE_EURO = "REPAIR_TYPE_EURO"
REPAIR_TYPE_AVERAGE = "REPAIR_TYPE_AVERAGE"
REPAIR_TYPE_TO_BE_RENOVATED = "REPAIR_TYPE_TO_BE_RENOVATED"
REPAIR_TYPE_ROUGH_FINISH = "REPAIR_TYPE_ROUGH_FINISH"
REPAIR_TYPE_WHITEBOX = "REPAIR_TYPE_WHITEBOX"

MARKET_TYPE_PRIMARY = "MARKET_TYPE_PRIMARY"
MARKET_TYPE_SECONDARY = "MARKET_TYPE_SECONDARY"

BUILDING_TYPE_PANEL = "BUILDING_TYPE_PANEL"
BUILDING_TYPE_BRICK = "BUILDING_TYPE_BRICK"
BUILDING_TYPE_MONOLITH = "BUILDING_TYPE_MONOLITH"
BUILDING_TYPE_BLOCK = "BUILDING_TYPE_BLOCK"
BUILDING_TYPE_WOOD = "BUILDING_TYPE_WOOD"


QUERY_PARAMS = {
    ORDER_BY_PRICE_DESC: "filter_float_price:desc",
    ORDER_BY_PRICE_ASC: "filter_float_price:asc",
    ORDER_BY_NEW: "created_at:desc",
    ORDER_BY_RELEVANCE: "relevance:desc",
    REPAIR_TYPE_CUSTOM: 1,
    REPAIR_TYPE_EURO: 2,
    REPAIR_TYPE_AVERAGE: 3,
    REPAIR_TYPE_TO_BE_RENOVATED: 4,
    REPAIR_TYPE_ROUGH_FINISH: 5,
    REPAIR_TYPE_WHITEBOX: 6,
    MARKET_TYPE_PRIMARY: "primary",
    MARKET_TYPE_SECONDARY: "secondary",
}


FILTERS_QUERY_KEYS = {
    "order": "search[order]",
    "price_from": "search[filter_float_price:from]",
    "price_to": "search[filter_float_price:to]",
    "rooms_from": "search[filter_float_number_of_rooms:from]",
    "rooms_to": "search[filter_float_number_of_rooms:to]",
    "is_furnished": "search[filter_enum_furnished]",
    "market_type": "search[filter_enum_type_of_market]",
    "repair_status": "search[filter_enum_repairs]",
    "area_from": "search[filter_float_total_area:from]",
    "area_to": "search[filter_float_total_area:to]",
    "is_commissioned": "search[filter_enum_comission]",
    "floor_from": "search[filter_float_floor:from]",
    "floor_to": "search[filter_float_floor:to]",
}

COMPLEX_QUERY_KEYS = {"order", "market_type", "repair_status"}


CUSTOM_FIELD_ROOMS = "Количество комнат"
CUSTOM_FIELD_MARKET_TYPE = "Тип жилья"
CUSTOM_FIELD_AREA = "Общая площадь"
CUSTOM_FIELD_FLOOR = "Этаж"
CUSTOM_FIELD_LAST_FLOOR = "Этажность дома"
CUSTOM_FIELD_BUILDING_TYPE = "Тип строения"
CUSTOM_FIELD_SANITARY_UNIT = "Санузел"
CUSTOM_FIELD_IS_FURNISHED = "Меблирована"
CUSTOM_FIELD_REPAIR_TYPE = "Ремонт"
CUSTOM_FIELD_AD_TYPE = "Тип"
CUSTOM_FIELD_PRICE = "Цена"
CUSTOM_FIELD_KITCHEN_AREA = "Площадь кухни"
CUSTOM_FIELD_ARCHITECTURE = "Планировка"
CUSTOM_FIELD_OPERATION_YEAR = "Год постройки/сдачи"
CUSTOM_FIELD_IS_FIRST_FLOOR = "Первый этаж"
CUSTOM_FIELD_IS_LAST_FLOOR = "Последний этаж"

CUSTOM_FILTERS = [
    CUSTOM_FIELD_BUILDING_TYPE,
    CUSTOM_FIELD_IS_FIRST_FLOOR,
    CUSTOM_FIELD_IS_LAST_FLOOR,
]

yes_no_mapping = {"yes": "Да", "no": "Нет"}

building_type_mapping = {
    "BUILDING_TYPE_PANEL": "Панельный",
    "BUILDING_TYPE_BRICK": "Кирпичный",
    "BUILDING_TYPE_MONOLITH": "Монолитный",
    "BUILDING_TYPE_BLOCK": "Блочный",
    "BUILDING_TYPE_WOOD": "Деревянный",
}


def setup_database():
    conn = sqlite3.connect("olx.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            card JSON DEFAULT NULL
        )
    """
    )

    conn.commit()
    conn.close()


def build_url(base_url: str, filters: Dict[str, Union[str, int, List[str]]]) -> str:
    for key, value in filters.items():
        if value is None:
            continue
        query_key = FILTERS_QUERY_KEYS.get(key)
        if query_key is None:
            continue
        if key in COMPLEX_QUERY_KEYS:
            if isinstance(value, list):
                for idx, item in enumerate(value):
                    query_value = QUERY_PARAMS[item]
                    base_url += f"&{query_key}[{idx}]={query_value}"
            else:
                query_value = QUERY_PARAMS[value]
                base_url += f"&{query_key}={query_value}"
        else:
            if isinstance(value, list):
                for idx, item in enumerate(value):
                    base_url += f"&{query_key}[{idx}]={item}"
            else:
                base_url += f"&{query_key}={value}"
    return base_url


async def get_olx_page(session, p, filters=None):
    base_url = BASE_URL

    if filters is not None:
        base_url = build_url(base_url, filters)

    url = f"{base_url}&page={p}"
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, "lxml")
        items = soup.find_all(class_=CARD_CLASS_ID)
        return [OLX_HOST + c["href"] for c in items]


async def get_olx_pages(pages=5, filters=None):
    cards = []
    async with aiohttp.ClientSession() as session:
        tasks = [get_olx_page(session, p, filters) for p in range(1, pages + 1)]
        results = await asyncio.gather(*tasks)
        for result in results:
            cards.extend(result)
    conn = sqlite3.connect("olx.db")
    cursor = conn.cursor()
    cursor.execute("BEGIN TRANSACTION")
    for card in cards:
        cursor.execute("INSERT OR IGNORE INTO cards (url) VALUES (?)", (card,))
    cursor.execute("COMMIT")
    conn.close()
    return cards


def process_props(props):
    props_dict = {}
    for p in props[1:]:
        k, v = p.text.split(":")
        props_dict[k] = v.strip()
    props_dict["Тип"] = props[0].text
    return props_dict


async def get_card_metadata(session, url_path: str):
    card_metadata = {}
    async with session.get(url_path) as response:
        text = await response.text()
        soup = BeautifulSoup(text, "lxml")
        props = soup.find_all(class_=PROPERTIES_CLASS_ID)
        card_metadata = card_metadata | process_props(props)
        price = soup.find(class_=PRICE_CLASS_ID)
        card_metadata["Цена"] = price.text if price else None
        images = soup.find_all(class_=IMAGES_CLASS_ID)
        card_metadata["Фото"] = [i["src"] for i in images]

    return card_metadata


async def get_cards_metadata(cards):
    metadata = []
    conn = sqlite3.connect("olx.db")
    cursor = conn.cursor()
    cursor.execute("BEGIN TRANSACTION")
    new_cards = []
    for card in cards:
        cursor.execute(
            "SELECT COUNT(*) FROM cards WHERE url = ? AND card IS NOT NULL",
            (card,),
        )
        count = cursor.fetchone()[0]
        if count == 0:
            new_cards.append(card)
        else:
            cursor.execute(
                "SELECT card FROM cards WHERE url = ?",
                (card,),
            )
            data = cursor.fetchone()[0]
            metadata.append(data)

    async with aiohttp.ClientSession() as session:
        tasks = [get_card_metadata(session, c) for c in new_cards]
        new_metadata = await asyncio.gather(*tasks)
        metadata.extend(new_metadata)

    for card, data in zip(new_cards, new_metadata):
        cursor.execute(
            "UPDATE cards SET card = ? WHERE url = ?", (json.dumps(data), card)
        ),

    cursor.execute("COMMIT")
    conn.close()
    return metadata


def add_custom_fields(cards):
    new_cards = []
    for card in cards:
        if isinstance(card, str):
            card = json.loads(card)
        new_cards.append(card.copy())
    for c in new_cards:
        floor = c.get(CUSTOM_FIELD_FLOOR)
        last_floor = c.get(CUSTOM_FIELD_LAST_FLOOR)
        c[CUSTOM_FIELD_IS_FIRST_FLOOR] = "Да" if int(floor) == 1 else "Нет"
        if floor is not None and last_floor is not None:
            c[CUSTOM_FIELD_IS_LAST_FLOOR] = "Да" if floor == last_floor else "Нет"
    return new_cards


def apply_custom_filters(
    cards, is_first_floor="no", is_last_floor="no", btype=BUILDING_TYPE_BRICK
):
    filtered_cards = []
    for card in cards:
        first_floor = card.get(CUSTOM_FIELD_IS_FIRST_FLOOR)
        last_floor = card.get(CUSTOM_FIELD_IS_LAST_FLOOR)
        building_type = card.get(CUSTOM_FIELD_BUILDING_TYPE)
        if (
            (first_floor == yes_no_mapping[is_first_floor] or first_floor is None)
            and (last_floor == yes_no_mapping[is_last_floor] or last_floor is None)
            and (building_type == building_type_mapping[btype] or building_type is None)
        ):
            filtered_cards.append(card)
    return filtered_cards
