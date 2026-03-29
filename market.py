import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из магазина на Яндекс Маркете.

    Запрашивает у API Яндекс Маркета очередную порцию товаров (до 200 штук)
    для последующей обработки. Используется для постраничной навигации
    по каталогу товаров продавца.

    Args:
        page: Токен страницы для пагинации. Пустая строка для первого запроса.
        campaign_id: Идентификатор кампании (магазина) в Яндекс Маркете.
        access_token: OAuth-токен для авторизации запросов к API.

    Returns:
        Словарь с результатами запроса, содержащий ключи:
        - 'offerMappingEntries': список товаров
        - 'paging': информация о пагинации (nextPageToken)

    Examples:
        Корректное использование:
        >>> get_product_list("", "12345", "abc-token")
        {'offerMappingEntries': [...], 'paging': {'nextPageToken': 'xyz'}}

        Некорректное использование:
        >>> get_product_list(None, None, None)  # None вместо строк
        # Вызовет ошибку авторизации или валидации данных
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров в магазине на Яндекс Маркете.

    Отправляет в API Яндекс Маркета список товаров с новым количеством на складе.
    Поддерживает пакетную отправку до 2000 товаров за один запрос.

    Args:
        stocks: Список словарей с данными об остатках. Каждый словарь содержит:
            - 'sku': артикул товара
            - 'warehouseId': идентификатор склада
            - 'items': список с количеством и датой обновления
        campaign_id: Идентификатор кампании (магазина) в Яндекс Маркете.
        access_token: OAuth-токен для авторизации запросов к API.

    Returns:
        Словарь с результатом операции от API Яндекс Маркета.

    Examples:
        Корректное использование:
        >>> stocks = [{'sku': '123', 'warehouseId': '1', 'items': [...]}]
        >>> update_stocks(stocks, "12345", "abc-token")
        {'result': {...}}

        Некорректное использование:
        >>> update_stocks([{'sku': '123', 'warehouseId': '1', 'items': [{'count': -5}]}], "12345", "abc-token")
        # Отрицательное количество вызовет ошибку валидации
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров в магазине на Яндекс Маркете.

    Отправляет в API Яндекс Маркета список товаров с новыми ценами для обновления.
    Поддерживает пакетную отправку до 500 товаров за один запрос.

    Args:
        prices: Список словарей с данными о ценах. Каждый словарь содержит:
            - 'id': артикул товара
            - 'price': объект с ценой и валютой
        campaign_id: Идентификатор кампании (магазина) в Яндекс Маркете.
        access_token: OAuth-токен для авторизации запросов к API.

    Returns:
        Словарь с результатом операции от API Яндекс Маркета.

    Examples:
        Корректное использование:
        >>> prices = [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
        >>> update_price(prices, "12345", "abc-token")
        {'result': {...}}

        Некорректное использование:
        >>> update_price([], "12345", "abc-token")  # Пустой список
        # Запрос пройдёт, но ничего не обновится
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает все артикулы (shopSku) товаров магазина на Яндекс Маркете.

    Последовательно запрашивает все страницы товаров из магазина
    и извлекает из них уникальные идентификаторы предложений (shopSku).
    Используется для сверки с файлом поставщика.

    Args:
        campaign_id: Идентификатор кампании (магазина) в Яндекс Маркете.
        market_token: OAuth-токен для авторизации запросов к API.

    Returns:
        Список строк с артикулами товаров (shopSku).
        Например: ['12345', '67890', '11111']

    Examples:
        Корректное использование:
        >>> get_offer_ids("12345", "abc-token")
        ['12345', '67890', '11111']

        Некорректное использование:
        >>> get_offer_ids("", "")  # Пустые учётные данные
        # Вызовет ошибку авторизации API
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создаёт список остатков для загрузки на Яндекс Маркет.

    Сравнивает товары из файла поставщика с товарами в магазине Яндекс Маркета.
    Для совпадающих товаров берёт количество из файла, для отсутствующих
    в файле — устанавливает остаток 0. Добавляет временную метку обновления.

    Args:
        watch_remnants: Список словарей с данными из файла поставщика.
            Каждый словарь содержит ключи 'Код' и 'Количество'.
        offer_ids: Список артикулов товаров, загруженных в магазин Яндекс Маркета.
            Используется для фильтрации и поиска отсутствующих товаров.
        warehouse_id: Идентификатор склада для привязки остатков.

    Returns:
        Список словарей для API Яндекс Маркета с ключами:
        - 'sku': артикул товара
        - 'warehouseId': идентификатор склада
        - 'items': список с количеством, типом и датой обновления

    Examples:
        Корректное использование:
        >>> watch_remnants = [{'Код': '123', 'Количество': '50'}]
        >>> offer_ids = ['123', '456']
        >>> create_stocks(watch_remnants, offer_ids, "1")
        [{'sku': '123', 'warehouseId': '1', 'items': [...]}, ...]

        Некорректное использование:
        >>> create_stocks([], ['123'], "1")  # Пустой файл поставщика
        # Всем товарам будет установлен остаток 0
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создаёт список цен для загрузки на Яндекс Маркет.

    Формирует структуру данных с ценами для товаров, которые есть
    и в файле поставщика, и в магазине Яндекс Маркета.

    Args:
        watch_remnants: Список словарей с данными из файла поставщика.
            Каждый словарь содержит ключи 'Код' и 'Цена'.
        offer_ids: Список артикулов товаров, загруженных в магазин Яндекс Маркета.
            Используется для фильтрации товаров.

    Returns:
        Список словарей для API Яндекс Маркета с ключами:
        - 'id': артикул товара
        - 'price': объект с ценой (value) и валютой (currencyId)

    Examples:
        Корректное использование:
        >>> watch_remnants = [{'Код': '123', 'Цена': "5'990.00 руб."}]
        >>> offer_ids = ['123']
        >>> create_prices(watch_remnants, offer_ids)
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

        Некорректное использование:
        >>> create_prices([], ['123'])  # Пустой файл поставщика
        # Вернёт пустой список, цены не обновятся
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загружает обновлённые цены товаров на Яндекс Маркет.

    Получает список товаров из магазина, создаёт структуру с новыми ценами
    из файла поставщика и отправляет данные в API Яндекс Маркета пакетно.

    Args:
        watch_remnants: Список словарей с данными из файла поставщика.
            Каждый словарь содержит ключи 'Код' и 'Цена'.
        campaign_id: Идентификатор кампании (магазина) в Яндекс Маркете.
        market_token: OAuth-токен для авторизации запросов к API.

    Returns:
        Список словарей с данными о ценах, которые были отправлены на Яндекс Маркет.

    Examples:
        Корректное использование:
        >>> watch_remnants = [{'Код': '123', 'Цена': "5'990.00 руб."}]
        >>> await upload_prices(watch_remnants, "12345", "abc-token")
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

        Некорректное использование:
        >>> await upload_prices([], "12345", "abc-token")  # Пустые данные
        # Вернёт пустой список, цены не обновятся
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружает обновлённые остатки товаров на Яндекс Маркет.

    Получает список товаров из магазина, создаёт структуру с новыми остатками
    из файла поставщика и отправляет данные в API Яндекс Маркета пакетно.

    Args:
        watch_remnants: Список словарей с данными из файла поставщика.
            Каждый словарь содержит ключи 'Код' и 'Количество'.
        campaign_id: Идентификатор кампании (магазина) в Яндекс Маркете.
        market_token: OAuth-токен для авторизации запросов к API.
        warehouse_id: Идентификатор склада для привязки остатков.

    Returns:
        Кортеж из двух списков:
        - Список товаров с ненулевыми остатками
        - Полный список всех товаров с остатками

    Examples:
        Корректное использование:
        >>> watch_remnants = [{'Код': '123', 'Количество': '50'}]
        >>> await upload_stocks(watch_remnants, "12345", "abc-token", "1")
        ([{'sku': '123', 'warehouseId': '1', 'items': [...]}], [...])

        Некорректное использование:
        >>> await upload_stocks([], "12345", "abc-token", "1")  # Пустые данные
        # Всем товарам будет установлен остаток 0
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Запускает процесс синхронизации цен и остатков с Яндекс Маркетом.

    Основная точка входа в программу. Получает учётные данные из
    переменных окружения, скачивает файл поставщика и обновляет
    цены и остатки в магазинах на Яндекс Маркете (FBS и DBS схемы).

    Обрабатывает основные ошибки сети и доступа к API.

    Examples:
        Корректное использование:
        >>> main()  # Запускается из командной строки
        # Синхронизирует цены и остатки для FBS и DBS

        Некорректное использование:
        # При отсутствии переменных окружения MARKET_TOKEN, FBS_ID и других
        # функция вызовет исключение environs.exceptions.EnvValidationException
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()