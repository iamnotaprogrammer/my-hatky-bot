import asyncio
import logging
import random

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from const import undegrounds
from settings import cian_bot, chat_id

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(filename)-20s %(message)s')

body = {"foot_min": {"type": "range", "value": {"lte": 10}}, "_type": "flatrent", "room": {"type": "terms", "value": [1, 2, 9]},
        "has_fridge": {"type": "term", "value": True}, "for_day": {"type": "term", "value": "!1"},
        "price": {"type": "range", "value": {"lte": 41000}}, "only_foot": {"type": "term", "value": "2"},
        "engine_version": {"type": "term", "value": 2}, "has_washer": {"type": "term", "value": True},
        "currency": {"type": "term", "value": 2}, "has_kitchen_furniture": {"type": "term", "value": True},
        "wp": {"type": "term", "value": True}, "publish_period": {"type": "term", "value": 600},
        "has_furniture": {"type": "term", "value": True}, "demolished_in_moscow_programm": {"type": "term", "value": False},
        "geo": undegrounds, "region": {"type": "terms", "value": [1]}, "repair": {"type": "terms", "value": [3, 2]}}


async def send_to_telegram_message(bot_token, chat_id, message):
    TELEGRAM_API_URL = "https://api.telegram.org/bot%s/sendMessage" % bot_token

    retry = 0
    if isinstance(message, dict):
        message = '\n'.join("{}: {}".format(k, v) for k, v in message.items())
    logging.info(message)
    async with aiohttp.ClientSession() as session:
        while retry <= 10:
            async with session.get(TELEGRAM_API_URL, params={"chat_id": chat_id, "text": message}) as resp:
                logging.info('telegram response status {} {} '.format(resp.status, await resp.text()))
                if resp.status == 200:
                    return True
            retry += 1
            await asyncio.sleep(random.randint(0, 10))


async def get_offers(proxy=None):
    url = 'https://www.cian.ru/cian-api/site/v1/search-offers/'

    try:
        async with aiohttp.ClientSession(read_timeout=600, conn_timeout=600) as session:
            async with session.post(url, json=body, proxy=proxy) as resp:
                if resp.status == 200:
                    return await resp.json()
    except (ClientResponseError, aiohttp.client_exceptions.ClientOSError):
        logging.error('cian block')
    except aiohttp.client_exceptions.ServerDisconnectedError:
        logging.error('proxy bad')
    else:
        await asyncio.sleep(random.randint(5, 20))


async def _main():
    offers = [el['fullUrl'] for el in (await get_offers())['data']['offersSerialized']]
    logging.info('count offers %s ' % len(offers))
    for el in offers:
        await send_to_telegram_message(bot_token=cian_bot, chat_id=chat_id, message=el)
    while True:
        async for proxy in get_proxy():
            logging.debug(proxy)
            res = await get_offers(proxy)
            if not res:
                continue
            new_offers = res['data']['offersSerialized']
            new_offers_url = [el['fullUrl'] for el in new_offers if el['fullUrl'] not in offers]
            logging.info('count offers %s ' % len(new_offers_url))
            offers.extend(new_offers_url)
            if new_offers_url:
                await send_to_telegram_message(bot_token=cian_bot, chat_id=chat_id, message='Новые горячие хатки: ')
            for url in new_offers_url:
                await send_to_telegram_message(bot_token=cian_bot, chat_id=chat_id, message=url)

            await asyncio.sleep(random.randint(30, 90))


async def get_proxy():
    while True:
        url = 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list.txt'
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    res = await resp.text()
                    for proxy_line in res.split('\n\n')[1].split('\n'):
                        proxy = proxy_line.split(' ')[0]
                        yield 'http://' + proxy


def main():
    logging.info('start')
    loop = asyncio.get_event_loop()
    loop.create_task(_main())
    loop.run_forever()


if __name__ == '__main__':
    main()