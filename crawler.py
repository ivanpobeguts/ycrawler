import os

import aiohttp
import asyncio
import logging
from collections import namedtuple

from bs4 import BeautifulSoup

import async_timeout

BASE_URL = 'https://news.ycombinator.com/'
COMMENTS_URL_TEMPLATE = 'https://news.ycombinator.com/item?id='
MAX_PAGES_COUNT = 30
BASE_DIR = 'articles'

Response = namedtuple("Response", "status, content")
Article = namedtuple('Article', 'link, id')


async def fetch_main_html(session, url):
    try:
        async with session.get(url) as response:
            try:
                response.raise_for_status()
            except aiohttp.ClientResponseError:
                logging.info(f'Client response error: {url}')
            except asyncio.TimeoutError:
                pass
            if 'text/html' in response.headers['Content-Type']:
                html = await response.read()
                return Response(response.status, html)
            else:
                logging.info(f'Not html: {url}')
                return
    except aiohttp.ClientConnectorError:
        logging.info(f'Client connection error: {url}')


def save_page(html, base_dir, object_id):
    article_path = os.path.join(base_dir, object_id)
    if not os.path.exists(article_path):
        os.makedirs(article_path)
    with open(os.path.join(article_path, 'article.html'), 'wb') as f:
        f.write(html)


async def handle_article(link, id, session):
    with async_timeout.timeout(10):
        result = await fetch_main_html(session, link)
        if result:
            logging.info(result.status)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                save_page,
                result.content, BASE_DIR, id
            )
        return result


def get_articles(html):
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('tr', class_='athing')
    for row in rows:
        a = row.find('a', class_='storylink')
        yield Article(a['href'], row['id'])


async def download_main_page():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        response = await fetch_main_html(session, BASE_URL)
        if response:
            logging.info('Status {}'.format(response.status))
            articles = list(set(get_articles(response.content)))
            tasks = [handle_article(article.link, article.id, session) for article in articles]
            await asyncio.gather(*tasks)
            logging.info("DONE")


def main():
    asyncio.run(download_main_page())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    main()
