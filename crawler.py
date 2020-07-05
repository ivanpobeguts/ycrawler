import aiohttp
import asyncio
import logging
from collections import namedtuple

from bs4 import BeautifulSoup

import async_timeout

BASE_URL = 'https://news.ycombinator.com/'
COMMENTS_URL_TEMPLATE = 'https://news.ycombinator.com/item?id='
MAX_PAGES_COUNT = 30

Response = namedtuple("Response", "status, content")
Article = namedtuple('Article', 'link, id')


async def get_html(session, url):
    async with session.get(url) as response:
        try:
            response.raise_for_status()
        except aiohttp.ClientResponseError:
            pass
        if 'text/html' in response.headers['Content-Type']:
            html = await response.read()
            return Response(response.status, html)
        else:
            logging.info(f'Not html: {url}')
            return


async def fetch(url, session):
    try:
        response = await get_html(session, url)
        if response and response.status == 200:
            return response
    except Exception as e:
        logging.info(e)


async def handle_article(link, id, session):
    with async_timeout.timeout(5):
        result = await fetch(link, session)
        if result:
            logging.info(result.status)
        return result


def get_articles(html):
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('tr', class_='athing')
    for row in rows:
        a = row.find('a', class_='storylink')
        yield Article(a['href'], row['id'])


async def download_main_page():
    async with aiohttp.ClientSession() as session:
        response = await fetch(BASE_URL, session)
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
