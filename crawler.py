import os
import random

import aiohttp
import asyncio
import logging
from collections import namedtuple
from uuid import uuid4

from bs4 import BeautifulSoup

BASE_URL = 'https://news.ycombinator.com/'
COMMENTS_URL_TEMPLATE = ''.join([BASE_URL, 'item?id='])
BASE_DIR = 'articles'
USER_AGENTS_LIST = [
    # Chrome Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
    # Chrome MacOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
    # Chrome Linux
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
    # Firefox Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0',
    # Firefox MacOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:75.0) Gecko/20100101 Firefox/75.0',
    # Firefox Linux
    'Mozilla/5.0 (X11; Linux i686; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Mozilla/5.0 (Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0',
    # Safari MacOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36'
]

Response = namedtuple('Response', 'status, content')
Article = namedtuple('Article', 'link, id')


async def fetch(url, session):
    async with session.get(url) as response:
        try:
            if 'text/html' in response.headers['Content-Type']:
                html = await response.read()
                return Response(response.status, html)
            else:
                logging.info(f'Not html: {url}')
                return
        except KeyError:
            logging.info(f'Content-Type header not found: {url}')
            return


async def get_html(url):
    headers = {'User-Agent': random.choice(USER_AGENTS_LIST)}
    semaphore = asyncio.Semaphore(1)
    async with semaphore:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), headers=headers) as session:
            attempts = 0
            while True:
                try:
                    return await fetch(url, session)
                except asyncio.TimeoutError:
                    if attempts < 3:
                        attempts += 1
                        await asyncio.sleep(1)
                        logging.info(f'Reconnect #{attempts}. URL: {url}.')
                    else:
                        logging.info(f'Connection timeout: {url}')
                        return
                except aiohttp.client_exceptions.ClientConnectorError:
                    logging.info(f'Could not connect to: {url}')
                    return


def save_page(html, path, file_name):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(os.path.join(path, file_name), 'wb') as f:
        f.write(html)


async def handle_article(link, id):
    result = await get_html(link)
    if result:
        logging.info(result.status)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            save_page,
            result.content, os.path.join(BASE_DIR, id), 'article.html'
        )
        result = await get_html(''.join([COMMENTS_URL_TEMPLATE, id]))
        if result:
            comments_links = list(set(get_comments_links(result.content)))
            comments_tasks = [handle_comment(link, id) for link in comments_links]
            await asyncio.gather(*comments_tasks)
    return result


async def handle_comment(link, article_id):
    result = await get_html(link)
    if result:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            save_page,
            result.content, os.path.join(BASE_DIR, article_id, 'comment_links'), str(uuid4()) + '.html',
        )


def get_articles(html):
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('tr', class_='athing')
    for row in rows:
        a = row.find('a', class_='storylink')
        yield Article(a['href'], row['id'])


def get_comments_links(html):
    soup = BeautifulSoup(html, 'lxml')
    comments = soup.find_all('tr', class_='athing')
    for c in comments:
        links = c.find_all('a')
        for link in links:
            href = link['href']
            if is_external(href):
                yield href


def is_external(link):
    return link.startswith('http') and 'ycombinator' not in link


async def download_main_page():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    response = await get_html(BASE_URL)
    if response:
        logging.info(f'Got main page: {BASE_URL}')
        articles = list(set(get_articles(response.content)))
        tasks = [handle_article(article.link, article.id) for article in articles]
        await asyncio.gather(*tasks)
        logging.info('DONE')


def main():
    asyncio.run(download_main_page())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    main()
