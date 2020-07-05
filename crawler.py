import asyncio
from collections import namedtuple
import logging
import os
import random
from uuid import uuid4

import aiohttp
from bs4 import BeautifulSoup

import config as cfg

Response = namedtuple('Response', 'status, content')
Article = namedtuple('Article', 'link, id')


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


def save_page(html, path, file_name):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(os.path.join(path, file_name), 'wb') as f:
        f.write(html)


def is_external(link):
    return link.startswith('http') and 'ycombinator' not in link


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
    headers = {'User-Agent': random.choice(cfg.USER_AGENTS_LIST)}
    semaphore = asyncio.Semaphore(1)
    async with semaphore:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=cfg.CONNECT_TIMEOUT),
                                         headers=headers) as session:
            attempts = 0
            while True:
                try:
                    return await fetch(url, session)
                except asyncio.TimeoutError:
                    if attempts < cfg.RETRY_NUM:
                        attempts += 1
                        await asyncio.sleep(1)
                        logging.info(f'Reconnect #{attempts}. URL: {url}.')
                    else:
                        logging.info(f'Connection timeout: {url}')
                        return
                except (aiohttp.ClientConnectorError, aiohttp.ClientOSError):
                    logging.info(f'Could not connect to: {url}')
                    return


async def handle_article(link, article_idid):
    path = os.path.join(cfg.BASE_DIR, article_idid)
    if os.path.exists(path):
        return
    article_response = await get_html(link)
    if article_response:
        logging.info(f'Got article: {link}')
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            save_page,
            article_response.content, path, 'article.html'
        )

        comments_response = await get_html(''.join([cfg.COMMENTS_URL_TEMPLATE, article_idid]))
        if comments_response:
            comments_links = list(set(get_comments_links(comments_response.content)))
            comments_tasks = [handle_comment(link, article_idid) for link in comments_links]
            await asyncio.gather(*comments_tasks)


async def handle_comment(link, article_id):
    result = await get_html(link)
    if result:
        loop = asyncio.get_running_loop()
        path = os.path.join(cfg.BASE_DIR, article_id, 'comment_links')
        await loop.run_in_executor(
            None,
            save_page,
            result.content, path, str(uuid4()) + '.html',
        )


async def download():
    if not os.path.exists(cfg.BASE_DIR):
        os.makedirs(cfg.BASE_DIR)

    response = await get_html(cfg.BASE_URL)
    if response:
        logging.info('Searching for new articles...')
        articles = list(set(get_articles(response.content)))
        tasks = [handle_article(article.link, article.id) for article in articles]
        await asyncio.gather(*tasks)
        logging.info('Finished')


async def main():
    while True:
        asyncio.create_task(download())
        await asyncio.sleep(cfg.SLEEP_INTERVAL)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
