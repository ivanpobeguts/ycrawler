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
SLEEP_INTERVAL = 30
CONNECT_TIMEOUT = 10
RETRY_NUM = 3
