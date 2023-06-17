import json
import logging
import multiprocessing
import requests
import re
import urllib3
from urllib.parse import urljoin
from os import makedirs
from os.path import exists

# 当 verify=False 时会有警告，故屏蔽
urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# 请求地址
BASE_URL = 'https://ssr1.scrape.center'
# 请求最大页码
TOTAL_PAGE = 3

# 结果保存地址，不存在则创建
RESULTS_DIR = './results'
exists(RESULTS_DIR) or makedirs(RESULTS_DIR)


def scrape_page(url):
    """
    爬取页面
    :param url:
    :return:
    """
    try:
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            return response.text
    except requests.RequestException:
        # exc_info 设置为 True 可以打印 Traceback 错误堆栈信息。
        logging.error('error occurred while scrape %s', url, exc_info=True)


def scrape_index(page):
    """
    爬取列表页
    :param page:
    :return:
    """
    index_url = f'{BASE_URL}/page/{page}'
    logging.info('scraping %s...', index_url)
    return scrape_page(index_url)


def parse_index(html):
    """
    解析列表页的 HTML 代码
    :param html:
    :return:
    """
    # 定义提取标题超链接 href 属性的正则，其中 .*? 表示非贪婪匹配
    pattern = re.compile('<a.*?href="(.*?)".*?class="name">')
    items = re.findall(pattern, html)
    if not items:
        return []

    for item in items:
        detail_url = urljoin(BASE_URL, item)
        logging.info('detail url %s', detail_url)
        yield detail_url


def scrape_detail(url):
    """
    爬取详情页
    :param url:
    :return:
    """
    logging.info('scraping %s...', url)
    return scrape_page(url)


def parse_detail(html):
    """
    解析详情页
    :param html:
    :return:
    """
    # 封面，只有一个匹配，故用 search
    cover_pattern = re.compile('<img.*?src="(.*?)".*?class="cover"', re.S)
    match_res = re.search(cover_pattern, html)
    cover = match_res.group(1).strip() if match_res else None
    # 名称
    name_pattern = re.compile('<h2.*?>(.*?)</h2>')
    match_res = re.search(name_pattern, html)
    name = match_res.group(1).strip() if match_res else None
    # 类别
    categories_pattern = re.compile('<button.*?category.*?<span>(.*?)</span>.*?</button>', re.S)
    match_res = re.findall(categories_pattern, html)
    categories = match_res if match_res else []
    # 上映时间
    published_at_pattern = re.compile('(\d{4}-\d{2}-\d{2})\s?上映')
    match_res = re.search(published_at_pattern, html)
    published_at = match_res.group(1).strip() if match_res else None
    # 剧情简介
    drama_pattern = re.compile('drama.*?<p.*?>(.*?)</p>', re.S)
    match_res = re.search(drama_pattern, html)
    drama = match_res.group(1).strip() if match_res else None
    # 评分
    score_pattern = re.compile('<p.*?score.*?>(.*?)</p>', re.S)
    match_res = re.search(score_pattern, html)
    score = match_res.group(1).strip() if match_res else None

    return {
        'cover': cover,
        'name': name,
        'categories': categories,
        'published_at': published_at,
        'drama': drama,
        'score': score
    }


def save_data(data):
    file_name = data.get('name')
    file_path = f'{RESULTS_DIR}/{file_name}.json'
    # ensure_ascii=False 可以保证中文正常表示；indent=2 设置缩进
    json.dump(data, open(file_path, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)


def main(page):
    """
    爬虫流程：
        1. 请求页面
        2. 解析返回的 HTML，获取详情页的链接
        3. 请求详情页
        4. 解析返回的 HTML,获取详情页的数据
        5. 保存数据到文件
    :return:
    """
    # 请求页面
    index_html = scrape_index(page)
    # 解析页面，获得详情页的链接
    detail_urls = parse_index(index_html)

    # 请求详情页
    for detail_url in detail_urls:
        detail_html = scrape_detail(detail_url)
        data = parse_detail(detail_html)
        logging.info('get detail data %s', data)
        # 保存数据
        save_data(data)
        logging.info('data save successfully')


if __name__ == '__main__':
    # 声明进程池
    pool = multiprocessing.Pool()
    # 声明需要遍历的页码
    pages = range(1, TOTAL_PAGE + 1)
    # 遍历 pages，将每个页码分别传递给 main()，并把每次调用变成一个进程，加入到进程池，进程数会根据机器 CPU 核数决定。
    pool.map(main, pages)
    pool.close()
    pool.join()
