import os
import sys
import signal
from bs4 import BeautifulSoup
import requests
# from queue import Queue
from persistqueue import Queue
import time
from urllib.parse import urljoin, quote, urlparse
import json
from bloom_filter2 import BloomFilter

from sqlite_set import SqliteSet


class Crawler:
    def __init__(self, base_url):
        self.visited = SqliteSet('visited.db', 'visited')
        self.queue = Queue("queue")
        self.base_url = base_url

    def get_html_data(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup

    def get_path(self, url):
        result = urlparse(url)
        return result.scheme + "://" + result.netloc + result.path

    def parse_html(self, soup, url):
        data = {
            'url': url,
            'title': str(soup.title.string) if soup.title else None,
            'time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'text_size': len(soup.get_text()),
            # 'size' is the length of the 'text' field, not the size of the HTML file
            'html_size': len(str(soup)),
            # 'html': str(soup),
            'text': soup.get_text(),
            'links': set(),
            'images': [urljoin(self.base_url, img['src']) for img in soup.find_all('img', src=True)]
        }

        for a in soup.find_all('a', href=True):
            link = self.get_path(urljoin(self.base_url, a['href']))
            if link == url:
                continue
            parsed_link = urlparse(link)
            if parsed_link.netloc == "namu.wiki" and parsed_link.path.startswith("/w/"):
                data['links'].add(link)
        data['links'] = list(data['links'])
        data['text'] = data['text'].replace('\n', ' ').replace('\t', ' ')
        safe_url = quote(url, safe='')
        if not data['title']:
            data['title'] = safe_url
        else:
            data['title'] = data['title'][:-7]
        filename = (data['title'].replace('/', '_slash_')
                    .replace('?', '_question_')
                    .replace(':', '_colon_')
                    .replace('*', '_star_')
                    .replace('"', '_quote_')
                    .replace('<', '_lt_')
                    .replace('>', '_gt_')
                    .replace('|', '_pipe_')
                    .replace('\\', '_backslash_'))
        filename = f'jsons/{filename}.json'
        if os.path.exists(filename):
            print(f"Already crawled {url}")
            return data

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Crawled {url}")
        return data

    def crawl(self):
        while self.queue.qsize() > 0:
            url = self.queue.get()
            self.visited.add(url)
            try:
                soup = self.get_html_data(url)
                data = self.parse_html(soup, url)
                for link in data['links']:
                    if not self.visited.contains(link):
                        self.queue.put(link)
            except Exception as e:
                print(f"Error in: {e}")
                # write error to file
                with open('errors.txt', 'a', encoding='utf-8') as f:
                    f.write(f'Error in {url}: {e}\n')
            self.queue.task_done()
            time.sleep(0.5)

    def start_crawling(self):
        self.queue.put(self.base_url)
        self.crawl()


os.makedirs('jsons', exist_ok=True)

start_url = sys.argv[1]
crawler = Crawler(start_url)
crawler.start_crawling()
