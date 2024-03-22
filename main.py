import os
import sys
import signal
from bs4 import BeautifulSoup
import requests
from queue import Queue
import time
from urllib.parse import urljoin, quote, urlparse
import json
from bloom_filter2 import BloomFilter


class Crawler:
    def __init__(self, base_url, external_queue=None):
        self.visited = BloomFilter(max_elements=1000000, error_rate=0.05)
        self.queue = None
        if external_queue is None or external_queue.empty():
            self.queue = Queue()
            self.queue.put(base_url)
        else:
            self.queue = external_queue
        self.base_url = base_url

    def save_queue(self, sig, frame):
        with open('queue.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(list(self.queue.queue)))
        sys.exit(0)

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
        safe_url = quote(url, safe='')
        if not data['title']:
            data['title'] = safe_url
        else:
            data['title'] = data['title'][:-7]
        data['text'] = data['text'].replace('\n', ' ').replace('\t', ' ')
        filename = (data['title'].replace('/', '_slash_')
                    .replace('?', '_question_')
                    .replace(':', '_colon_')
                    .replace('*', '_star_')
                    .replace('"', '_quote_')
                    .replace('<', '_lt_')
                    .replace('>', '_gt_')
                    .replace('|', '_pipe_'))
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
            if url not in self.visited:
                self.visited.add(url)
                try:
                    soup = self.get_html_data(url)
                    data = self.parse_html(soup, url)
                    for link in data['links']:
                        if link not in self.visited:
                            self.queue.put(link)
                except Exception as e:
                    print(f"Error in: {e}")
                    # write error to file
                    with open('errors.txt', 'a', encoding='utf-8') as f:
                        f.write(f'Error in {url}: {e}\n')
            self.queue.task_done()
            time.sleep(0.5)

    def start_crawling(self):
        self.crawl()


def read_queue():
    with open('queue.txt', 'r', encoding='utf-8') as f:
        return f.read().split('\n')


os.makedirs('jsons', exist_ok=True)
if os.path.exists('queue.txt'):
    external_temp_queue = read_queue()
    external_queue = Queue()
    for url in external_temp_queue:
        external_queue.put(url)
else:
    external_queue = None

start_url = sys.argv[1]
crawler = Crawler(start_url, external_queue)
signal.signal(signal.SIGINT, crawler.save_queue)
crawler.start_crawling()
