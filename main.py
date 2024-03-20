from bs4 import BeautifulSoup
import requests
from queue import Queue
from threading import Thread
import time
from urllib.parse import urljoin, quote, urlparse
import json

class Crawler:
    def __init__(self, start_url):
        self.visited = set()
        self.queue = Queue()
        self.queue.put(start_url)
        self.base_url = start_url

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
            'text_size': len(soup.get_text()), # 'size' is the length of the 'text' field, not the size of the HTML file
            'html_size': len(str(soup)),
            'html': str(soup),
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
        with open(f'jsons/{filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii = False)

        return data

    def crawl(self):
        while True:
            url = self.queue.get()
            if url not in self.visited:
                self.visited.add(url)
                try:
                    soup = self.get_html_data(url)
                    data = self.parse_html(soup, url)
                    print(f"Crawled {url}")
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

# create jsons folder
import os
os.makedirs('jsons', exist_ok=True)

crawler = Crawler("https://namu.wiki/w/Microsoft%20Azure")
crawler.start_crawling()