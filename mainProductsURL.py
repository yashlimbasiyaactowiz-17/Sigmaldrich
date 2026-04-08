import requests
import json
from parsel import Selector
from lxml import html
import sys

sys.stdout.reconfigure(encoding='utf-8')

headers = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

response = requests.get('https://www.sigmaaldrich.com/SG/en', headers=headers)
data = response.content
tree = html.fromstring(data)

base_xpath = tree.xpath("string(//script[@type='application/json']//text())")
print(base_xpath)

json_data = json.loads(base_xpath)

with open("main.json", "w", encoding="utf-8") as file:
    json.dump(json_data, file, indent=4, ensure_ascii=False)

def get_main_products_url(url):
    selector = Selector(text=data)
    main_products_url = selector.xpath(
        
    )
