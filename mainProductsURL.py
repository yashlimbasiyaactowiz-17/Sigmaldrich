import requests
import json
from lxml import html
import sys
from urllib.parse import urljoin
from pipeline import create_table, insert_data

sys.stdout.reconfigure(encoding='utf-8')

headers = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

def fetch_json_and_save(url, filename="main.json"):

    response = requests.get(url, headers=headers)
    tree = html.fromstring(response.content)

    json_text = tree.xpath("string(//script[@type='application/json']//text())")

    json_data = json.loads(json_text)

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(json_data, file, indent=4, ensure_ascii=False)

    return json_data


def parse_categories(json_data):
    urls_data = []

    base_url = "https://www.sigmaaldrich.com"

    nav = (
        json_data
        .get("props", {})
        .get("apolloState", {})
        .get("ROOT_QUERY", {})
        .get("aemHeaderFooter", {})
        .get("header", {})
        .get("topnav", [])[0]
    )

    items = nav.get("items", [])

    for item in items:
        main_cate = item.get("title")
        main_url = item.get("url")


        if item.get("childrens"):
            for sub in (item.get("childrens") or []):
                sub_cate = sub.get("title")
                sub_url = sub.get("url")

                sub_children = sub.get("childrens")

                if sub_children:
                    for sub_sub in (sub_children or []):
                        urls_data.append({
                            "main_cate": main_cate,
                            "main_url": urljoin(base_url, main_url),
                            "sub_cate": sub_cate,
                            "sub_url": urljoin(base_url, sub_url),
                            "sub_sub_cate": sub_sub.get("title"),
                            "url": urljoin(base_url, sub_sub.get("url")),
                            "status": "pending"
                        })
                else:
                    urls_data.append({
                        "main_cate": main_cate,
                        "main_url": urljoin(base_url, main_url),
                        "sub_cate": sub_cate,
                        "sub_url": urljoin(base_url, sub_url),
                        "sub_sub_cate": "",
                        "url": urljoin(base_url, sub.get("url")),
                        "status": "pending"
                    })
        else:
            urls_data.append({
                "main_cate": main_cate,
                "main_url": urljoin(base_url, main_url),
                "sub_cate": "",
                "sub_url": urljoin(base_url, sub_url),
                "sub_sub_cate": "",
                "url": urljoin(base_url, item.get("url")),
                "status": "pending",
                
            })

    # for i in urls_data:
    #     print(i)

    return urls_data

data = fetch_json_and_save("https://www.sigmaaldrich.com/SG/en", "main.json")
parse_categories(data)

create_table()

data = fetch_json_and_save("https://www.sigmaaldrich.com/SG/en")
final_data = parse_categories(data)
print(final_data)
insert_data(final_data)