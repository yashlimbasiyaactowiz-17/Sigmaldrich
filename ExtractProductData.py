from curl_cffi import requests
from lxml import html
import json
import os
import gzip
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline import fetch_urls, insert_products, update_status_done, create_product_table

sys.stdout.reconfigure(encoding='utf-8')

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

#  CONFIG
MAX_WORKERS = 5        # threading for pages
URL_THREADS = 3        # threading for URLs
BATCH_SIZE = 2         # number of URLs per batch


def get_page_json(url):
    for attempt in range(3):
        response = requests.get(url, headers=headers, timeout=60, impersonate="chrome110")

        if not response.content.strip():
            time.sleep(2)
            continue

        tree = html.fromstring(response.content)
        raw = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')

        if not raw:
            time.sleep(2)
            continue

        try:
            return json.loads(raw[0])
        except:
            time.sleep(2)

    return {}


def get_search_block(root_query, page):
    for key in root_query.keys():
        if key.startswith("getProductSearchResults") and f'"page":{page}' in key:
            return root_query[key]
    return {}


def save_json(data, file_name):
    os.makedirs("json_pages", exist_ok=True)
    file_path = f"json_pages/{file_name}.json.gz"

    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"Saved JSON: {file_path}")


def parse_page(page, url, file_name):
    page_url = f"{url}?page={page}"
    page_data = get_page_json(page_url)

    if not page_data:
        return []

    root_query = page_data.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {})
    item_p = get_search_block(root_query, page)
    items = item_p.get("items", [])

    page_products = []

    for item in items:
        product = item.get("name")
        productKey = item.get("productKey")
        brand = item.get("brand", {}).get("key", "").lower()

        prod_url = f"https://www.sigmaaldrich.com/SG/en/product/{brand}/{productKey}"

        page_products.append({
            "productName": product,
            "productKey": productKey,
            "productUrl": prod_url,
            "category_url": url
        })

    return page_products

def process_url(url):
    try:
        print("Processing:", url)

        file_name = url.split("/")[-1]

        data = get_page_json(url)
        if not data:
            return []

        root_query = data.get("props", {}).get("apolloState", {}).get("ROOT_QUERY", {})
        item_path = get_search_block(root_query, 1)

        total_pages = item_path.get("metadata", {}).get("numPages", 1)

        products = []

        # page-level threading (already tha)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(parse_page, page, url, file_name)
                       for page in range(1, total_pages + 1)]

            for future in as_completed(futures):
                products.extend(future.result())

        print(f"Total Products ({url}):", len(products))

        return products

    except Exception as e:
        print("ERROR:", url, e)
        return []


# BATCH PROCESSING
def process_batch(batch_urls):
    all_products = []

    with ThreadPoolExecutor(max_workers=URL_THREADS) as executor:
        futures = [executor.submit(process_url, url) for url in batch_urls]

        for future, url in zip(futures, batch_urls):
            products = future.result()

            if products:
                all_products.extend(products)
                update_status_done(url)

    return all_products


# MAIN UPDATED
def main():
    create_product_table()

    while True:
        urls = fetch_urls(limit=10)   # ek baar me 10 uthao

        if not urls:
            print("All pending urls completed")
            break

        print(f"Fetched {len(urls)} URLs")

        # 🔹 batching
        for i in range(0, len(urls), BATCH_SIZE):
            batch = urls[i:i + BATCH_SIZE]

            print(f"\nProcessing Batch {i//BATCH_SIZE + 1}")

            products = process_batch(batch)

            if products:
                insert_products(products)
                print(f"Inserted: {len(products)}")
            else:
                print("No products in this batch")


if __name__ == "__main__":
    main()