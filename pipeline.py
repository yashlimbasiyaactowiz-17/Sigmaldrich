import mysql.connector


#  CONNECTION
def make_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="actowiz",
        database="sigma_aldrich"
    )


#  CREATE TABLE (UNCHANGED - SAME AS YOUR FILE)
def create_table():
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS category_urls (
        id INT AUTO_INCREMENT PRIMARY KEY,
        main_cate VARCHAR(255),
        main_url VARCHAR(255),
        sub_cate VARCHAR(255),
        sub_url VARCHAR(255),
        sub_sub_cate VARCHAR(255),
        url VARCHAR(555) UNIQUE,
        status VARCHAR(50)
    )
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


#  INSERT CATEGORY DATA
def insert_data(urls_data):
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    INSERT IGNORE INTO category_urls (
        main_cate,
        main_url,
        sub_cate,
        sub_url,
        sub_sub_cate,
        url,
        status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    values = []
    for item in urls_data:
        values.append((
            item.get("main_cate"),
            item.get("main_url"),
            item.get("sub_cate"),
            item.get("sub_url"),
            item.get("sub_sub_cate"),
            item.get("url"),
            item.get("status")
        ))

    cursor.executemany(query, values)
    conn.commit()

    print(f"Inserted rows: {cursor.rowcount}")

    cursor.close()
    conn.close()


#  FETCH PENDING URLS (FIXED)
def fetch_urls(limit=None):
    conn = make_connection()
    cursor = conn.cursor()

    query = "SELECT url FROM category_urls WHERE status = 'pending'"

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    urls = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return urls


#  CREATE PRODUCT TABLE
def create_product_table():
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS product_urls (
        id INT AUTO_INCREMENT PRIMARY KEY,
        productName TEXT,
        productKey VARCHAR(255),
        productUrl TEXT,
        category_url TEXT
    )
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


#  INSERT PRODUCTS
def insert_products(products):
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO product_urls (
        productName,
        productKey,
        productUrl,
        category_url
    ) VALUES (%s, %s, %s, %s)
    """

    values = []
    for item in products:
        values.append((
            item.get("productName"),
            item.get("productKey"),
            item.get("productUrl"),
            item.get("category_url")
        ))

    cursor.executemany(query, values)
    conn.commit()

    print(f"Inserted products: {cursor.rowcount}")

    cursor.close()
    conn.close()


#  UPDATE STATUS
def update_status_done(url):
    conn = make_connection()
    cursor = conn.cursor()

    query = "UPDATE category_urls SET status = 'done' WHERE url = %s"
    cursor.execute(query, (url,))
    conn.commit()

    print(f"Updated done: {url}")

    cursor.close()
    conn.close()