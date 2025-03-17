import psycopg2

try:
    conn = psycopg2.connect(
        dbname='electronic_store',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    print('Соединение с БД установлено')
#                                        user    password   host    port  dbname
# conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5432/postgres')
except Exception as err:
    conn = None
    print(f'Возникла ошибка {err}')

# Объект cursor для выполнения запросов
conn.autocommit = True  # Немедленное выполнение команды с изменением состояния БД
cur = conn.cursor()
cur.execute('SELECT version();')

version = cur.fetchone()  # Одна строка
# version = cur.fetchall() # Все строки
# version = cur.fetchmany(2) # с указанием кол-ва строк
cur.close()
print(version)

# with conn.cursor() as cur:
#     cur.execute('CREATE DATABASE library;')

create_table_category = '''
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
'''
create_table_product = '''
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    category_id INT REFERENCES categories(id)
);
'''

create_table_customers = '''
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);
'''

create_table_orders = '''
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''

create_table_order_items = '''
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL
);
'''

with conn.cursor() as cur:
    cur.execute(create_table_category)
    cur.execute(create_table_product)
    cur.execute(create_table_customers)
    cur.execute(create_table_orders)
    cur.execute(create_table_order_items)

insert_data_category = '''
INSERT INTO categories (name) VALUES 
('Электроника'),
('Компьютеры'),
('Мобильные телефоны');
'''

insert_data_products = '''
INSERT INTO products (name, description, price, category_id) VALUES 
('Смартфон', 'Современный смартфон с большим экраном', 599.99, 3),
('Ноутбук', 'Мощный ноутбук для работы и игр', 999.99, 2),
('Наушники', 'Беспроводные наушники с хорошим звуком', 199.99, 1);
'''

insert_data_customers = '''
INSERT INTO customers (first_name, last_name, email) VALUES 
('Иван', 'Иванов', 'ivan@example.com'),
('Мария', 'Петрова', 'maria@example.com');
'''

insert_data_orders = '''
INSERT INTO orders (customer_id) VALUES 
(1),
(2);
'''

insert_data_order_items = '''
INSERT INTO order_items (order_id, product_id, quantity) VALUES 
(1, 1, 1),
(1, 3, 2),
(2, 2, 1);
'''

with conn.cursor() as cur:
    cur.execute(insert_data_category)
    cur.execute(insert_data_products)
    cur.execute(insert_data_customers)
    cur.execute(insert_data_orders)
    cur.execute(insert_data_order_items)

# Выбор всех товаров с их категориями
query_1 = '''
SELECT p.id, p.name AS product_name, c.name AS category_name, p.price
FROM products p
JOIN categories c ON p.category_id = c.id;
'''

with conn.cursor() as cur:
    cur.execute(query_1)
    results = cur.fetchall()

print('Выбор всех товаров с их категориями:')
print(*results, sep='\n')

# Количество товаров по каждой категории
query_2 = '''
SELECT c.name AS category_name, COUNT(p.id) AS product_count
FROM categories c
LEFT JOIN products p ON c.id = p.category_id
GROUP BY c.name;
'''

with conn.cursor() as cur:
    cur.execute(query_2)
    results = cur.fetchall()

print('Количество товаров в каждой категории:')
print(*results, sep='\n')

name = 'iphon 16'
description = '512GB'
price = 85000
category_id = 1

query_3 = '''
INSERT INTO products (name, description, price, category_id) VALUES 
(%s, %s, %s, %s);
'''

with conn.cursor() as cur:
    cur.execute(query_3, (name, description, price, category_id))

with conn.cursor() as cur:
    cur.execute(query_1)
    results = cur.fetchall()

print('Выбор всех товаров с их категориями:')
print(*results, sep='\n')

# Получение всех заказов определённого клиента
query_4 = '''
SELECT o.id AS order_id, o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE c.email = 'ivan@example.com';
'''

with conn.cursor() as cur:
    cur.execute(query_4)
    results = cur.fetchall()

print('Все заказы определённого клиента:')
print(*results, sep='\n')

# Общая стоимость заказа
query_5 = '''
SELECT o.id AS order_id, SUM(p.price * oi.quantity) AS total_price
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
GROUP BY o.id;
'''

with conn.cursor() as cur:
    cur.execute(query_5)
    results = cur.fetchall()

print('Общая стоимость заказа:')
print(*results, sep='\n')

# Наименования всех товаров, купленных каждым клиентом
query_6 = '''
SELECT c.id AS customer_id, p.name AS product_name
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON p.id = oi.product_id;
'''

with conn.cursor() as cur:
    cur.execute(query_6)
    results = cur.fetchall()

print('Наименования всех товаров, купленных каждым клиентом:')
print(*results, sep='\n')

# Количество заказов, в которых есть каждый товар
query_7 = '''
SELECT p.id AS product_id, COUNT(o.id) AS count_orders
FROM products p
JOIN order_items oi ON p.id = oi.product_id
JOIN orders o ON o.id = oi.order_id
GROUP BY p.id;
'''

with conn.cursor() as cur:
    cur.execute(query_7)
    results = cur.fetchall()

print('Количество заказов, в которых есть каждый товар:')
print(*results, sep='\n')

# Количество штук всех товаров в заказе
query_8 = '''
SELECT o.id AS order_id, SUM(oi.quantity) AS sum_products
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
WHERE o.id = 1
GROUP BY o.id;
'''

with conn.cursor() as cur:
    cur.execute(query_8)
    results = cur.fetchall()

print('Количество штук всех товаров в заказе:')
print(*results, sep='\n')

# Закрываем соединение после завершения работы
conn.close()