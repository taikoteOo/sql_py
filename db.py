import psycopg2

try:
    conn = psycopg2.connect(
        dbname='library',
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

with conn.cursor() as cur:
    cur.execute(create_table_category)
    cur.execute(create_table_product)

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

# with conn.cursor() as cur:
#     cur.execute(insert_data_category)
#     cur.execute(insert_data_products)

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

# Закрываем соединение после завершения работы
conn.close()