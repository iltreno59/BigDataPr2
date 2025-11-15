import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()

def generate_ecommerce_data(num_customers=1000, num_products=500, num_orders=5000):
    
    # Генерация клиентов
    customers = []
    for i in range(num_customers):
        customers.append({
            'customer_id': i + 1,
            'name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'segment': random.choice(['new', 'regular', 'vip'])
        })
    
    # Генерация продуктов
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']
    products = []
    for i in range(num_products):
        products.append({
            'product_id': i + 1,
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(5, 1000), 2),
            'cost': round(random.uniform(2, 500), 2),
            'brand': fake.company(),
            'stock_quantity': random.randint(0, 1000)
        })
    
    # Генерация заказов
    orders = []
    order_items = []
    
    for i in range(num_orders):
        customer_id = random.randint(1, num_customers)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        order_data = {
            'order_id': i + 1,
            'customer_id': customer_id,
            'order_date': order_date,
            'status': random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled']),
            'total_amount': 0,
            'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer']),
            'shipping_address': fake.address().replace('\n', ', ')
        }
        
        # Генерация товаров в заказе
        num_items = random.randint(1, 5)
        order_total = 0
        
        for j in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 3)
            item_total = product['price'] * quantity
            order_total += item_total
            
            order_items.append({
                'order_item_id': len(order_items) + 1,
                'order_id': i + 1,
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': product['price'],
                'total_price': item_total
            })
        
        order_data['total_amount'] = round(order_total, 2)
        orders.append(order_data)
    
    return {
        'customers': pd.DataFrame(customers),
        'products': pd.DataFrame(products),
        'orders': pd.DataFrame(orders),
        'order_items': pd.DataFrame(order_items)
    }

# Генерация данных
data = generate_ecommerce_data(1000, 200, 5000)

# Сохранение в CSV
for table_name, df in data.items():
    df.to_csv(f'{table_name}.csv', index=False)
    print(f"Создан {table_name}.csv с {len(df)} записями")