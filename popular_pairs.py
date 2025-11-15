#!/usr/bin/env python3

from hdfs import InsecureClient
from collections import defaultdict
import sys
import csv

def get_top_pairs_debug(target_product_name, hdfs_host='localhost', hdfs_port=50070):
    """Версия с отладкой для поиска проблемы"""
    
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='hadoop')
    
    try:
        # 1. Читаем products.csv
        product_id_to_name = {}
        name_to_product_id = defaultdict(list)
        
        print("[DEBUG] Reading products.csv...")
        with client.read('/user/kupriyanovvn/cross-correlation/input/products.csv', encoding='utf-8') as reader:
            csv_reader = csv.reader(reader)
            headers = next(csv_reader)
            print(f"[DEBUG] Products headers: {headers}")
            
            product_count = 0
            for row in csv_reader:
                if len(row) >= 2:
                    product_id = row[0].strip()
                    product_name = row[1].strip()
                    product_id_to_name[product_id] = product_name
                    name_to_product_id[product_name].append(product_id)
                    product_count += 1
                    
                    # Выводим информацию о целевом продукте
                    if product_name == target_product_name:
                        print(f"[DEBUG] Found target product: ID={product_id}, Name={product_name}")
        
        print(f"[DEBUG] Total products loaded: {product_count}")
        
        # Проверяем существование продукта
        if target_product_name not in name_to_product_id:
            print(f"[ERROR] Продукт '{target_product_name}' не найден в {list(name_to_product_id.keys())[:5]}...")
            return []
        
        target_product_ids = name_to_product_id[target_product_name]
        print(f"[DEBUG] Target product IDs: {target_product_ids}")
        
        # 2. Анализируем order_items.csv с детальной отладкой
        all_pairs_count = defaultdict(int)
        orders_processed = 0
        orders_with_target = 0
        orders_with_pairs = 0
        
        print("[DEBUG] Reading order_items.csv...")
        with client.read('/user/kupriyanovvn/cross-correlation/input/order_items.csv', encoding='utf-8') as reader:
            csv_reader = csv.reader(reader)
            headers = next(csv_reader)
            print(f"[DEBUG] Order items headers: {headers}")
            
            current_order = None
            products_in_order = []
            
            for row_num, row in enumerate(csv_reader, 2):
                if len(row) < 3:
                    continue
                    
                order_id = row[1].strip()  # order_id во второй колонке
                product_id = row[2].strip()  # product_id в третьей колонке
                
                if order_id != current_order:
                    # Обрабатываем предыдущий заказ
                    if products_in_order:
                        orders_processed += 1
                        
                        # Проверяем содержит ли заказ целевой товар
                        has_target = any(pid in target_product_ids for pid in products_in_order)
                        if has_target:
                            orders_with_target += 1
                            print(f"[DEBUG] Order {current_order} contains target product. Products: {products_in_order}")
                        
                        # Создаем пары если в заказе 2+ товара
                        if len(products_in_order) >= 2:
                            orders_with_pairs += 1
                            sorted_products = sorted(products_in_order)
                            for i in range(len(sorted_products)):
                                for j in range(i + 1, len(sorted_products)):
                                    pair = (sorted_products[i], sorted_products[j])
                                    all_pairs_count[pair] += 1
                            
                            if has_target and len(products_in_order) <= 5:  # Выводим небольшие заказы для отладки
                                print(f"[DEBUG] Order {current_order} pairs created: {[(sorted_products[i], sorted_products[j]) for i in range(len(sorted_products)) for j in range(i+1, len(sorted_products))]}")
                    
                    # Начинаем новый заказ
                    products_in_order = []
                    current_order = order_id
                
                products_in_order.append(product_id)
                
                # Периодический вывод прогресса
                if row_num % 10000 == 0:
                    print(f"[DEBUG] Processed {row_num} rows, {orders_processed} orders, {orders_with_target} orders with target")
        
        # Обрабатываем последний заказ
        if products_in_order:
            orders_processed += 1
            has_target = any(pid in target_product_ids for pid in products_in_order)
            if has_target:
                orders_with_target += 1
            
            if len(products_in_order) >= 2:
                orders_with_pairs += 1
                sorted_products = sorted(products_in_order)
                for i in range(len(sorted_products)):
                    for j in range(i + 1, len(sorted_products)):
                        pair = (sorted_products[i], sorted_products[j])
                        all_pairs_count[pair] += 1
        
        print(f"\n[DEBUG] === STATISTICS ===")
        print(f"[DEBUG] Total orders processed: {orders_processed}")
        print(f"[DEBUG] Orders with target product: {orders_with_target}")
        print(f"[DEBUG] Orders with 2+ products: {orders_with_pairs}")
        print(f"[DEBUG] Total unique pairs found: {len(all_pairs_count)}")
        
        # Выводим несколько пар для отладки
        if all_pairs_count:
            sample_pairs = list(all_pairs_count.items())[:5]
            print(f"[DEBUG] Sample pairs: {sample_pairs}")
        
        # 3. Фильтруем пары с целевым товаром
        target_pairs = {}
        for (prod1, prod2), count in all_pairs_count.items():
            if prod1 in target_product_ids or prod2 in target_product_ids:
                # Определяем какой товар является парным (не целевой)
                if prod1 in target_product_ids:
                    paired_product_id = prod2
                else:
                    paired_product_id = prod1
                
                target_pairs[paired_product_id] = target_pairs.get(paired_product_id, 0) + count
        
        print(f"[DEBUG] Pairs with target product: {len(target_pairs)}")
        
        if target_pairs:
            print(f"[DEBUG] Target pairs sample: {list(target_pairs.items())[:5]}")
        
        # 4. Сортируем по популярности и берем топ-10
        top_pairs = sorted(target_pairs.items(), key=lambda x: x[1], reverse=True)[:10]
        
        print(f"\nТоп-10 продуктов, которые чаще всего покупают с '{target_product_name}':")
        print("-" * 70)
        
        if not top_pairs:
            print("Не найдено ни одной пары с указанным продуктом")
            print("\n[DEBUG] Возможные причины:")
            print("- Продукт никогда не покупался")
            print("- Продукт всегда покупался один (без других товаров в заказе)")
            print("- Ошибка в данных или логике обработки")
            return []
        
        results = []
        for idx, (paired_product_id, count) in enumerate(top_pairs, 1):
            paired_name = product_id_to_name.get(paired_product_id, f"Product_{paired_product_id}")
            print(f"{idx}. {paired_name} (ID: {paired_product_id}): {count} совместных покупок")
            results.append((paired_name, paired_product_id, count))
        
        return results
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python popular_pairs.py 'Название продукта'")
        sys.exit(1)
    
    target_product = sys.argv[1]
    get_top_pairs_debug(target_product)