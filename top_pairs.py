#!/usr/bin/env python3

from collections import defaultdict
import sys
from hdfs import InsecureClient

def get_top_pairs_with_product(target_product, hdfs_path='/user/kupriyanovvn/pr2/data/orders.txt'):
    """Находит топ-10 товаров, которые чаще всего покупают с целевым товаром из HDFS"""
    
    # Подключение к HDFS
    client = InsecureClient('http://localhost:50070', user='kupriyanovvn')
    
    try:
        # Чтение данных из HDFS
        print(f"[INFO] Чтение данных из HDFS: {hdfs_path}")
        orders = []
        
        with client.read(hdfs_path, encoding='utf-8') as reader:
            for line in reader:
                line = line.strip()
                if line:
                    items = line.split()
                    orders.append(items)
        
        print(f"[INFO] Всего заказов прочитано: {len(orders)}")
        
        # Подсчет всех пар товаров
        all_pairs_count = defaultdict(int)
        
        for order in orders:
            if len(order) >= 2:
                # Сортируем для уникальности пар
                sorted_items = sorted(order)
                for i in range(len(sorted_items)):
                    for j in range(i + 1, len(sorted_items)):
                        pair = (sorted_items[i], sorted_items[j])
                        all_pairs_count[pair] += 1
        
        print(f"[INFO] Всего уникальных пар найдено: {len(all_pairs_count)}")
        
        # Фильтруем пары с целевым товаром
        target_pairs = {}
        for (item1, item2), count in all_pairs_count.items():
            if item1 == target_product or item2 == target_product:
                # Определяем парный товар (не целевой)
                paired_item = item2 if item1 == target_product else item1
                target_pairs[paired_item] = target_pairs.get(paired_item, 0) + count
        
        print(f"[INFO] Найдено пар с товаром '{target_product}': {len(target_pairs)}")
        
        if not target_pairs:
            print(f"[WARNING] Не найдено ни одной пары с товаром '{target_product}'")
            return []
        
        # Сортируем по популярности и берем топ-10
        top_pairs = sorted(target_pairs.items(), key=lambda x: x[1], reverse=True)[:10]
        
        print(f"\nТоп-10 товаров, которые покупают с '{target_product}':")
        print("=" * 55)
        
        for idx, (product, count) in enumerate(top_pairs, 1):
            print(f"{idx:2}. {product:<20} {count:>3} совместных покупок")
        
        return top_pairs
        
    except Exception as e:
        print(f"[ERROR] Ошибка при работе с HDFS: {e}")
        return []

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python popular_pairs.py <название_товара>")
        print("Пример: python popular_pairs.py laptop")
        sys.exit(1)
    
    target_product = sys.argv[1]
    
    # Проверяем существование файла в HDFS
    client = InsecureClient('http://localhost:50070', user='kupriyanovvn')
    hdfs_path = '/user/kupriyanovvn/pr2/data/orders.txt'
    
    try:
        if not client.status(hdfs_path, strict=False):
            print(f"Файл {hdfs_path} не найден в HDFS")
            sys.exit(1)
    except Exception as e:
        print(f"Ошибка при проверке файла в HDFS: {e}")
        sys.exit(1)
    
    # Анализ для целевого товара
    top_pairs = get_top_pairs_with_product(target_product, hdfs_path)