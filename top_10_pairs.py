#!/usr/bin/env python3

import sys
import pandas as pd
from io import StringIO

def get_most_popular_pairs(product_name):
    flag = ''
    order_items_lines = []
    products_lines = []
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Detect which CSV based on header
        if line.startswith("order_item_id"):
            flag = "order_items"
            order_items_lines.append(line)
        elif line.startswith("product_id,"):
            # product_id header - products file (has comma to distinguish)
            flag = "products"
            products_lines.append(line)
        elif flag == "order_items":
            order_items_lines.append(line)
        elif flag == "products":
            products_lines.append(line)
    
    # Creating pd.DataFrames from the collected lines
    if not order_items_lines:
        print("Warning: No order_items data found in stdin", file=sys.stderr)
        order_items_lines = []
    if not products_lines:
        print("Warning: No products data found in stdin", file=sys.stderr)
        products_lines = []
    
    if order_items_lines:
        df_order_items = pd.read_csv(StringIO('\n'.join(order_items_lines)))
    else:
        df_order_items = pd.DataFrame()
    
    if products_lines:
        df_products = pd.read_csv(StringIO('\n'.join(products_lines)))
    else:
        df_products = pd.DataFrame()
    
    # Merge only if both dataframes have data
    if df_order_items.empty or df_products.empty:
        print("Error: One or both dataframes are empty. Cannot proceed.", file=sys.stderr)
        return []
    
    # Merge on product_id (inner join)
    merged_df = pd.merge(df_order_items, df_products, on='product_id', how='inner')
    
    # Filter by product_name if provided
    if product_name:
        # Find product_id(s) matching the given product name
        matching_products = df_products[df_products['product_name'] == product_name]['product_id'].unique()
        if len(matching_products) == 0:
            print(f"Error: No product found with name '{product_name}'", file=sys.stderr)
            return []
        # Filter merged_df to only include orders containing the specified product(s)
        merged_df = merged_df[merged_df['product_id'].isin(matching_products)]
    
    # Group by order_id to find product pairs bought together
    # For each order, get all product pairs (combinations of 2)
    pairs_count = {}
    for order_id, group in merged_df.groupby('order_id'):
        products = sorted(group['product_id'].unique())
        # Generate all pairs from products in this order
        for i in range(len(products)):
            for j in range(i + 1, len(products)):
                pair = (products[i], products[j])
                pairs_count[pair] = pairs_count.get(pair, 0) + 1
    
    # Sort by frequency (descending) and get top 10
    top_10_pairs = sorted(pairs_count.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # Print results
    if product_name:
        print(f"Top 10 Product Pairs with '{product_name}':")
    else:
        print("Top 10 Product Pairs:")
    print("-" * 60)
    for idx, (pair, count) in enumerate(top_10_pairs, 1):
        print(f"{idx}. Products {pair[0]} and {pair[1]}: bought together {count} times")
    
    return top_10_pairs

if __name__ == "__main__":
    import sys as sys_module
    product_name = sys_module.argv[1] if len(sys_module.argv) > 1 else None
    get_most_popular_pairs(product_name)