orders = ['cpu monitor gpu',
'cpu gpu',
'keyboard mouse',
'keyboard mouse',
'keyboard mouse',
'mic headphones',
'mic headphones',
'keyboard mouse mic headphones',
'keyboard mouse mic headphones',
'keyboard mouse mic headphones',
'keyboard mouse mic headphones',
'disk']



result = []
for order in orders:
    items = order.split(' ')
    if len(items) > 1:
        for i in range(0, len(items) - 1):
            for j in range(i + 1, len(items)):
                if items[i] < items[j]:
                    print(f"{items[i]},{items[j]}\t1")
                    result.append(f"{items[i]},{items[j]}\t1")
                else: 
                    print(f"{items[j]},{items[i]}\t1")
                    result.append(f"{items[j]},{items[i]}\t1")

print(result)
print("\n\n---------------------------------------\n\n")
pair_counts = {}
    
for line in result:
    line = line.strip()
    if not line:
        continue
    try:
        pair, count = line.split("\t")
        count = int(count)
        if pair in pair_counts.keys():
            pair_counts[pair] += count
        else:
            pair_counts[pair] = count
    except ValueError:
        continue
for item in pair_counts.items():
    print(f"{item[0]}\t{item[1]}")

