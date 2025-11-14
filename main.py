from hdfs import InsecureClient

class CrossCorrelationPairs:
    def __init__(self):
        self.pairs = []
        self.counts = []
    
    def map(self, items):
        for item_i in items:
            for item_j in items:
                if item_i != item_j:
                    pair = (item_i, item_j)
                    self.pairs.append(pair)
                    self.counts.append(1)

    def reduce(self, pairs, counts):
        reduced_results = {}
        for pair, count in zip(pairs, counts):
            if pair not in reduced_results:
                reduced_results[pair] = count
            else:
                reduced_results[pair] += count
        return reduced_results


class CrossCorrelationStripes:
    def __init__(self):
        self.items = []
        self.stripes = []
        pass

    def map(self, items):
        for item_i in items:
            stripe = {}
            for item_j in items:
                if item_i != item_j:
                    if item_j not in stripe:
                        stripe[item_j] = 1
                    else:
                        stripe[item_j] += 1

            self.items.append(item_i)
            self.stripes.append(stripe)

    def reduce(self, items, stripes):
        reduced_results = {}
        for item, stripe in zip(items, stripes):
            if item not in reduced_results:
                reduced_results[item] = stripe
            else:
                for key, value in stripe.items():
                    if key not in reduced_results[item]:
                        reduced_results[item][key] = value
                    else:
                        reduced_results[item][key] += value
        return reduced_results


def test_cross_correlation_pairs():
    text = 'hello world hello hdfs hello mapreduce hello big data hello data'
    items = text.split(' ')
    ccp = CrossCorrelationPairs()
    ccp.map(items)
    for pair, count in zip(ccp.pairs, ccp.counts):
        print(f"{pair}: {count}")
    print("--------------------------------")
    reduced = ccp.reduce(ccp.pairs, ccp.counts)
    for pair, count in reduced.items():
        print(f"{pair}: {count}")
    print("\n\n--------------------------------\n\n")

def test_cross_correlation_stripes():
    text = 'hello world hello hdfs hello mapreduce hello big data hello data'
    items = text.split(' ')
    ccs = CrossCorrelationStripes()
    ccs.map(items)
    for item, stripe in zip(ccs.items, ccs.stripes):
        print(f"{item}: {stripe}")
    print("--------------------------------")
    reduced = ccs.reduce(ccs.items, ccs.stripes)
    for item, stripe in reduced.items():
        print(f"{item}: {stripe}")

if __name__ == "__main__":
    client = InsecureClient('http://localhost:50070', user='kupriyanovvn')
    print("Connected to HDFS successfully.", end='/n--------------------------------/n')

    
