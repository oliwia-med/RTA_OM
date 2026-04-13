from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#stan startowy dla statystyk
stats = defaultdict(lambda: {
    'count': 0, 
    'total': 0.0, 
    'min': float('inf'), 
    'max': float('-inf')
})

msg_count = 0

print("analiza statystyk dla kategorii...")

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']
    
    # pobranie statystyk
    s = stats[cat]
    
    # aktualizowanie stanu
    s['count'] += 1
    s['total'] += amt
    s['min'] = min(s['min'], amt)
    s['max'] = max(s['max'], amt) 
    
    msg_count += 1
    
    # podsumowanie co 10 transakcji
    if msg_count % 10 == 0:
        print(f"\n--- RAPORT KATEGORII (po {msg_count} transakcji) ---")
        header = f"{'Kategoria':<12} | {'Ilość':<5} | {'Suma':<9} | {'Min':<8} | {'Max':<8}"
        print(header)
        print("-" * len(header))
        
        for category in sorted(stats.keys()):
            d = stats[category]
            print(f"{category:<12} | {d['count']:<5} | {d['total']:>9.2f} | {d['min']:>8.2f} | {d['max']:>8.2f}")
