from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# stan początkowy
store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

print("agregacja danych per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    # aktualizowanie stanu
    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    # co 10 transakcji tabela podsumowująca
    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (po {msg_count} transakcjach) ---")
        print(f"{'Sklep':<12} | {'Liczba':<7} | {'Suma':<10} | {'Średnia':<8}")
        print("-" * 50)
        
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            suma = total_amount[s]
            srednia = suma / count
            print(f"{s:<12} | {count:<7} | {suma:>9.2f} | {srednia:>8.2f}")
        print("-" * 50)
