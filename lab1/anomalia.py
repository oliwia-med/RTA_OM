from kafka import KafkaConsumer
import json
import time
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest', #spojrzenie na ostatnie transakcje
    group_id='anomalia-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# słownik logowań dla każdego użytkownika
user_timestamps = defaultdict(list)

print("System wyszukiwania anomalii uruchomiony")

for message in consumer:
    tx = message.value
    u_id = tx['user_id']
    now = time.time() #aktualny czas w sekundach
    
    # dodanie czasu obecnej transakcji do listy użytkownika
    user_timestamps[u_id].append(now)
    
    # czyszczenie listy z transakcji starszych niż 1 minuta
    user_timestamps[u_id] = [t for t in user_timestamps[u_id] if now - t <= 60]
    
    # warunek anomalii: więcej niż 3 transakcje w ciągu 1 minuty
    count = len(user_timestamps[u_id])
    if count > 3:
        print(f"!!! ALARM ANOMALII !!!")
        print(f"Liczba zdarzeń: {count} dla użytkownika: {u_id} w ciągu ostatnich 60s!")
        print(f"Ostatnia transakcja: {tx['tx_id']} | Kwota: {tx['amount']} | Sklep: {tx['store']}")
        print("-" * 40)
