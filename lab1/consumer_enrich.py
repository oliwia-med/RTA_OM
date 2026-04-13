from kafka import KafkaConsumer
import json

#konsument z unikalnym group_id
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enricher-group', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Oceniam ryzyko transakcji...")

for message in consumer:
    tx = message.value
    
    # klasyfikacja transakcji
    if tx['amount'] > 3000:
        tx['risk_level'] = "HIGH"
    elif tx['amount'] > 1000:
        tx['risk_level'] = "MEDIUM"
    else:
        tx['risk_level'] = "LOW"     
    # kwalifikacja transakcji
    print(f"[{tx['risk_level']:>6}] ID: {tx['tx_id']} | Kwota: {tx['amount']:.2f} | Sklep: {tx['store']}")
