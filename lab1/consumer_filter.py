from kafka import KafkaConsumer
import json

#zainicjowanie konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',  #sprawdzanie od pierwszych transakcji żeby nie pominąć
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Czekam na duże transakcje (amount > 3000)...")

for message in consumer:
    #dane transakcji wyciagnięte z obiektu message
    tx = message.value
    # sprawdzenie warunku (ammount >3000)
    if tx['amount'] > 3000:
        #wyświetlanie alertu gdy spełniony warunek
        print(f"ALERT: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
