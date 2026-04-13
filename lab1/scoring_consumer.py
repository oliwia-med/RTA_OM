from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

# definicja funkcji scorinowej
def score_transaction(tx):
    score = 0
    rules = []
    if tx['amount'] > 3000:
        score += 3
        rules.append("R1")
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append("R2")
    dt = datetime.fromisoformat(tx['timestamp'])
    if dt.hour < 6:
        score += 2
        rules.append("R3")
    return score, rules

#konsument czytający transakcje
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#producent wysyłający alerty
alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("System scoringowy aktywny")

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    if score >= 3:
       #wynik scoringowy
        tx['fraud_score'] = score
        tx['triggered_rules'] = rules
        
        # wysłanie do oddzielnego tematu alerts
        alert_producer.send('alerts', value=tx)
        
        print(f"!ALERT WYSŁANY! ID: {tx['tx_id']} | Score: {score} | Reguły: {rules}")
    else:
        print(f"Transakcja {tx['tx_id']} zweryfikowana pozytywnie (Score: {score})")
