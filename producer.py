import pandas as pd
import json
import time
import random
from kafka import KafkaProducer


# CẤU HÌNH
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "credit_card_transactions"
CSV_FILE = "User0_credit_card_transactions.csv"


# KHỞI TẠO PRODUCER
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3
)


# ĐỌC FILE CSV
df = pd.read_csv(CSV_FILE)

print(f"Total transactions: {len(df)}")


# GỬI TỪNG GIAO DỊCH
for _, row in df.iterrows():
    raw_amount = str(row["Amount"]).replace("$", "").replace(",", "").strip()
    try:
        amount = float(raw_amount)
    except ValueError:
        amount = None

    transaction = {
        "user": row["User"],
        "card": row["Card"],
        "year": int(row["Year"]),
        "month": int(row["Month"]),
        "day": int(row["Day"]),
        "time": row["Time"],
        "amount": amount,
        "use_chip": row["Use Chip"],
        "merchant_name": row["Merchant Name"],
        "merchant_city": row["Merchant City"],
        "merchant_state": row["Merchant State"],
        "zip": row["Zip"],
        "mcc": row["MCC"],
        "errors": row["Errors?"] if pd.notna(row["Errors?"]) else None,
        "is_fraud": row["Is Fraud?"]
    }

    producer.send(TOPIC_NAME, value=transaction)
    print("Sent transaction:", transaction)

    time.sleep(random.randint(1, 5))


# ĐÓNG PRODUCER
producer.flush()
producer.close()

print("Finished sending all transactions.")