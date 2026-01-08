import pandas as pd
import json
import time
import random
import os
from kafka import KafkaProducer


# CẤU HÌNH
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "credit_card_transactions"
CSV_FILE = "User0_credit_card_transactions.csv"
OFFSET_FILE = "producer_offset.txt"


# KHỞI TẠO PRODUCER
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3
)


# ĐỌC FILE CSV
df = pd.read_csv(CSV_FILE)

print(f"Total transactions: {len(df)}")

# ĐỌC OFFSET NẾU CÓ
if os.path.exists(OFFSET_FILE):
    with open(OFFSET_FILE, "r") as f:
        offset = int(f.read().strip())
    print(f"Resuming from offset: {offset}")
else:
    offset = 0
    print("Starting from the beginning.")

def to_py(x):
    if pd.isna(x):
        return None
    if hasattr(x, "item"):
        return x.item()
    return x

# GỬI TỪNG GIAO DỊCH
for i in range(offset, len(df)):
    row = df.iloc[i]

    raw_amount = str(row["Amount"]).replace("$", "").replace(",", "").strip()
    try:
        amount = float(raw_amount)
    except ValueError:
        amount = None

    # transaction = {
    #     "user": row["User"],
    #     "card": row["Card"],
    #     "year": int(row["Year"]),
    #     "month": int(row["Month"]),
    #     "day": int(row["Day"]),
    #     "time": row["Time"],
    #     "amount": amount,
    #     "use_chip": row["Use Chip"],
    #     "merchant_name": row["Merchant Name"],
    #     "merchant_city": row["Merchant City"],
    #     "merchant_state": row["Merchant State"],
    #     "zip": row["Zip"],
    #     "mcc": row["MCC"],
    #     "errors": row["Errors?"] if pd.notna(row["Errors?"]) else None,
    #     "is_fraud": row["Is Fraud?"]
    # }

    transaction = {
        "user": to_py(row["User"]),
        "card": to_py(row["Card"]),
        "year": int(row["Year"]),
        "month": int(row["Month"]),
        "day": int(row["Day"]),
        "time": to_py(row["Time"]),
        "amount": amount,
        "use_chip": to_py(row["Use Chip"]),
        "merchant_name": to_py(row["Merchant Name"]),
        "merchant_city": to_py(row["Merchant City"]),
        "merchant_state": to_py(row["Merchant State"]),
        "zip": to_py(row["Zip"]),
        "mcc": to_py(row["MCC"]),
        "errors": to_py(row["Errors?"]),
        "is_fraud": to_py(row["Is Fraud?"])
    }

    producer.send(TOPIC_NAME, value=transaction)
    print("Sent transaction:", transaction)

    with open(OFFSET_FILE, "w") as f:
        f.write(str(i + 1))
        
    time.sleep(random.randint(1, 5))


# ĐÓNG PRODUCER
producer.flush()
producer.close()

print("Finished sending all transactions.")