import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, expr, lit
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np

# Cấu hình
INPUT_NO_FRAUD = "hdfs://localhost:9000/credit_card_transactions/data/"
INPUT_FRAUD = "hdfs://localhost:9000/credit_card_transactions/fraud/"
# WAREHOUSE_PATH = "hdfs://localhost:9000/user/hive/warehouse"
OUTPUT_PATH = "hdfs://localhost:9000/credit_card_transactions/analytics/"
DATABASE_NAME = "credit_card_analytics"

# Parse tham số dòng lệnh
parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="YYYY-MM-DD")
args = parser.parse_args()

# Khoảng thời gian
process_date_str = args.date
# theo hướng thống kê tích lũy  
process_date_start = datetime.strptime("2002-09-01", "%Y-%m-%d")
# theo hướng thông kê theo ngày mới nhất
# process_date_start = datetime.strptime(process_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
process_date_end = datetime.strptime(process_date_str, "%Y-%m-%d")
print(f"Khoảng thời gian: {process_date_start} đến {process_date_end}")

# SPARK SESSION
# spark = SparkSession.builder \
#     .appName(f"Analytics {process_date_str}") \
#     .getOrCreate() # Chỉ cần gọi getOrCreate() là đủ
# spark.sparkContext.setLogLevel("WARN")

spark = SparkSession.builder \
    .appName(f"Write Tables Directly For Tableau") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

# Tải và lọc dữ liệu theo ngày
df_no_fraud = spark.read.parquet(INPUT_NO_FRAUD)
df_no_fraud_filtered = df_no_fraud.filter(
    (col("transaction_date") >= process_date_start) & 
    (col("transaction_date") <= process_date_end)
)
# Nếu không có dữ liệu mới, dừng job
if df_no_fraud.rdd.isEmpty():
    print(f"No data for date {process_date_str}, stopping job.")
    spark.stop()
    exit(0)

df_fraud = spark.read.parquet(INPUT_FRAUD)
df_fraud = df_fraud.withColumn(
    "transaction_date",
    expr("""
        try_to_timestamp(
            concat_ws('/', day, month, year),
            'dd/MM/yyyy'
        )
    """)
)
df_fraud_filtered = df_fraud.filter(
    (col("transaction_date") >= process_date_start) & 
    (col("transaction_date") <= process_date_end)
)

# Chuyển thành dataframe pandas
pdf_no_fraud = df_no_fraud_filtered.toPandas()
pdf_fraud = df_fraud_filtered.toPandas() 

# |user|card|year|month|day|time |amount|use_chip|merchant_name|merchant_city|merchant_state|zip|mcc |errors|is_fraud|transaction_date|amount_vnd|
# Thống kê và phân tích
def analyze_by_user(pdf_no_fraud: pd.DataFrame, pdf_fraud: pd.DataFrame) -> pd.DataFrame:
    """
    Phân tích theo user
    """
    
    # Thresholds
    COUNT_THRESHOLD_7D_TRANSACTIONS = 5 # Ngưỡng: Có 5 giao dịch trong cửa sổ
    ROLLING_WINDOW_7D = '7D' # Cửa sổ trượt: 7 ngày gần nhất
    
    # 1. Chuẩn bị cho Rolling Window (chỉ áp dụng cho NO-FRAUD)
    pdf_no_fraud_sorted = pdf_no_fraud.sort_values(by=['user', 'transaction_date']).copy()
    pdf_indexed = pdf_no_fraud_sorted.set_index('transaction_date') 
    
    # 2. Đếm tổng số giao dịch trong 7 NGÀY
    pdf_no_fraud_sorted['transactions_in_7d'] = pdf_indexed \
        .groupby('user')['amount_vnd'] \
        .rolling(ROLLING_WINDOW_7D) \
        .count() \
        .reset_index(level=0, drop=True)
    
    # 3. Tổng hợp dữ liệu NO-FRAUD theo user
    result_no_fraud = pdf_no_fraud_sorted.groupby('user').agg(
        # Đếm tổng số lỗi
        error_count=('errors', lambda x: x.notna().sum()), 
        # Lấy giá trị max rolling count trong 7 ngày
        current_transactions_7d=('transactions_in_7d', 'max') 
    ).reset_index()
    
    # Đếm số lượng giao dịch fraud (size) cho mỗi user
    result_fraud = pdf_fraud.groupby('user').agg(
        fraud_count=('amount_vnd', 'size')
    ).reset_index()
    
    # 4. Gộp Dữ liệu & chuẩn hóa
    final_df = result_no_fraud.merge(
        result_fraud, 
        on='user', 
        how='outer'
    )
    final_df['error_count'] = final_df['error_count'].fillna(0)
    final_df['fraud_count'] = final_df['fraud_count'].fillna(0)
    final_df['current_transactions_7d'] = final_df['current_transactions_7d'].fillna(0)
    
    # 6. Tạo cột Phân loại Khối lượng cao (high volume)
    # Cột này chỉ cần kiểm tra sau khi điền 0, vì người chỉ có fraud sẽ có current_transactions_7d = 0
    final_df['is_high_volume_7d'] = (
        final_df['current_transactions_7d'] >= COUNT_THRESHOLD_7D_TRANSACTIONS
    )
    
    # 7. Tính TRUNG BÌNH TOÀN BỘ (trên tất cả người dùng sau khi điền 0)
    # Đây chính là cách tính: Tổng Lỗi / Tổng Người Dùng
    avg_error_system = final_df['error_count'].mean()
    avg_fraud_system = final_df['fraud_count'].mean() 
    
    # 8. Phân loại Vượt quá Trung bình
    final_df['error_beyond_avg'] = final_df['error_count'] > avg_error_system
    final_df['fraud_beyond_avg'] = final_df['fraud_count'] > avg_fraud_system
    
    # 9. Chọn cột đầu ra
    result_df = final_df[['user', 
                          'current_transactions_7d', 'is_high_volume_7d', 
                          'error_beyond_avg', 'fraud_beyond_avg']]
    
    return result_df

df_user = analyze_by_user(pdf_no_fraud.copy(), pdf_fraud.copy())

# Lọc ra các giao dịch thành công (không lỗi) để thực hiện tính toán cho báo cáo kinh doanh
pdf_no_fraud = pdf_no_fraud[pdf_no_fraud['error'].isna()]

def analyze_by_hour(filter_threshold, pdf_no_fraud: pd.DataFrame, pdf_fraud: pd: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo giờ.
    """
    
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_no_fraud['amount_vnd'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold=filter_threshold

    # lọc ra các giao dịch thành công 

    # 2. Tính các chỉ số
    result_df = pdf_no_fraud.groupby('hour').agg(
        # Đếm tổng giao dịch
        transaction_count=('amount_vnd', 'size'), 
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount_vnd', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    result_fraud_df = pdf_fraud.groupby('hour').agg(transaction_count=('amount_vnd', 'size')).reset_index()

    # 4. Tính toán Fraud Rate 
    result_df['fraud_rate'] = result_fraud_df['transaction_count'] / (result_df['transaction_count'] + result_fraud_df['transaction_count']) * 100
    
    result_df = result_df[["hour", "transaction_count", "fraud_rate", "count_big_amount"]]    
    return result_df

df_hour = analyze_by_hour(5000000, pdf_no_fraud.copy(), pdf_fraud.copy())

def analyze_by_merchant(threshold, pdf_no_fraud: pd.DataFrame, pdf_fraud: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo merchant.
    """
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_no_fraud['amount_vnd'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold = threshold

    # 2.Tính toán tất cả các chỉ số
    result_df = pdf_no_fraud.groupby('merchant_name').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount_vnd', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount_vnd', 'sum'),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount_vnd', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    result_fraud_df = pdf_fraud.groupby('merchant_name').agg(transaction_count=('amount_vnd', 'size')).reset_index()

    # 3. Tính toán Fraud Rate 
    result_df['fraud_rate'] = result_fraud_df['transaction_count'] / (result_df['transaction_count'] + result_fraud_df['transaction_count']) * 100
    
    result_df = result_df[["merchant_name", "transaction_count", "sum_amount", "fraud_rate", "count_big_amount"]]    
    return result_df

df_merchant = analyze_by_merchant(5000000, pdf_no_fraud.copy(), pdf_fraud.copy())

def analyze_by_city(threshold, pdf_no_fraud: pd.DataFrame, pdf_fraud: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo city.
    """
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_no_fraud['amount_vnd'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold = threshold

    # 2.Tính toán tất cả các chỉ số
    result_df = pdf_no_fraud.groupby('merchant_city').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount_vnd', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount_vnd', 'sum'),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount_vnd', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    result_fraud_df = pdf_fraud.groupby('merchant_city').agg(transaction_count=('amount_vnd', 'size')).reset_index()

    # 3. Tính toán Fraud Rate 
    result_df['fraud_rate'] = result_fraud_df['transaction_count'] / (result_df['transaction_count'] + result_fraud_df['transaction_count']) * 100
    
    result_df = result_df[["merchant_city", "transaction_count", "sum_amount", "fraud_rate", "count_big_amount"]]    
    return result_df

df_city = analyze_by_city(5000000, pdf_no_fraud.copy(), pdf_fraud.copy())

def analyze_by_state(threshold, pdf_no_fraud: pd.DataFrame, pdf_fraud: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo state.
    """
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_no_fraud['amount_vnd'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold = threshold

    # 2.Tính toán tất cả các chỉ số
    result_df = pdf_no_fraud.groupby('merchant_state').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount_vnd', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount_vnd', 'sum'),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount_vnd', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    result_fraud_df = pdf_fraud.groupby('merchant_city').agg(transaction_count=('amount_vnd', 'size')).reset_index()

    # 3. Tính toán Fraud Rate 
    result_df['fraud_rate'] = result_fraud_df['transaction_count'] / (result_df['transaction_count'] + result_fraud_df['transaction_count']) * 100
    
    result_df = result_df[["merchant_state", "transaction_count", "sum_amount", "fraud_rate", "count_big_amount"]]    
    return result_df

df_state = analyze_by_state(5000000, pdf_no_fraud.copy(), pdf_fraud.copy())

def analyze_by_dayOfWeek(pdf_no_fraud: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo day of week
    """
    # 1. Trích xuất ngày trong tuần
    pdf_no_fraud['day_of_week'] = pdf_no_fraud['transaction_date'].dt.day_name()

    # 3.Tính toán tất cả các chỉ số
    result_df = pdf_no_fraud.groupby('day_of_week').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount_vnd', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount_vnd', 'sum')
    ).reset_index()
    
    result_df = result_df[["day_of_week", "transaction_count", "sum_amount"]]    
    return result_df

df_dayOfWeek = analyze_by_dayOfWeek(pdf_no_fraud.copy())



# # EXPORT CSV TO HADOOP
# def save_pandas_to_hdfs(pdf: pd.DataFrame, file_name: str, spark: SparkSession, base_path: str):
#     """
#     Chuyển đổi Pandas DataFrame sang Spark DataFrame và lưu dưới dạng CSV lên HDFS.
#     Spark sẽ tự động tạo thư mục con (ví dụ: df_user.csv) nếu nó chưa tồn tại.
#     Chúng ta sẽ ghi vào thư mục có tên tương ứng (ví dụ: df_user_summary)
#     """
    
#     # Tạo tên thư mục đầu ra (Không dùng .csv ở cuối)
#     output_dir = base_path + file_name.replace(".csv", "_summary")
    
#     # 1. Chuyển đổi Pandas DF sang Spark DF
#     try:
#         spark_df = spark.createDataFrame(pdf)
#     except Exception as e:
#         print(f"Lỗi khi chuyển đổi DataFrame {file_name}: {e}")
#         return

#     # 2. Ghi Spark DF lên HDFS (Spark tự tạo thư mục output_dir nếu chưa tồn tại)
#     print(f"Bắt đầu ghi dữ liệu {file_name} vào: {output_dir}")
    
#     spark_df.write.mode("overwrite").option("header", "true").option("delimiter", ",").csv(output_dir) 
        
#     print(f"Hoàn thành ghi {file_name}")

# save_pandas_to_hdfs(df_user, "df_user.csv", spark, OUTPUT_PATH)
# save_pandas_to_hdfs(df_hour, "df_hour.csv", spark, OUTPUT_PATH)
# save_pandas_to_hdfs(df_merchant, "df_merchant.csv", spark, OUTPUT_PATH)
# save_pandas_to_hdfs(df_city, "df_city.csv", spark, OUTPUT_PATH)
# save_pandas_to_hdfs(df_state, "df_state.csv", spark, OUTPUT_PATH)
# save_pandas_to_hdfs(df_dayOfWeek, "df_dayOfWeek.csv", spark, OUTPUT_PATH)

# EXPORT DIRECTLY TO HIVE TABLES FOR TABLEAU
def save_pandas_to_table(
    pdf: pd.DataFrame, 
    table_name: str, 
    spark: SparkSession, 
    database_name: str
):
    """
    Chuyển đổi Pandas DataFrame sang Spark DataFrame và lưu trực tiếp vào bảng Hive.
    """
    
    # 1. Chuyển đổi Pandas DF sang Spark DF
    try:
        spark_df = spark.createDataFrame(pdf)
    except Exception as e:
        print(f"Lỗi khi chuyển đổi DataFrame {table_name}: {e}")
        return

    # 2. Ghi Spark DF vào bảng Hive (Tạo bảng nếu chưa tồn tại)
    full_table_name = f"{database_name}.{table_name}"
    print(f"Bắt đầu ghi dữ liệu vào bảng Hive: {full_table_name}")
    
    spark_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("path", "hdfs://localhost:9000/credit_card_transactions/analytics/" + table_name) \
        .saveAsTable(full_table_name)        
    print(f"Hoàn thành ghi vào bảng {full_table_name}")

save_pandas_to_table(df_user, "df_user", spark, DATABASE_NAME)
save_pandas_to_table(df_hour, "df_hour", spark, DATABASE_NAME)
save_pandas_to_table(df_merchant, "df_merchant", spark, DATABASE_NAME)
save_pandas_to_table(df_city, "df_city", spark, DATABASE_NAME)
save_pandas_to_table(df_state, "df_state", spark, DATABASE_NAME)
save_pandas_to_table(df_dayOfWeek, "df_day_of_week", spark, DATABASE_NAME)

# OPTIONAL: export to csv
df_user.to_csv("user_aggreation.csv", index=False)
df_hour.to_csv("hour_aggreation.csv", index=False)
df_merchant.to_csv("merchant_aggreation.csv", index=False)
df_city.to_csv("city_aggreation.csv", index=False)
df_state.to_csv("state_aggreation.csv", index=False)

views = {
    "df_user": "v_user",
    "df_hour": "v_hour",
    "df_merchant": "v_merchant",
    "df_city": "v_city",
    "df_state": "v_state",
    "df_day_of_week": "v_day_of_week"
}

for table, view in views.items():
    spark.sql(f"""
    CREATE OR REPLACE VIEW {DATABASE_NAME}.{view} AS
    SELECT *
    FROM {DATABASE_NAME}.{table}
    """)

print("DONE!")

spark.stop()