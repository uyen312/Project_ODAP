import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, expr, lit
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np

# CONFIG
INPUT_NO_FRAUD = "hdfs://localhost:9000/credit_card_transactions/data/"
INPUT_FRAUD = "hdfs://localhost:9000/credit_card_transactions/fraud/"
# WAREHOUSE_PATH = "hdfs://localhost:9000/user/hive/warehouse"
OUTPUT_PATH = "hdfs://localhost:9000/credit_card_transactions/analytics/"
DATABASE_NAME = "credit_card_analytics"

# PARAMETER PARSING
parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="YYYY-MM-DD")
args = parser.parse_args()

# TIME RANGE
process_date_str = args.date
# theo hướng thống kê tích lũy  
process_date_start = datetime.strptime("2002-09-01", "%Y-%m-%d")
# theo hướng thông kê theo ngày mới nhất
# process_date_start = datetime.strptime(process_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
process_date_end = datetime.strptime(process_date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S")
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

# LOAD & FILTER DATA BY TIME RANGE
df_no_fraud = spark.read.parquet(INPUT_NO_FRAUD)
df_no_fraud_filtered = df_no_fraud.filter(
    (col("transaction_datetime") >= process_date_start) & 
    (col("transaction_datetime") <= process_date_end)
)

df_fraud = spark.read.parquet(INPUT_FRAUD)
# tạo cột mới là transaction_datetime từ year, month, day, time (time có dạng hh:mm)
df_fraud = df_fraud.withColumn(
    "transaction_datetime",
    expr("""
        try_to_timestamp(
            concat_ws(' ', concat_ws('-', year, month, day), concat(time, ':00')),
            'yyyy-M-d HH:mm:ss'
        )
    """)
)
df_fraud = df_fraud.withColumn(
    "amount_vnd",
    lit(None).cast(DoubleType())
)
df_fraud_filtered = df_fraud.filter(
    (col("transaction_datetime") >= process_date_start) & 
    (col("transaction_datetime") <= process_date_end)
)

# COMBINE 2 DATAFRAMES
df_combined = df_no_fraud_filtered.unionByName(df_fraud_filtered)

if df_combined.rdd.isEmpty():
    print(f"No data for date {process_date_str}, stopping job.")
    spark.stop()
    exit(0)
    
# CONVERT TO PANDAS FOR ANALYSIS
pdf_combined = df_combined.toPandas()
# |user|card|year|month|day|time |amount|use_chip|merchant_name|merchant_city|merchant_state|zip|mcc |errors|is_fraud|transaction_datetime|amount_vnd|


# ANALYTICS
def analyze_by_hour(filter_threshold, pdf_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo giờ.
    input: 
        threshold: ngưỡng lọc giá trị lớn

    output: 
        dataframe (hour, transaction_count, fraud_rate, count_big_amount)
    """
    # 1. Trích xuất giờ giao dịch
    pdf_combined['hour'] = pdf_combined['transaction_datetime'].dt.hour
    
    # 2. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_combined['amount'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold=filter_threshold

    # 3. Sử dụng groupby() và agg() để tính toán tất cả các chỉ số
    result_df = pdf_combined.groupby('hour').agg(
        # Đếm tổng giao dịch
        transaction_count=('amount', 'size'), 
        
        # Đếm số lần gian lận (is_fraud = Yes)
        fraud_count=('is_fraud', lambda x: (x == 'Yes').sum()),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    # 4. Tính toán Fraud Rate 
    result_df['fraud_rate'] = (result_df['fraud_count'] / result_df['transaction_count']) * 100
    
    result_df = result_df[["hour", "transaction_count", "fraud_rate", "count_big_amount"]]    
    return result_df

df_hour = analyze_by_hour(2000, pdf_combined.copy())

def analyze_by_merchant(threshold, pdf_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo merchant.
    input: 
        threshold: ngưỡng lọc giá trị lớn

    output: 
        dataframe (merchant_name, transaction_count, sum_amount, fraud_rate, count_big_amount)
    """
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_combined['amount'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold = threshold

    # 2.Tính toán tất cả các chỉ số
    result_df = pdf_combined.groupby('merchant_name').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount', 'sum'),
        
        # Đếm số lần gian lận (is_fraud = Yes)
        fraud_count=('is_fraud', lambda x: (x == 'Yes').sum()),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    # 3. Tính toán Fraud Rate 
    result_df['fraud_rate'] = (result_df['fraud_count'] / result_df['transaction_count']) * 100
    
    result_df = result_df[["merchant_name", "transaction_count", "sum_amount", "fraud_rate", "count_big_amount"]]    
    return result_df

df_merchant = analyze_by_merchant(2000, pdf_combined.copy())

def analyze_by_city(threshold, pdf_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo city.
    input: 
        threshold: ngưỡng lọc giá trị lớn

    output: 
        dataframe (merchant_city, transaction_count, sum_amount, fraud_rate, count_big_amount)
    """
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_combined['amount'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold = threshold

    # 2.Tính toán tất cả các chỉ số
    result_df = pdf_combined.groupby('merchant_city').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount', 'sum'),
        
        # Đếm số lần gian lận (is_fraud = Yes)
        fraud_count=('is_fraud', lambda x: (x == 'Yes').sum()),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    # 3. Tính toán Fraud Rate 
    result_df['fraud_rate'] = (result_df['fraud_count'] / result_df['transaction_count']) * 100
    
    result_df = result_df[["merchant_city", "transaction_count", "sum_amount", "fraud_rate", "count_big_amount"]]    
    return result_df

df_city = analyze_by_city(5000000, pdf_combined.copy())

def analyze_by_state(threshold, pdf_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo state.
    input: 
        threshold: ngưỡng lọc giá trị lớn

    output: 
        dataframe (merchant_state, transaction_count, sum_amount, fraud_rate, count_big_amount)
    """
    # 1. Định nghĩa Threshold cho giao dịch có giá trị lớn
    # Nếu muốn dùng percentile của TOÀN BỘ dữ liệu:
    # threshold = pdf_combined['amount'].quantile(<quantile>) 
    
    # Nếu dùng giá trị cố định:
    filter_threshold = threshold

    # 2.Tính toán tất cả các chỉ số
    result_df = pdf_combined.groupby('merchant_state').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount', 'sum'),
        
        # Đếm số lần gian lận (is_fraud = Yes)
        fraud_count=('is_fraud', lambda x: (x == 'Yes').sum()),
        
        # Đếm số giao dịch lớn
        count_big_amount=('amount', lambda x: (x > filter_threshold).sum())
    ).reset_index()

    # 3. Tính toán Fraud Rate 
    result_df['fraud_rate'] = (result_df['fraud_count'] / result_df['transaction_count']) * 100
    
    result_df = result_df[["merchant_state", "transaction_count", "sum_amount", "fraud_rate", "count_big_amount"]]    
    return result_df

df_state = analyze_by_state(2000, pdf_combined.copy())

def analyze_by_dayOfWeek(pdf_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Tối ưu phân tích theo day of week.
    input: 
        threshold: ngưỡng lọc giá trị lớn

    output: 
        dataframe (day_of_week, transaction_count, sum_amount)
    """
    # 1. Trích xuất ngày trong tuần
    pdf_combined['day_of_week'] = pdf_combined['transaction_datetime'].dt.day_name()

    # 3.Tính toán tất cả các chỉ số
    result_df = pdf_combined.groupby('day_of_week').agg(
        # Đếm số lượng giao dịch
        transaction_count=('amount', 'size'), 

        # Tính tổng giá trị giao dịch 
        sum_amount=('amount', 'sum')
    ).reset_index()
    
    result_df = result_df[["day_of_week", "transaction_count", "sum_amount"]]    
    return result_df

df_dayOfWeek = analyze_by_dayOfWeek(pdf_combined.copy())

def analyze_by_user(pdf_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Phân tích theo User, sử dụng Cửa sổ Trượt 24 Giờ (1 Ngày) để phát hiện hành vi giao dịch nhanh.
    """
    
    # Threshold
    TIME_THRESHOLD_SECONDS = 180 # Giao dịch nhanh: cách nhau dưới 180 giây
    COUNT_THRESHOLD_FAST_TRANSACTIONS = 5 # Nhiều: Có 5 giao dịch nhanh trong cửa sổ
    ROLLING_WINDOW = '15Min' # Cửa sổ trượt: 24 giờ gần nhất
    
    # 1.Nhóm theo user và time để tính diff
    pdf_combined = pdf_combined.sort_values(by=['user', 'transaction_datetime'])
    pdf_combined['time_diff'] = pdf_combined.groupby('user')['transaction_datetime'].diff().dt.total_seconds().fillna(np.inf)
    
    # 2. Tạo cờ boolean cho từng giao dịch: True nếu time_diff < 60s
    pdf_combined['is_fast'] = pdf_combined['time_diff'] < TIME_THRESHOLD_SECONDS
    
    # 3. Áp dụng Rolling Window: Đếm tổng số giao dịch 'is_fast' trong 24H tính tới giao dịch hiện tại
    pdf_indexed = pdf_combined.set_index('transaction_datetime')
    pdf_combined['fast_transactions'] = pdf_indexed \
        .groupby('user')['is_fast'] \
        .rolling(ROLLING_WINDOW) \
        .sum() \
        .reset_index(level=0, drop=True)
    
    # 4. Tổng hợp dữ liệu (Chỉ lấy giá trị Rolling Window ở giao dịch MỚI NHẤT của mỗi User)
    result_df = pdf_combined.groupby('user').agg(
        total_transactions=('amount', 'size'), 
        error_count=('errors', lambda x: x.notna().sum()),
        fraud_count=('is_fraud', lambda x: (x == 'Yes').sum()),
        
        # Lấy giá trị rolling count tại giao dịch cuối cùng của user đó
        fast_transactions=('fast_transactions', 'last') 
        
    ).reset_index()
    
    # Số lượng giao dịch nhanh có lớn hơn Ngưỡng 5 (COUNT_THRESHOLD) trong 24H không?
    result_df['is_suspiciously_fast'] = (
        result_df['fast_transactions'] >= COUNT_THRESHOLD_FAST_TRANSACTIONS
    )

    # 5. Phân tích cuối
    avg_error = result_df['error_count'].mean()
    avg_fraud = result_df['fraud_count'].mean()
    
    result_df['error_beyond_avg'] = result_df['error_count'] > avg_error
    result_df['fraud_beyond_avg'] = result_df['fraud_count'] > avg_fraud
    
    result_df = result_df[['user', 
                           'fast_transactions', 'is_suspiciously_fast', 
                           'error_beyond_avg', 'fraud_beyond_avg']]
    
    return result_df

df_user = analyze_by_user(pdf_combined.copy())

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