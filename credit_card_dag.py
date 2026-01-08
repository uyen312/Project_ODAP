from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Định nghĩa DAG
with DAG(
    'credit_card_pipeline_daily',
    default_args=default_args,
    description='Lập lịch tổng hợp dữ liệu từ HDFS sang CSV cho Power BI hàng ngày',
    schedule_interval='@daily', 
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Kiểm tra kết nối Hadoop HDFS xem có hoạt động không
    check_hadoop = BashOperator(
        task_id='check_hdfs_connection',
        bash_command='hdfs dfs -ls /credit_card_transactions/data'
    )

    # Task 2: Chay Spark Batch 
    # Cong viec: Doc Parquet tu HDFS -> Hop thanh 1 file -> Luu CSV ra local
    prepare_csv_for_bi = BashOperator(
        task_id='convert_parquet_to_csv',
        bash_command="""
        spark-submit --master local[*] -e "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Đọc toàn bộ dữ liệu từ folder data mà Spark Streaming đã ghi ra
df = spark.read.parquet('hdfs://localhost:9000/credit_card_transactions/data/')
# Gộp lại thành 1 file csv duy nhất và lưu đè vào thư mục export
df.coalesce(1).write.mode('overwrite').option('header', 'true').csv('file:///home/ubuntu/Project_ODAP/powerbi_export')
spark.stop()
        "
        """
    )

    # Thiết lập thứ tự chạy
    check_hadoop >> prepare_csv_for_bi 