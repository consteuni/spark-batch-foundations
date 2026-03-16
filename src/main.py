from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("spark-batch-foundations")
        .master("local[*]")
        .getOrCreate()
    )


def read_sample_data(spark: SparkSession):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/sample/orders.csv")
    )


def run_basic_checks(df):
    print("\n=== SHOW DATA ===")
    df.show()

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== SELECT COLUMNS ===")
    df.select("order_id", "product_category", "amount").show()

    print("\n=== FILTER COMPLETED ORDERS > 50 ===")
    df.filter((col("status") == "completed") & (col("amount") > 50)).show()


def main():
    spark = create_spark_session()
    df = read_sample_data(spark)
    run_basic_checks(df)
    spark.stop()


if __name__ == "__main__":
    main()
