from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Function to create and configure a SparkSession
def create_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession configured for the application.

    Returns:
        SparkSession: A SparkSession object configured for local execution.
    """
    return (
        SparkSession.builder
        .appName("spark-batch-foundations")
        .master("local[*]")
        .getOrCreate()
    )

# Function to read sample data from a CSV file
def read_sample_data(spark: SparkSession):
    """
    Reads sample data from a CSV file and returns a DataFrame.

    Args:
        spark (SparkSession): The active SparkSession.

    Returns:
        DataFrame: A DataFrame containing the sample data.
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/sample/orders.csv")
    )

# Function to perform basic checks and transformations on the DataFrame
def run_basic_checks(df):
    """
    Performs basic operations on the DataFrame, such as displaying data,
    printing the schema, selecting specific columns, and filtering rows.

    Args:
        df (DataFrame): The DataFrame to process.
    """
    print("\n=== SHOW DATA ===")
    df.show()

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== SELECT COLUMNS ===")
    df.select("order_id", "product_category", "amount").show()

    print("\n=== FILTER COMPLETED ORDERS > 50 ===")
    df.filter((col("status") == "completed") & (col("amount") > 50)).show()

# Main function to orchestrate the Spark job
def main():
    """
    Main entry point for the Spark application. Creates a SparkSession,
    reads sample data, performs basic checks, and stops the SparkSession.
    """
    spark = create_spark_session()
    df = read_sample_data(spark)
    run_basic_checks(df)
    spark.stop()

# Entry point for the script
if __name__ == "__main__":
    main()
