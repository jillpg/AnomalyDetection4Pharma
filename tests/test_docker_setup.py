from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("CSV Test") \
        .master("local[*]") \
        .getOrCreate()

    csv_path = "data/test_data/sample.csv"
    parquet_path = "/data/processed/sample_parquet"

    df = spark.read.option("header", "true").csv(csv_path)

    df.printSchema()
    df.show()

    count = df.count()
    assert count == 3, f"Expected 3 rows but got {count}"

    df.write.mode("overwrite").parquet(parquet_path)
    print(f"Parquet saved to {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    main()
