from pyspark.sql import SparkSession
import sys

def main():
    spark = SparkSession.builder.appName("CSV Test").getOrCreate()
    
    # Ruta relativa dentro del contenedor (montada)
    csv_path = "data/test_data/sample.csv"
    parquet_path = "data/processed/sample_parquet"
    
    # Leer CSV
    df = spark.read.option("header", "true").csv(csv_path)
    
    # Mostrar esquema y algunas filas
    df.printSchema()
    df.show()
    
    # Comprobar número de filas (assert)
    count = df.count()
    assert count == 3, f"Expected 3 rows but got {count}"
    
    # Guardar como Parquet
    df.write.mode("overwrite").parquet(parquet_path)
    
    print(f"Parquet saved to {parquet_path}")
    spark.stop()

if __name__ == "__main__":
    main()
