from pyspark.sql import SparkSession
import sys
import os

def verify_spark_s3_public():
    print("🚀 Iniciando verificación de Spark + S3 (Bucket Público)...")
    
    # Ya no necesitamos configurar JARs manualmente ni diagnósticos complejos
    # porque PYSPARK_SUBMIT_ARGS en el Dockerfile se encarga de todo.
    
    try:
        spark = (SparkSession.builder
            .appName("VerifyS3Public")
            # Configuración crítica para S3A
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Proveedor de credenciales anónimo para buckets públicos
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .getOrCreate())
        
        print(f"✅ Spark Session creada. Versión: {spark.version}")
        
        # Prueba de lectura de un archivo público
        public_s3_path = "s3a://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet"
        
        print(f"📂 Intentando leer archivo público: {public_s3_path}")
        
        # Leemos solo el esquema
        df = spark.read.parquet(public_s3_path)
        
        print("✅ Lectura exitosa!")
        print("📋 Esquema detectado:")
        df.printSchema()
        
        print("\n🎉 VERIFICACIÓN COMPLETADA CON ÉXITO: Tu entorno está listo y configurado automáticamente.")
        
    except Exception as e:
        print("\n❌ ERROR CRÍTICO DURANTE LA VERIFICACIÓN:")
        print(str(e))
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    verify_spark_s3_public()
