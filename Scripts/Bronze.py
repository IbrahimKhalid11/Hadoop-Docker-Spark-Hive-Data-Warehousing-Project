import os
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("CSV_to_Parquet") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    try:
        # Get input and output paths from environment variables or command-line arguments
        local_csv_folder = os.getenv("LOCAL_CSV_FOLDER", "/Input_Data")
        hdfs_bronze_path = os.getenv("HDFS_BRONZE_PATH", "hdfs://namenode:8020/data/bronze/")

        if not os.path.exists(local_csv_folder):
            print(f"Error: CSV folder not found at {local_csv_folder}", file=sys.stderr)
            sys.exit(1)

        print(f"Processing CSV files from: {local_csv_folder}")
        print(f"Storing Parquet files in HDFS: {hdfs_bronze_path}")

        # Process each CSV file in the directory
        for file in os.listdir(local_csv_folder):
            if file.endswith(".csv"):
                local_file_path = os.path.join(local_csv_folder, file)
                print(f"Processing file: {local_file_path}")

                # Read CSV file into DataFrame
                df = spark.read.option("header", "true").csv(f"file://{local_file_path}")

                # Define Parquet file path in HDFS
                parquet_filename = file.replace(".csv", "_parquet")
                hdfs_parquet_path = f"{hdfs_bronze_path}{parquet_filename}"

                # Write DataFrame to HDFS as Parquet
                df.write.mode("overwrite").parquet(hdfs_parquet_path)
                print(f"Converted {file} to Parquet and stored in HDFS: {hdfs_parquet_path}")

        print("All CSV files processed and stored as Parquet in HDFS.")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()