from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
def initialize_spark():
    spark = SparkSession.builder \
        .appName("Gold_Arch") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Debugging: Print the warehouse directory configuration
    print("Spark SQL Warehouse Directory:", spark.conf.get("spark.sql.warehouse.dir"))
    return spark

# Create Hive database if not exists
def create_hive_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("USE gold")

# Load data from HDFS
def load_parquet(spark, path):
    try:
        df = spark.read.parquet(path)
        print(f"Successfully loaded data from {path}")
        return df
    except Exception as e:
        print(f"Error loading data from {path}: {e}")
        raise

# Save DataFrame to Hive table
def save_to_hive(df, table_name, mode="overwrite"):
    try:
        df.write.format("hive").mode(mode).saveAsTable(table_name)
        print(f"Data saved to Hive table: {table_name}")
    except Exception as e:
        print(f"Error saving data to Hive table {table_name}: {e}")
        raise

# Save DataFrame to HDFS in Parquet format
def save_to_hdfs(df, hdfs_path, mode="overwrite"):
    try:
        df.write.mode(mode).parquet(hdfs_path)
        print(f"Data saved to HDFS at: {hdfs_path}")
    except Exception as e:
        print(f"Error saving data to HDFS at {hdfs_path}: {e}")
        raise

# Process Athlete Dimension
def process_athlete_dim(spark):
    hdfs_silver_path = "hdfs://namenode:8020/data/silver/Athlete_parquet"
    athlete = load_parquet(spark, hdfs_silver_path)
    save_to_hive(athlete, "gold.Athlete_Dim")
    save_to_hdfs(athlete, "hdfs://namenode:8020/data/gold/Athlete_parquet")

# Process Games Dimension
def process_games_dim(spark):
    hdfs_silver_path = "hdfs://namenode:8020/data/silver/OlympicsGames_parquet"
    games = load_parquet(spark, hdfs_silver_path)
    save_to_hive(games, "gold.Games_Dim")
    save_to_hdfs(games, "hdfs://namenode:8020/data/gold/OlympicsGames_parquet")

# Process Results Dimension
def process_results_dim(spark):
    hdfs_silver_path = "hdfs://namenode:8020/data/silver/OlympicResults_parquet"
    results = load_parquet(spark, hdfs_silver_path)
    save_to_hive(results, "gold.Results_Dim")
    save_to_hdfs(results, "hdfs://namenode:8020/data/gold/OlympicResults_parquet")

# Process Country Dimension
def process_country_dim(spark):
    hdfs_silver_path = "hdfs://namenode:8020/data/silver/OlympicsCountry_parquet"
    country = load_parquet(spark, hdfs_silver_path)
    save_to_hive(country, "gold.Country_Dim")
    save_to_hdfs(country, "hdfs://namenode:8020/data/gold/Country_parquet")

# Process Date Dimension
def process_date_dim(spark):
    hdfs_silver_path = "hdfs://namenode:8020/data/silver/Date_parquet"
    date_df = load_parquet(spark, hdfs_silver_path)
    save_to_hive(date_df, "gold.Date_Dim")
    save_to_hdfs(date_df, "hdfs://namenode:8020/data/gold/Date_parquet")

# Process Fact Table
def process_fact_table(spark):
    fact_path = "hdfs://namenode:8020/data/silver/Fact_Table_parquet"
    date_path = "hdfs://namenode:8020/data/silver/Date_parquet"

    fact_df = load_parquet(spark, fact_path)
    date_df = load_parquet(spark, date_path)

    fact_with_date_id = fact_df.join(
        date_df.select("Date_id"),
        fact_df["EditionID"] == date_df["Date_id"],
        how="left"
    )
    save_to_hive(fact_with_date_id, "gold.Olympics_Fact")
    save_to_hdfs(fact_with_date_id, "hdfs://namenode:8020/data/gold/Olympics_Fact_parquet")

# Process Business Logic Dimension
def process_business_logic_dim(spark):
    # Load required DataFrames
    fact_path = "hdfs://namenode:8020/data/silver/Fact_Table_parquet"
    athlete_path = "hdfs://namenode:8020/data/silver/Athlete_parquet"
    country_path = "hdfs://namenode:8020/data/silver/OlympicsCountry_parquet"
    games_path = "hdfs://namenode:8020/data/silver/OlympicsGames_parquet"
    date_path = "hdfs://namenode:8020/data/silver/Date_parquet"

    df_fact = load_parquet(spark, fact_path)
    df_athlete = load_parquet(spark, athlete_path)
    df_country = load_parquet(spark, country_path)
    df_games = load_parquet(spark, games_path)
    df_date = load_parquet(spark, date_path)

    # Drop duplicates and calculate medals
    df_unique_results = df_fact.dropDuplicates(["CountryNOC", "EditionID"])
    df_medals = df_unique_results.groupBy("CountryNOC", "EditionID").agg(
        sum("GoldMedals").alias("total_gold"),
        sum("SilverMedals").alias("total_silver"),
        sum("BronzeMedals").alias("total_bronze")
    )
    df_medals = df_medals.withColumn("total_medals", expr("total_gold + total_silver + total_bronze"))

    # Join with athlete data
    df_athlete = df_athlete.select(
        col("AthleteID").alias("bio_athlete_id"),
        "Name",
        "Sex",
        "Born",
        "Height",
        "Weight",
        "Nationality"
    )
    df_athletes_with_medals = df_fact.join(
        df_athlete,
        df_fact["AthleteID"] == df_athlete["bio_athlete_id"],
        "left_outer"
    )

    # Aggregate stats
    df_stats = df_athletes_with_medals.groupBy("CountryNOC", "EditionID").agg(
        count("AthleteID").alias("total_athletes"),
        avg("Height").alias("avg_height"),
        avg("Weight").alias("avg_weight"),
        sum(when(df_athlete["Sex"] == 1, 1).otherwise(0)).alias("male_athletes"),
        sum(when(df_athlete["Sex"] == 0, 1).otherwise(0)).alias("female_athletes")
    )
    df_stats = df_stats.select(
        col("CountryNOC").alias("NOC"),
        col("EditionID").alias("edition_id"),
        "total_athletes",
        "avg_height",
        "avg_weight",
        "male_athletes",
        "female_athletes"
    )

    # Join medals and other dimensions
    df_stats_with_medals = df_stats.join(
        df_medals,
        (df_stats["edition_id"] == df_medals["EditionID"]) & (df_stats["NOC"] == df_medals["CountryNOC"]),
        "inner"
    )
    df_stats_with_medals_participating_cnt = df_stats_with_medals.join(
        df_country,
        df_stats_with_medals["NOC"] == df_country["noc"],
        "inner"
    )
    df_games = df_games.withColumnRenamed("EditionID", "id")
    df_final = df_stats_with_medals_participating_cnt.join(
        df_games,
        df_stats_with_medals_participating_cnt["edition_id"] == df_games["id"],
        "inner"
    )
    df_final_with_date = df_final.join(
        df_date,
        df_final["edition_id"] == df_date["Date_id"],
        "inner"
    )

    # Final schema
    df_final_with_date = df_final_with_date.select(
        col("edition_id").alias("EditionID"),
        col("EditionName").alias("EditionName"),
        col("FlagURL").alias("FlagURL"),
        col("HostCountry").alias("HostCountry"),
        col("HostCity").alias("HostCity"),
        col("country").alias("ParticipateCountry"),
        col("year").alias("Year"),
        col("avg_height").alias("AverageHeight"),
        col("avg_weight").alias("AverageWeight"),
        col("male_athletes").alias("MaleAthletes"),
        col("female_athletes").alias("FemaleAthletes"),
        col("total_athletes").alias("TotalAthletes"),
        col("total_gold").alias("TotalGold"),
        col("total_silver").alias("TotalSilver"),
        col("total_bronze").alias("TotalBronze"),
        col("total_medals").alias("TotalMedals")
    )

    # Save to HDFS
    hdfs_gold_path = "hdfs://namenode:8020/data/gold/BusinessLogic_Parquet"
    save_to_hdfs(df_final_with_date, hdfs_gold_path)

    save_to_hive(df_final_with_date, "gold.BusinessLogic")
    

# Main function
if __name__ == "__main__":
    spark = initialize_spark()
    create_hive_database(spark)

    process_athlete_dim(spark)
    process_games_dim(spark)
    process_results_dim(spark)
    process_country_dim(spark)
    process_date_dim(spark)
    process_fact_table(spark)
    process_business_logic_dim(spark)

    spark.stop()