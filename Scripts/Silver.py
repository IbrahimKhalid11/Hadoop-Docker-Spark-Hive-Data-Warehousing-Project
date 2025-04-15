from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, regexp_extract, to_date, trim, dayofmonth, month

spark = SparkSession.builder \
    .appName("Silver_Arch") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def load_parquet(path):
    """Load a Parquet file from HDFS."""
    try:
        df = spark.read.parquet(path)
        print(f"Successfully loaded data from {path}")
        return df
    except Exception as e:
        print(f"Error loading data from {path}: {e}")
        raise

def save_as_parquet(df, path):
    """Save a DataFrame as a Parquet file to HDFS."""
    try:
        df.write.mode("overwrite").parquet(path)
        print(f"Data saved successfully to {path}")
    except Exception as e:
        print(f"Error saving data to {path}: {e}")
        raise

def transform_athlete_dim():
    """Transform and load Athlete Dimension."""
    athlete_path = "hdfs://namenode:8020/data/bronze/Olympic_Athlete_Bio_parquet"
    athlete_df = load_parquet(athlete_path)

    # Data transformations
    athlete_df = athlete_df.withColumn("athlete_id", col("athlete_id").cast("int")) \
                           .withColumn("height", col("height").cast("int")) \
                           .withColumn("weight", col("weight").cast("int")) \
                           .withColumn("sex", when(col("sex") == "Male", 1).otherwise(0)) \
                           .drop("country_noc", "description", "special_notes") \
                           .na.drop(subset=["born"])

    # Fill missing values with averages
    avg_height = athlete_df.select(avg(col("height"))).collect()[0][0]
    avg_weight = athlete_df.select(avg(col("weight"))).collect()[0][0]
    athlete_df = athlete_df.fillna({"height": avg_height, "weight": avg_weight})

    # Convert date column
    athlete_df = athlete_df.withColumn("born", to_date(col("born"), "d MMMM yyyy"))

    # Final schema
    athlete_df_final = athlete_df.select(
        col("athlete_id").alias("AthleteID"),
        col("name").alias("Name"),
        col("sex").alias("Sex"),
        col("born").alias("Born"),
        col("height").alias("Height"),
        col("weight").alias("Weight"),
        col("country").alias("Nationality")
    )

    # Save to HDFS
    save_as_parquet(athlete_df_final, "hdfs://namenode:8020/data/silver/Athlete_parquet")
    print("----------------------Done Athlete Dim----------------------")


def transform_games_dim():
    """Transform and load Olympics Games Dimension."""
    games_path = "hdfs://namenode:8020/data/bronze/Olympics_Games_parquet"
    medal_tally_path = "hdfs://namenode:8020/data/bronze/Olympics_Country_parquet"

    games_df = load_parquet(games_path)
    medal_tally_df = load_parquet(medal_tally_path)

    # Join datasets
    df_final = games_df.join(
        medal_tally_df,
        games_df["country_noc"] == medal_tally_df["noc"],
        how="left"
    ).select(
        col("edition_id").cast("int").alias("EditionID"),
        col("edition").alias("EditionName"),
        col("edition_url").alias("EditionURL"),
        col("country_flag_url").alias("FlagURL"),
        col("city").alias("HostCity"),
        col("country").alias("HostCountry")
    )

    # Save to HDFS
    save_as_parquet(df_final, "hdfs://namenode:8020/data/silver/OlympicsGames_parquet")
    print("----------------------Done Games Dim----------------------")


def transform_results_dim():
    """Transform and load Olympic Results Dimension."""
    results_path = "hdfs://namenode:8020/data/bronze/Olympic_Results_parquet"
    athlete_event_results_path = "hdfs://namenode:8020/data/bronze/Olympic_Athlete_Event_Results_parquet"

    results_df = load_parquet(results_path)
    athlete_event_df = load_parquet(athlete_event_results_path)

    # Filter and deduplicate athlete event results
    athlete_event_df = athlete_event_df.select(
        col("result_id").alias("df_result_id"),
        "isTeamSport",
        "event"
    ).distinct().dropDuplicates(["df_result_id"])

    # Join datasets
    results_df_final = results_df.filter(col("result_id").rlike("^[0-9]+$")).join(
        athlete_event_df,
        results_df["result_id"] == athlete_event_df["df_result_id"],
        how="left"
    ).drop("edition", "edition_id", "df_result_id").na.drop(subset=["result_location"])

    # Extract and convert date
    date_pattern = r"(\d{1,2} \w+ \d{4})"
    results_df_final = results_df_final.withColumn(
        "result_date",
        to_date(regexp_extract(col("result_date"), date_pattern, 1), "d MMMM yyyy")
    ).dropna(subset=["result_date"])

    # Final schema
    results_df_final = results_df_final.select(
        col("result_id").cast("int").alias("ResultID"),
        col("event_title").cast("string").alias("EventTitle"),
        col("event").cast("string").alias("EventName"),
        col("sport").cast("string").alias("Sport"),
        col("sport_url").cast("string").alias("SportURL"),
        col("result_date").cast("date").alias("ResultDate"),
        col("result_location").cast("string").alias("ResultLocation"),
        col("result_participants").cast("string").alias("ResultParticipants"),
        col("isTeamSport").cast("boolean").alias("IsTeamSport")
    )

    # Save to HDFS
    save_as_parquet(results_df_final, "hdfs://namenode:8020/data/silver/OlympicResults_parquet")
    print("----------------------Done Results Dim----------------------")


def transform_country_dim():
    """Transform and load Olympics Country Dimension."""
    country_path = "hdfs://namenode:8020/data/bronze/Olympics_Country_parquet"
    country_df = load_parquet(country_path)

    # Save to HDFS
    save_as_parquet(country_df, "hdfs://namenode:8020/data/silver/OlympicsCountry_parquet")
    print("----------------------Done Country Dim----------------------")


def transform_date_dim():
    """Transform and load Date Dimension."""
    games_path = "hdfs://namenode:8020/data/bronze/Olympics_Games_parquet"
    games_df = load_parquet(games_path)

    # Transform date columns
    date_df = games_df.select(
        col("edition_id").alias("Date_id").cast("integer"),
        col("year").cast("int"),
        to_date(trim(col("start_date")), "d MMMM").alias("start_date"),
        to_date(trim(col("end_date")), "d MMMM").alias("end_date")
    ).withColumn("start_day_date", dayofmonth(col("start_date"))) \
     .withColumn("start_month_date", month(col("start_date"))) \
     .withColumn("end_day_date", dayofmonth(col("end_date"))) \
     .withColumn("end_month_date", month(col("end_date")))

    # Final schema
    date_df = date_df.select(
        "Date_id",
        "year",
        "start_day_date",
        "start_month_date",
        "end_day_date",
        "end_month_date"
    )

    # Save to HDFS
    save_as_parquet(date_df, "hdfs://namenode:8020/data/silver/Date_parquet")
    print("----------------------Done Date Dim----------------------")


def transform_fact_table():
    """Transform and load Fact Table."""
    athlete_event_results_path = "hdfs://namenode:8020/data/bronze/Olympic_Athlete_Event_Results_parquet"
    medal_tally_path = "hdfs://namenode:8020/data/bronze/Olympic_Games_Medal_Tally_parquet"

    athlete_event_df = load_parquet(athlete_event_results_path)
    medal_tally_df = load_parquet(medal_tally_path)

    # Select and join datasets
    selected_df = athlete_event_df.select(
        "edition_id",
        "country_noc",
        "result_id",
        "athlete_id",
        "pos",
        "medal"
    ).join(
        medal_tally_df.select(
            col("edition_id").alias("ed_id"),
            col("country_noc").alias("noc"),
            "gold",
            "silver",
            "bronze",
            "total"
        ),
        (col("country_noc") == col("noc")) & (col("edition_id") == col("ed_id")),
        how="inner"
    )

    # Final schema
    fact_table_df = selected_df.select(
        col("edition_id").cast("int").alias("EditionID"),
        col("country_noc").alias("CountryNOC"),
        col("result_id").cast("int").alias("ResultID"),
        col("athlete_id").cast("int").alias("AthleteID"),
        col("pos").alias("Position"),
        col("medal").alias("MedalAthlete"),
        col("gold").cast("int").alias("GoldMedals"),
        col("silver").cast("int").alias("SilverMedals"),
        col("bronze").cast("int").alias("BronzeMedals"),
        col("total").cast("int").alias("TotalMedals")
    ).fillna({"MedalAthlete": "NoMedal"})

    # Save to HDFS
    save_as_parquet(fact_table_df, "hdfs://namenode:8020/data/silver/Fact_Table_parquet")
    print("----------------------Done Fact Table----------------------")


if __name__ == "__main__":
    # Execute transformations
    transform_athlete_dim()
    transform_games_dim()
    transform_results_dim()
    transform_country_dim()
    transform_date_dim()
    transform_fact_table()

    # Stop Spark session
    spark.stop()