
"""
transform and load function
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/databricks_project/games.csv", 
         dataset2="dbfs:/FileStore/databricks_project/players.csv",
         dataset3="dbfs:/FileStore/databricks_project/plays.csv",
         dataset4="dbfs:/FileStore/databricks_project/tackles.csv",
         dataset5="dbfs:/FileStore/databricks_project/La_vs_buf.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()

    # load csv and transform it by inferring schema 
    games_df = spark.read.csv(dataset, header=True, inferSchema=True)
    players_df = spark.read.csv(dataset2, header=True, inferSchema=True)
    plays_df = spark.read.csv(dataset3, header=True, inferSchema=True)
    tackles_df = spark.read.csv(dataset4, header=True, inferSchema=True)
    La_vs_buf_df = spark.read.csv(dataset5, header=True, inferSchema=True)

    # add unique IDs to the DataFrames

    games_df = games_df.withColumn("id", monotonically_increasing_id())
    players_df = players_df.withColumn("id", monotonically_increasing_id())
    plays_df = plays_df.withColumn("id", monotonically_increasing_id())
    tackles_df = tackles_df.withColumn("id", monotonically_increasing_id())
    La_vs_buf_df = La_vs_buf_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    games_df.write.format("delta").mode("overwrite").saveAsTable("nfl_games_2022")
    players_df.write.format("delta").mode("overwrite").saveAsTable("nfl_players_2022")
    plays_df.write.format("delta").mode("overwrite").saveAsTable("nflplays")
    tackles_df.write.format("delta").mode("overwrite").saveAsTable("nfl_tackles_2022")
    La_vs_buf_df.write.format("delta").mode("overwrite").saveAsTable("nfl_La_vs_buf_2022")

    # print(games_df.columns)
    # print(players_df.columns)
    # print(plays_df.columns)
    # print(tackles_df.columns)
    # print(La_vs_buf_df.columns)

    # num_rows = games_df.count()
    # print(num_rows)
    # num_rows = players_df.count()
    # print(num_rows)
    # num_rows = plays_df.count()
    # print(num_rows)
    # num_rows = tackles_df.count()
    # print(num_rows)
    # num_rows = La_vs_buf_df.count()
    # print(num_rows)

    return "finished transform and load"

if __name__ == "__main__":
    load()
