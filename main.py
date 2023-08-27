import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, lag, avg, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("PokemonAnalysis").getOrCreate()

# Read CSV data
pokemon_df = spark.read.csv("pokemon.csv", header=True, inferSchema=True)
pokemon_df.printSchema()

# -------------------------------- QUERIES ----------------------------------------------------

###  QUERY 1: 20 Pokemon with highest Attack stat and their Attack stat compared to highest Attack
### For each type (first)
print (f"QUERY 1: \n")
attack_window = Window.partitionBy("type1").orderBy(col("attack").desc())

attack_df = pokemon_df.withColumn(
    "highest_attack", max(col("attack")).over(attack_window)
).withColumn("attack_diff", col("attack") - col("highest_attack"))

attack_df = attack_df.selectExpr(
    "number", "name", "type1", "attack", "highest_attack", "attack_diff"
)
attack_df.show(20)

# Choose type
attack_df = attack_df.where(col("type1") == "Dragon")
attack_df.show(20)


### QUERY 2: Average Special Attack stat for given type compared to Special Attack for each pokemon in %
### excluding Mega Evolutions
print (f"QUERY 2: \n")
sp_attack_window = Window.partitionBy("type1")

sp_attack_df = pokemon_df.withColumn(
    "avg_sp_attack", avg(col("sp_attack")).over(sp_attack_window)
).withColumn("sp_attack_perc", (col("sp_attack") / col("avg_sp_attack")) * 100)
sp_attack_df = sp_attack_df.filter(
    (~col("name").contains("Mega")) & (col("type1") == "Fire") # Choose type
).orderBy(desc("sp_attack"))

sp_attack_df = sp_attack_df.selectExpr(
    "number", "name", "type1", "sp_attack", "avg_sp_attack", "sp_attack_perc"
)
sp_attack_df.show(20)


# Stop Spark session
spark.stop()
