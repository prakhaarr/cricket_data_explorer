# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import col,sum,when,avg,row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import year, month, dayofmonth, when, lower, regexp_replace,expr,current_date


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPL DATA ANALYSIS").getOrCreate() 

# COMMAND ----------

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

ball_by_ball_df = spark.read.schema(ball_by_ball_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  # Assuming 'year' is represented as an integer
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),  # Assuming 'manofmach' is the abbreviation for 'man of the match'
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

match_df = spark.read.schema(match_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Match.csv")

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

player_df = spark.read.schema(player_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Player.csv")

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(10, 0), True),  # Adjust precision and scale as needed
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),  # Assuming 'year' is represented as an integer
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

# COMMAND ----------

player_match_df = spark.read.schema(player_match_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Player_match.csv")

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

# COMMAND ----------

team_df = spark.read.schema(team_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Team.csv")

# COMMAND ----------

#Creating filter to include only valid deliveries(excluding wides and no balls)
ball_by_ball_df = ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))


# COMMAND ----------

#calculating total and average runs scored in each match
total_and_avg_runs = ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
)

# COMMAND ----------

# total_and_avg_runs.toPandas()

# COMMAND ----------

#calculating running total of runs in each match for each over
windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")

ball_by_ball_df = ball_by_ball_df.withColumn(
    "running_total_runs",
    sum("runs_scored").over(windowSpec)
)

# COMMAND ----------

#Conditional columns, flag for high impact balls that is either wicket or more than 6 runs
ball_by_ball_df = ball_by_ball_df.withColumn(
    "high_impact",
    when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket")==True),True).otherwise(False) 
)

# COMMAND ----------

#Extracting year, month, and day from the match date for more detailed time-based analysis
match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("month", month("match_date"))
match_df = match_df.withColumn("day", dayofmonth("match_date"))

# COMMAND ----------

#creating Win Margin column for analysis

match_df = match_df.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)

# COMMAND ----------

#analysing the match and impact of the toss

match_df = match_df.withColumn(
    "toss_match_winner",
    when(col("toss_winner")==col("match_winner"),"Yes").otherwise("No")
)

# COMMAND ----------

#sanitising names and filling blanks fields with unknown
player_df = player_df.withColumn("player_name",lower(regexp_replace("player_name", "[^a-zA-z0-9 ]","")))
player_df = player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

# COMMAND ----------

#now categorizing left and right hand batsman
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("left"),"Left-Handed").otherwise("Right-Handed")
)

# COMMAND ----------

player_df.show(2)

# COMMAND ----------

player_match_df = player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >=35, "Veteran").otherwise("Non-Veteran")
)

# player_match_df = player_match_df.filter(col("batting_status") != "Did Not Bat")

player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date())-col("season_year"))
)

# COMMAND ----------

player_match_df.columns

# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
player_match_df.createOrReplaceTempView("player_match")
player_df.createOrReplaceTempView("player")
team_df.createOrReplaceTempView("team")
match_df.createOrReplaceTempView("match")

# COMMAND ----------

top_scoring_batsman_per_season = spark.sql("""
SELECT
p.player_name,
m.season_year,
SUM(b.runs_scored) AS total_runs
FROM ball_by_ball b                                         
JOIN match m ON b.match_id = m.match_id
JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON p.player_id = pm.player_id
GROUP BY p.player_name, m.season_year
ORDER BY m.season_year, total_runs DESC
""")

# COMMAND ----------

top_scoring_batsman_per_season.toPandas()

# COMMAND ----------

economical_bowlers_powerplay = spark.sql("""
SELECT p.player_name,
AVG(b.runs_scored) AS avg_runs_per_ball,
COUNT(b.bowler_wicket) AS total_wickets
FROM ball_by_ball b
JOIN player_match pm on b.match_id = pm.match_id AND b.bowler = pm.player_id
JOIN player p ON pm.player_id = p.player_id
where b.over_id<=6
GROUP BY p.player_name
HAVING COUNT(*) >= 1
ORDER BY avg_runs_per_ball, total_wickets desc                                        
                                         
                                         
"""
)

# COMMAND ----------

economical_bowlers_powerplay.show(5)

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
SELECT m.match_id, m.toss_winner,m.toss_name,m.match_winner,
        CASE when m.toss_winner = m.match_winner THEN 'WON' ELSE 'Lost' END AS match_outcome 
FROM match m
where m.toss_name IS NOT NULL
ORDER BY m.match_id                                                                          
""")

# COMMAND ----------

toss_impact_individual_matches.show()

# COMMAND ----------

average_runs_in_wins = spark.sql("""
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played FROM ball_by_ball b
JOIN player_match pm ON b.match_id= pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins DESC
""")

# COMMAND ----------

# Assuming `average_runs_in_wins` is your DataFrame
average_runs_in_wins.write.format("csv").option("header", "true").save("/dbfs/tmp/average_runs_in_wins.csv")


# COMMAND ----------

average_runs_in_wins.show(5)

# COMMAND ----------

import matplotlib.pyplot as plt
from IPython.display import display

# COMMAND ----------

spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

# COMMAND ----------

import matplotlib.pyplot as plt

# Assuming 'economical_bowlers_powerplay' is already executed and available as a Spark DataFrame
economical_bowlers_pd = economical_bowlers_powerplay.toPandas()

# Visualizing using Matplotlib
plt.figure(figsize=(12, 8))
# Limiting to top 10 for clarity in the plot
top_economical_bowlers = economical_bowlers_pd.nsmallest(10, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='skyblue')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot to DBFS
plot_path = '/dbfs/tmp/top_economical_bowlers.png'
plt.savefig(plot_path)

# Show the plot (optional, for local display)
plt.show()


# COMMAND ----------


