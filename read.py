import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, explode, split, lower, dayofweek, year, hour, row_number
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
import networkx as nxspark

# Set up the Spark session
spark = SparkSession.builder \
    .appName("YelpBusinessAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

# Set HDFS file paths
hdfs_business_path = "hdfs://node-master:9000/user/karim/yelp/business/yelp_academic_dataset_business.json"  # Change this to your HDFS path
hdfs_review_path = "hdfs://node-master:9000/user/karim/yelp/review/yelp_academic_dataset_review.json"  # Change this to your HDFS path
hdfs_user_path = "hdfs://node-master:9000/user/karim/yelp/user/yelp_academic_dataset_user.json"  # Change this to your HDFS path
hdfs_checkin_path = "hdfs://node-master:9000/user/karim/yelp/checkin/yelp_academic_dataset_checkin.json"  # Change this to your HDFS path
hdfs_tip_path = "hdfs://node-master:9000/user/karim/yelp/tip/yelp_academic_dataset_tip.json"
# Load the business data from HDFS
df_business = spark.read.json(hdfs_business_path)


print("Top 10 businesses with the highest star rating:")
spark.sql("""
    SELECT name, stars 
    FROM yelp_business 
    ORDER BY stars DESC 
    LIMIT 10
""").show()

print("Top 10 cities with the most merchants:")
spark.sql("""
    SELECT city, COUNT(*) as merchant_count 
    FROM yelp_business 
    GROUP BY city 
    ORDER BY merchant_count DESC 
    LIMIT 10
""").show()

print("Top 5 states with the most merchants:")
spark.sql("""
    SELECT state, COUNT(*) as merchant_count 
    FROM yelp_business 
    GROUP BY state 
    ORDER BY merchant_count DESC 
    LIMIT 5
""").show()

print("Number of reviews per year:")
spark.sql("""
    SELECT year(date) as year, COUNT(*) as review_count 
    FROM yelp_review 
    GROUP BY year 
    ORDER BY year
""").show()

print("Top reviewers based on review count:")
spark.sql("""
    SELECT name, review_count 
    FROM yelp_user 
    ORDER BY review_count DESC 
    LIMIT 10
""").show()

print("Number of check-ins per year:")
spark.sql("""
    SELECT year(date) as year, COUNT(*) as checkin_count 
    FROM yelp_checkin 
    GROUP BY year 
    ORDER BY year
""").show()

print("Top 10 most reviewed businesses:")
spark.sql("""
    SELECT name, review_count 
    FROM yelp_business 
    ORDER BY review_count DESC 
    LIMIT 10
""").show()

print("Average star rating per category:")
spark.sql("""
    SELECT category, AVG(stars) as avg_stars 
    FROM (
        SELECT explode(categories) as category, stars 
        FROM yelp_business
    ) 
    GROUP BY category 
    ORDER BY avg_stars DESC
""").show()

print("Most common business categories:")
spark.sql("""
    SELECT category, COUNT(*) as count 
    FROM (
        SELECT explode(categories) as category 
        FROM yelp_business
    ) 
    GROUP BY category 
    ORDER BY count DESC 
    LIMIT 10
""").show()

print("Average stars per city:")
spark.sql("""
    SELECT city, AVG(stars) as avg_stars 
    FROM yelp_business 
    GROUP BY city 
    ORDER BY avg_stars DESC 
    LIMIT 10
""").show()

print("Businesses with the most tips:")
spark.sql("""
    SELECT business_id, COUNT(*) as tip_count 
    FROM yelp_tip 
    GROUP BY business_id 
    ORDER BY tip_count DESC 
    LIMIT 10
""").show()

print("Most active tip users:")
spark.sql("""
    SELECT user_id, COUNT(*) as tip_count 
    FROM yelp_tip 
    GROUP BY user_id 
    ORDER BY tip_count DESC 
    LIMIT 10
""").show()

print("Top 10 users with most friends:")
spark.sql("""
    SELECT name, SIZE(friends) as friend_count 
    FROM yelp_user 
    ORDER BY friend_count DESC 
    LIMIT 10
""").show()

print("Top 10 users by useful votes:")
spark.sql("""
    SELECT name, useful 
    FROM yelp_user 
    ORDER BY useful DESC 
    LIMIT 10
""").show()

print("Hourly distribution of reviews:")
spark.sql("""
    SELECT hour(date) as hour, COUNT(*) as review_count 
    FROM yelp_review 
    GROUP BY hour 
    ORDER BY hour
""").show()

# Top 10 businesses with the highest star rating
top_10_businesses = df_business.orderBy('stars', ascending=False).limit(10)
print("Top 10 businesses with the highest star rating:")
top_10_businesses.show()

# Top 10 cities with the most merchants
top_cities = df_business.groupBy("city").count().orderBy(col("count").desc()).limit(10)
print("Top 10 cities with the most merchants:")
top_cities.show()

# Top 5 states with the most merchants
top_states = df_business.groupBy("state").count().orderBy(col("count").desc()).limit(5)
print("Top 5 states with the most merchants:")
top_states.show()

# Top 20 most common merchants and their average ratings
top_merchants = df_business.groupBy("name").agg(count("name").alias("merchant_count"), avg("stars").alias("avg_rating"))
top_merchants = top_merchants.orderBy(col("merchant_count").desc()).limit(20)
print("Top 20 most common merchants and their average ratings:")
top_merchants.show()

# Top 10 cities with the highest ratings
top_cities_rating = df_business.groupBy("city").agg(avg("stars").alias("avg_rating")).orderBy(col("avg_rating").desc()).limit(10)
print("Top 10 cities with the highest ratings:")
top_cities_rating.show()

# Top 10 most frequent categories and their count
top_categories = df_business.select(explode(col("categories")).alias("category")).groupBy("category").count().orderBy(col("count").desc()).limit(10)
print("Top 10 most frequent categories and their count:")
top_categories.show()

# Top 20 merchants that received the most five-star reviews
top_five_star_merchants = df_business.filter(col("stars") == 5.0).groupBy("name").count().orderBy(col("count").desc()).limit(20)
print("Top 20 merchants that received the most five-star reviews:")
top_five_star_merchants.show()

# Load review data from HDFS
df_reviews = spark.read.json(hdfs_review_path)

# Reviews per year
df_reviews = df_reviews.withColumn("year", year(col("date")))
reviews_per_year = df_reviews.groupBy("year").count().orderBy(col("year"))
print("Number of reviews per year:")
reviews_per_year.show()

# Count of useful, funny, and cool reviews
review_types = df_reviews.select(
    count("useful").alias("total_useful"),
    count("funny").alias("total_funny"),
    count("cool").alias("total_cool")
)
print("Total number of useful, funny, and cool reviews:")
review_types.show()

# Ranking of users by total number of reviews per year
user_reviews_per_year = df_reviews.groupBy("year", "user_id").count().orderBy(col("year"), col("count").desc())
print("Ranking of users by total number of reviews per year:")
user_reviews_per_year.show(10)

# Text analysis for reviews (top words, positive/negative)
stop_words = set(stopwords.words("english"))

def preprocess_text(text):
    words = text.lower().split()
    words = [word for word in words if word.isalpha() and word not in stop_words]
    return words

df_words = df_reviews.select(explode(split(lower(col("text")), " ")).alias("word"))
df_filtered = df_words.filter(col("word").rlike("^[a-z]+$"))  # Keep only words

top_words = df_filtered.groupBy("word").count().orderBy(col("count").desc()).limit(20)
print("Top 20 most common words in reviews:")
top_words.show()

# Generate word cloud from all reviews
word_list = df_reviews.select("text").rdd.flatMap(lambda x: x).collect()
word_list = " ".join(word_list)
tokens = nltk.word_tokenize(word_list)
filtered_tokens = [word for word, pos in pos_tag(tokens) if pos in ["NN", "VB", "JJ"]]  # Keep only nouns, verbs, adjectives

wordcloud = WordCloud(width=800, height=400, background_color="white").generate(" ".join(filtered_tokens))
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()


word_pairs = list(zip(tokens, tokens[1:])) 
G = nx.Graph()
for word1, word2 in word_pairs:
    if word1.isalpha() and word2.isalpha():
        G.add_edge(word1, word2)

plt.figure(figsize=(12, 8))
nx.draw(G, with_labels=True, font_weight="bold", node_size=20, edge_color="gray")
plt.show()

# Load user data from HDFS
df_users = spark.read.json(hdfs_user_path)

# Users joining each year
df_users = df_users.withColumn("year", year(col("yelping_since")))
users_per_year = df_users.groupBy("year").count().orderBy("year")
print("Number of users joining each year:")
users_per_year.show()

# Top reviewers based on review_count
top_reviewers = df_users.orderBy(col("review_count").desc()).limit(10)
print("Top reviewers based on review_count:")
top_reviewers.select("name", "review_count").show()

# Most popular users based on fans
top_fans = df_users.orderBy(col("fans").desc()).limit(10)
print("Most popular users based on fans:")
top_fans.select("name", "fans").show()

# Ratio of elite users to regular users each year
df_users = df_users.withColumn("is_elite", col("elite").isNotNull())
elite_ratio = df_users.groupBy("year").agg(
    count("user_id").alias("total_users"),
    sum(col("is_elite").cast("int")).alias("elite_users")
).withColumn("elite_ratio", col("elite_users") / col("total_users"))
print("Ratio of elite users to regular users each year:")
elite_ratio.show()

# Silent users (no reviews)
silent_users = df_users.withColumn("is_silent", (col("review_count") == 0).cast("int"))
silent_users_per_year = silent_users.groupBy("year").agg(
    count("user_id").alias("total_users"),
    sum("is_silent").alias("silent_users")
).withColumn("silent_ratio", col("silent_users") / col("total_users"))
print("Proportion of silent users each year:")
silent_users_per_year.show()

# Load check-in data from HDFS
df_checkins = spark.read.json(hdfs_checkin_path)

# Number of check-ins per year
df_checkins = df_checkins.withColumn("year", year(col("date")))
checkins_per_year = df_checkins.groupBy("year").count().orderBy("year")
print("Number of check-ins per year:")
checkins_per_year.show()

# Number of check-ins per hour
df_checkins = df_checkins.withColumn("hour", hour(col("date")))
checkins_per_hour = df_checkins.groupBy("hour").count().orderBy("hour")
print("Number of check-ins per hour in a 24-hour period:")
checkins_per_hour.show()


business_df = spark.read.json(hdfs_business_path).select("business_id", "name", "city", "stars", "review_count")
checkin_df = spark.read.json(hdfs_checkin_path).select("business_id", "date")


checkin_df = checkin_df.groupBy("business_id").count().withColumnRenamed("count", "checkin_count")

# Merge business and check-in data
merged_df = business_df.join(checkin_df, "business_id", "left").fillna({"checkin_count": 0})

# Define window specifications
window_spec_review = Window.partitionBy("city").orderBy(col("review_count").desc())
window_spec_rating = Window.partitionBy("city").orderBy(col("stars").desc())
window_spec_checkin = Window.partitionBy("city").orderBy(col("checkin_count").desc())

# Top merchants in each city based on different criteria
top_merchants_by_reviews = merged_df.withColumn("rank", row_number().over(window_spec_review)).filter(col("rank") <= 5)
top_merchants_by_rating = merged_df.withColumn("rank", row_number().over(window_spec_rating)).filter(col("rank") <= 5)
top_merchants_by_checkins = merged_df.withColumn("rank", row_number().over(window_spec_checkin)).filter(col("rank") <= 5)

# Display results
print("Top 5 merchants in each city based on number of reviews:")
top_merchants_by_reviews.select("city", "name", "review_count").show()

print("Top 5 merchants in each city based on average rating:")
top_merchants_by_rating.select("city", "name", "stars").show()

