import os
os.environ["SPARK_LOCAL_IP"] = "192.168.211.129"
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, count, avg, explode, sum, dayofweek
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
from nltk import pos_tag
import networkx as nx

spark = SparkSession.builder \
    .appName("Top10Businesses") \
    .getOrCreate()

hdfs_file_path = 'hdfs://localhost:9000/user/ibrahim/yelp/business/business.json'
df = spark.read.json(hdfs_file_path)
top_10_businesses = spark_df.orderBy('stars', ascending=False).limit(10)

print("Top 10 businesses with the highest star rating:")
top_10_businesses.show()

# Initialize Spark session
spark = SparkSession.builder.appName("BusinessAnalysis").getOrCreate()
# Load data
hdfs_file_path = "/mnt/data/business.json"
df = spark.read.json(hdfs_file_path)

top_cities = df.groupBy("city").count().orderBy(col("count").desc()).limit(10)
print("Top 10 cities with the most merchants:")
top_cities.show()

top_states = df.groupBy("state").count().orderBy(col("count").desc()).limit(5)
print("Top 5 states with the most merchants:")
top_states.show()

top_merchants = df.groupBy("name").agg(count("name").alias("merchant_count"), avg("stars").alias("avg_rating"))
top_merchants = top_merchants.orderBy(col("merchant_count").desc()).limit(20)
print("Top 20 most common merchants and their average ratings:")
top_merchants.show()

top_cities_rating = df.groupBy("city").agg(avg("stars").alias("avg_rating")).orderBy(col("avg_rating").desc()).limit(10)
print("Top 10 cities with the highest ratings:")
top_cities_rating.show()

category_count = df.select(explode(col("categories"))).distinct().count()
print(f"Total number of different categories: {category_count}")

top_categories = df.select(explode(col("categories")).alias("category")).groupBy("category").count().orderBy(col("count").desc()).limit(10)
print("Top 10 most frequent categories and their count:")
top_categories.show()

top_five_star_merchants = df.filter(col("stars") == 5.0).groupBy("name").count().orderBy(col("count").desc()).limit(20)
print("Top 20 merchants that received the most five-star reviews:")
top_five_star_merchants.show()

restaurant_counts = df.filter(col("attributes.is_restaurant") == True).select(explode(col("categories")).alias("category"))
restaurant_types = restaurant_counts.filter((col("category") == "Chinese") | (col("category") == "American") | (col("category") == "Mexican"))
restaurant_types_count = restaurant_types.groupBy("category").count()
print("Number of restaurant types (Chinese, American, Mexican):")
restaurant_types_count.show()

restaurant_reviews = df.filter(col("attributes.is_restaurant") == True).select(explode(col("categories")).alias("category"), "review_count")
restaurant_reviews = restaurant_reviews.filter((col("category") == "Chinese") | (col("category") == "American") | (col("category") == "Mexican"))
restaurant_reviews_count = restaurant_reviews.groupBy("category").sum("review_count").withColumnRenamed("sum(review_count)", "total_reviews")
print("Number of reviews for each restaurant type:")
restaurant_reviews_count.show()


rating_distribution = df.filter(col("attributes.is_restaurant") == True).select(explode(col("categories")).alias("category"), "stars")
rating_distribution = rating_distribution.filter((col("category") == "Chinese") | (col("category") == "American") | (col("category") == "Mexican"))
rating_distribution = rating_distribution.groupBy("category", "stars").count().orderBy("category", "stars")
print("Rating distribution for different restaurant types:")
rating_distribution.show()

# Load data
hdfs_file_path = "/mnt/data/review.json"
df = spark.read.json(hdfs_file_path)

df = df.withColumn("year", year(col("date")))
reviews_per_year = df.groupBy("year").count().orderBy(col("year"))
print("Number of reviews per year:")
reviews_per_year.show()

review_types = df.select(
    count("useful").alias("total_useful"),
    count("funny").alias("total_funny"),
    count("cool").alias("total_cool")
)
print("Total number of useful, funny, and cool reviews:")
review_types.show()


user_reviews_per_year = df.groupBy("year", "user_id").count().orderBy(col("year"), col("count").desc())
print("Ranking of users by total number of reviews per year:")
user_reviews_per_year.show(10)


stop_words = set(stopwords.words("english"))

def preprocess_text(text):
    words = text.lower().split()
    words = [word for word in words if word.isalpha() and word not in stop_words]
    return words

df_words = df.select(explode(split(lower(col("text")), " ")).alias("word"))
df_filtered = df_words.filter(col("word").rlike("^[a-z]+$"))  # Keep only words


top_words = df_filtered.groupBy("word").count().orderBy(col("count").desc()).limit(20)
print("Top 20 most common words in reviews:")
top_words.show()


positive_reviews = df.filter(col("stars") > 3)
positive_words = positive_reviews.select(explode(split(lower(col("text")), " ")).alias("word"))
top_positive_words = positive_words.groupBy("word").count().orderBy(col("count").desc()).limit(10)
print("Top 10 words from positive reviews:")
top_positive_words.show()


negative_reviews = df.filter(col("stars") <= 3)
negative_words = negative_reviews.select(explode(split(lower(col("text")), " ")).alias("word"))
top_negative_words = negative_words.groupBy("word").count().orderBy(col("count").desc()).limit(10)
print("Top 10 words from negative reviews:")
top_negative_words.show()


word_list = df.select("text").rdd.flatMap(lambda x: x).collect()
word_list = " ".join(word_list)
tokens = nltk.word_tokenize(word_list)
filtered_tokens = [word for word, pos in pos_tag(tokens) if pos in ["NN", "VB", "JJ"]]  # Keep only nouns, verbs, adjectives

wordcloud = WordCloud(width=800, height=400, background_color="white").generate(" ".join(filtered_tokens))
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()

word_pairs = list(zip(tokens, tokens[1:]))  # Create word pairs
G = nx.Graph()
for word1, word2 in word_pairs:
    if word1.isalpha() and word2.isalpha():
        G.add_edge(word1, word2)

plt.figure(figsize=(12, 8))
nx.draw(G, with_labels=True, font_weight="bold", node_size=20, edge_color="gray")
plt.show()


# Initialize Spark session
spark = SparkSession.builder.appName("UserAnalysis").getOrCreate()

# Load data
file_path = "/mnt/data/users.json"
df = spark.read.json(file_path)

df = df.withColumn("year", year(col("yelping_since")))

users_per_year = df.groupBy("year").count().orderBy("year")
print("Number of users joining each year:")
users_per_year.show()

top_reviewers = df.orderBy(col("review_count").desc()).limit(10)
print("Top reviewers based on review_count:")
top_reviewers.select("name", "review_count").show()

top_fans = df.orderBy(col("fans").desc()).limit(10)
print("Most popular users based on fans:")
top_fans.select("name", "fans").show()

df = df.withColumn("is_elite", col("elite").isNotNull())
elite_ratio = df.groupBy("year").agg(
    count("user_id").alias("total_users"),
    sum(col("is_elite").cast("int")).alias("elite_users")
).withColumn("elite_ratio", col("elite_users") / col("total_users"))
print("Ratio of elite users to regular users each year:")
elite_ratio.show()

silent_users = df.withColumn("is_silent", (col("review_count") == 0).cast("int"))
silent_users_per_year = silent_users.groupBy("year").agg(
    count("user_id").alias("total_users"),
    sum("is_silent").alias("silent_users")
).withColumn("silent_ratio", col("silent_users") / col("total_users"))
print("Proportion of silent users each year:")
silent_users_per_year.show()


yearly_stats = df.groupBy("year").agg(
    count("user_id").alias("new_users"),
    sum("review_count").alias("total_reviews"),
    sum(col("is_elite").cast("int")).alias("elite_users")
)
print("Yearly statistics of new users, reviews, and elite users:")
yearly_stats.show()

# Load data
file_path = "/mnt/data/users.json"
df = spark.read.json(file_path)

# Initialize Spark session
spark = SparkSession.builder.appName("CheckInAnalysis").getOrCreate()

# Sample dataset (to be replaced with actual HDFS file path if necessary)

# Create DataFrame
df = spark.createDataFrame(data)

# Split multiple check-in dates into separate rows
df = df.withColumn("date", explode(split(col("date"), ", ")))

df = df.withColumn("year", year(col("date")))
checkins_per_year = df.groupBy("year").count().orderBy("year")
print("Number of check-ins per year:")
checkins_per_year.show()

df = df.withColumn("hour", hour(col("date")))
checkins_per_hour = df.groupBy("hour").count().orderBy("hour")
print("Number of check-ins per hour in a 24-hour period:")
checkins_per_hour.show()

business_checkins = df.groupBy("business_id").count().orderBy(col("count").desc())
print("Businesses ranked by check-in count:")
business_checkins.show()

# Sample dataset (to be replaced with actual dataset if necessary)
data = [
    {"business_id": "12abb826-80bd-4a6b-b03", "stars": 5, "date": "2024-11-10"},
    {"business_id": "b4ea0684-d50c-489e-970", "stars": 3, "date": "2023-11-08"},
    {"business_id": "e1f92ee6-c593-40ad-989", "stars": 4, "date": "2024-01-02"},
    {"business_id": "9e98ba22-172d-4a91-9cb", "stars": 5, "date": "2023-08-08"},
    {"business_id": "1370ba55-9615-43c3-8e9", "stars": 2, "date": "2023-08-16"},
    {"business_id": "f923295e-90c4-4cca-8dc", "stars": 1, "date": "2024-07-06"},
]




rating_distribution = df.groupBy("stars").count().orderBy("stars")
print("Distribution of ratings (1-5 stars):")
rating_distribution.show()


df = df.withColumn("weekday", dayofweek(col("date")))  # 1=Sunday, 7=Saturday
weekly_rating_frequency = df.groupBy("weekday").count().orderBy("weekday")
print("Weekly rating frequency:")
weekly_rating_frequency.show()


top_five_star_businesses = df.filter(col("stars") == 5).groupBy("business_id").count().orderBy(col("count").desc())
print("Top businesses with the most five-star ratings:")
top_five_star_businesses.show()


spark.stop()


