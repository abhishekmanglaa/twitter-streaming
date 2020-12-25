# Twitter 

Mount JSON data using DBFS, define and apply a schema, parse fields, and saving the cleaned results back to DBFS.

## Instructions

A common source of data in ETL pipelines is <a href="https://kafka.apache.org/" target="_blank">Apache Kafka</a>, or the managed alternative
<a href="https://aws.amazon.com/kinesis/" target="_blank">Kinesis</a>.

Tweets were streamed from the <a href="https://developer.twitter.com/en/docs" target="_blank">Twitter firehose API</a> into such an kafka server and,
from there, dumped into the distributed file system.

## Part 1: Extracting and Exploring the Data

First, review the data.

### Step 1: Exploring the Folder Structure

Explore the mount and review the directory structure. Use `%fs ls`.  The data is located in `/mnt/training/twitter/firehose/`


```python

path = "/mnt/training/twitter/firehose/2018/01/10/01"
display(dbutils.fs.ls(path)) # %fs ls evaluates to this
```


<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>dbfs:/mnt/training/twitter/firehose/2018/01/10/01/twitterstream-1-2018-01-10-01-02-54-76b07dc1-609a-47d7-ab72-60975b109629</td><td>twitterstream-1-2018-01-10-01-02-54-76b07dc1-609a-47d7-ab72-60975b109629</td><td>27357467</td></tr><tr><td>dbfs:/mnt/training/twitter/firehose/2018/01/10/01/twitterstream-1-2018-01-10-01-12-55-ec34b878-b230-43a0-82a9-9d69dd0fbda5</td><td>twitterstream-1-2018-01-10-01-12-55-ec34b878-b230-43a0-82a9-9d69dd0fbda5</td><td>19907670</td></tr><tr><td>dbfs:/mnt/training/twitter/firehose/2018/01/10/01/twitterstream-1-2018-01-10-01-22-55-6916b906-8a3d-49a2-954b-bbfcd69b03fd</td><td>twitterstream-1-2018-01-10-01-22-55-6916b906-8a3d-49a2-954b-bbfcd69b03fd</td><td>21936208</td></tr><tr><td>dbfs:/mnt/training/twitter/firehose/2018/01/10/01/twitterstream-1-2018-01-10-01-33-42-134418ff-a7d1-4a7e-be5e-398638187699</td><td>twitterstream-1-2018-01-10-01-33-42-134418ff-a7d1-4a7e-be5e-398638187699</td><td>44</td></tr></tbody></table></div>


### Step 2: Exploring a Single File

Looking at a single file before injecting all of the data.

`twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4`. Find this in `/mnt/training/twitter/firehose/2018/01/08/18/`.  Save the results to the variable `df`.


```python


path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"


df = spark.read.json(path)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>


Count the records in the file. Save the result to `dfCount`.


```python
dfCount = df.count()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>


## Part 2: Applying a Schema

-sandbox
### Step 1: Creating a Schema for the `Tweet` Table



| Field | Type|
|-------|-----|
| tweet_id | integer |
| user_id | integer |
| language | string |
| text | string |
| created_at | string* |


```python
from pyspark.sql.types import StructField, StructType, StringType, LongType

path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
# path = "/mnt/training/twitter/firehose/2018/*/*/*/*"


tweetSchema = StructType([
  StructField("id", LongType(), True),
  StructField("user", StructType([
    StructField("id", LongType(), True)
  ]), True),  
  StructField("lang", StringType(), True),
  StructField("text", StringType(), True),
  StructField("created_at", StringType(), True)
])

tweetDF = spark.read.schema(tweetSchema).json(path)

display(tweetDF)
```

### Step 2: Schema for the remaining table


```python
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, IntegerType, LongType

path = "/mnt/training/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"
# path = "/mnt/training/twitter/firehose/2018/*/*/*/*"

fullTweetSchema = StructType([
  StructField("id", LongType(), True),
  StructField("user", StructType([
    StructField("id", LongType(), True),
    StructField("screen_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("friends_count", IntegerType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("description", StringType(), True)
  ]), True),
  StructField("entities", StructType([
    StructField("hashtags", ArrayType(
      StructType([
        StructField("text", StringType(), True)
      ]),
    ), True),
    StructField("urls", ArrayType(
      StructType([
        StructField("url", StringType(), True),
        StructField("expanded_url", StringType(), True),
        StructField("display_url", StringType(), True)
      ]),
    ), True)
  ]), True),
  StructField("lang", StringType(), True),
  StructField("text", StringType(), True),
  StructField("created_at", StringType(), True)
])

fullTweetDF = spark.read.schema(fullTweetSchema).json(path)
fullTweetDF.printSchema()
display(fullTweetDF)
```


```python
fullTweetDF.printSchema()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">root
-- id: long (nullable = true)
-- user: struct (nullable = true)
    |-- id: long (nullable = true)
    |-- screen_name: string (nullable = true)
    |-- location: string (nullable = true)
    |-- friends_count: integer (nullable = true)
    |-- followers_count: integer (nullable = true)
    |-- description: string (nullable = true)
-- entities: struct (nullable = true)
    |-- hashtags: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- text: string (nullable = true)
    |-- urls: array (nullable = true)
    |    |-- element: struct (containsNull = true)
    |    |    |-- url: string (nullable = true)
    |    |    |-- expanded_url: string (nullable = true)
    |    |    |-- display_url: string (nullable = true)
-- lang: string (nullable = true)
-- text: string (nullable = true)
-- created_at: string (nullable = true)

</div>


## Part 3: Creating the Tables

### Step 1: Filtering Nulls

The Twitter data contains both deletions and tweets.  This is why some records appear as null values. Create a DataFrame called `fullTweetFilteredDF` that filters out the null values.


```python

from pyspark.sql.functions import col

fullTweetFilteredDF = (fullTweetDF
  .filter(col("id").isNotNull())
)

display(fullTweetFilteredDF)
```

-sandbox
### Step 2: Creating the `Tweet` Table


```python
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import TimestampType

timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

tweetDF = fullTweetFilteredDF.select(col("id").alias("tweetID"), 
  col("user.id").alias("userID"), 
  col("lang").alias("language"),
  col("text"),
  unix_timestamp("created_at", timestampFormat).cast(TimestampType()).alias("createdAt")
)

display(tweetDF)
```

### Step 3: Creating the Account Table

Save the account table as `accountDF`.


```python

accountDF = fullTweetFilteredDF.select(col("user.id").alias("userID"), 
    col("user.screen_name").alias("screenName"),
    col("user.location"),
    col("user.friends_count").alias("friendsCount"),
    col("user.followers_count").alias("followersCount"),
    col("user.description")
)

display(accountDF)
```

-sandbox
### Step 4: Creating Hashtag and URL Tables Using `explode`


```python
from pyspark.sql.functions import explode, col

hashtagDF = fullTweetFilteredDF.select(col("id").alias("tweetID"), 
    explode(col("entities.hashtags.text")).alias("hashtag")
)

urlDF = (fullTweetFilteredDF.select(col("id").alias("tweetID"), 
    explode(col("entities.urls")).alias("urls"))
  .select(
    col("tweetID"),
    col("urls.url").alias("URL"),
    col("urls.display_url").alias("displayURL"),
    col("urls.expanded_url").alias("expandedURL"))
)

hashtagDF.show()
urlDF.show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+------------------+--------------------+
           tweetID|             hashtag|
+------------------+--------------------+
950438954288914432|                diet|
950438954280472576|صاروخ_سعودي_يرعب_...|
950438954297303040|                 Tea|
950438954297303040|        GoldenGlobes|
950438954305716226|      الهلال_الاتفاق|
950438954305761280|               beats|
950438954305761280|                 90s|
950438954305761280|           90shiphop|
950438954305761280|                 pac|
950438954305761280|              legend|
950438954305761280|                thug|
950438954305761280|               music|
950438954305761280|           westcoast|
950438954305761280|              eminem|
950438954305761280|               drdre|
950438954305761280|                trap|
950438954309832705|                BB11|
950438954309832705|          BiggBoss11|
950438954309832705|       WeekendKaVaar|
950438954293235714|              Fallen|
+------------------+--------------------+
only showing top 20 rows

+------------------+--------------------+--------------------+--------------------+
           tweetID|                 URL|          displayURL|         expandedURL|
+------------------+--------------------+--------------------+--------------------+
950438954280472576|https://t.co/j0Rg...|youtube.com/watch...|https://www.youtu...|
950438954284797958|https://t.co/B5Zg...|twitter.com/i/web...|https://twitter.c...|
950438954310033410|http://t.co/Kv3EWEhO|       bit.ly/OYlKII|http://bit.ly/OYlKII|
950438954305835008|https://t.co/l3x0...|    goo.gl/fb/atjACB|https://goo.gl/fb...|
950438954305761280|https://t.co/syjj...|instagram.com/p/B...|https://www.insta...|
950438954301644800|https://t.co/Mfid...|twitter.com/i/web...|https://twitter.c...|
950438954280550407|https://t.co/5C60...|        alathkar.org| http://alathkar.org|
950438954288996354|https://t.co/diiJ...|open.spotify.com/...|https://open.spot...|
950438954301616131|https://t.co/Ram2...|twitter.com/i/web...|https://twitter.c...|
950438954280587264|https://t.co/8IVH...|twitter.com/frann...|https://twitter.c...|
950438954284797957|https://t.co/xTUw...|twitter.com/thisi...|https://twitter.c...|
950438954272280577|https://t.co/HcVY...|       goo.gl/TU8gBa|http://goo.gl/TU8gBa|
950438958470545408|https://t.co/7qVu...|twitter.com/i/web...|https://twitter.c...|
950438958495920129|https://t.co/BV7h...|            du3a.org|   https://du3a.org/|
950438958487318528|https://t.co/tJTO...| whounfollowedme.org|http://whounfollo...|
950438958466551809|https://t.co/MalH...|       goo.gl/RgE1Vz|https://goo.gl/Rg...|
950438958504271873|https://t.co/zym0...|            du3a.org|     http://du3a.org|
950438958487494658|https://t.co/2enH...|twitter.com/i/web...|https://twitter.c...|
950438958487494657|https://t.co/48Z7...|twitter.com/i/web...|https://twitter.c...|
950438958470713346|https://t.co/Byu0...|twitter.com/edwar...|https://twitter.c...|
+------------------+--------------------+--------------------+--------------------+
only showing top 20 rows

</div>



```python
accountDF.write.mode("overwrite").parquet("/tmp/" + username + "/account.parquet")
tweetDF.write.mode("overwrite").parquet("/tmp/" + username + "/tweet.parquet")
hashtagDF.write.mode("overwrite").parquet("/tmp/" + username + "/hashtag.parquet")
urlDF.write.mode("overwrite").parquet("/tmp/" + username + "/url.parquet")
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

