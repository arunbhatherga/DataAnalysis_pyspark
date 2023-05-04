
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, avg, max, min, count, sum


if __name__ == '__main__':

    spark = SparkSession.builder.appName("MyApp").config("spark.jars", "/app/data/postgresql-42.6.0.jar").getOrCreate()

    df = spark.read.csv("/app/data/Project 1_dataset_bank-full (2).csv", header=True, inferSchema=True)

    df = df.withColumn("columns", split(df.columns[0], ";"))
    df = df.selectExpr(
        "columns[0] as age",
        "columns[1] as job",
        "columns[2] as marital",
        "columns[3] as education",
        "columns[4] as default",
        "columns[5] as balance",
        "columns[6] as housing",
        "columns[7] as loan",
        "columns[8] as contact",
        "columns[9] as day",
        "columns[10] as month",
        "columns[11] as duration",
        "columns[12] as campaign",
        "columns[13] as pdays",
        "columns[14] as previous",
        "columns[15] as poutcome",
        "columns[16] as y"
        )
    
    df = df.select([regexp_replace(col, '"', '').alias(col) for col in df.columns])
    
    df.show()

    total_entries = df.count()
    
    subscribed_entries = df.filter(df.y == 'yes').count()
    
    success_rate = (subscribed_entries / total_entries) * 100
    
    failure_rate = 100 - success_rate
    
    print('Marketing Success Rate: ', success_rate)
    
    print('Marketing Failure Rate: ', failure_rate)

    df_age = df.select(avg('age'), max('age'), min('age'))

    df_age.show()

    df_age_y = df.select('age', 'y')
    
    df_age_y.show()
    
    age_y_count = df_age_y.filter(df_age_y.y == 'yes').groupBy('age').agg(count('*').alias('Count'))
    
    age_count = df_age_y.groupBy('age').agg(count('*').alias('Total_Count'))
    
    age_y_percent = age_y_count.join(age_count, 'age').withColumn('Percentage', (col('Count')/col('Total_Count'))*100)
    
    age_y_percent.orderBy('age').show()

    df_marital_y = df.select('marital', 'y')
    
    df_marital_y.show()
    
    marital_y_count = df_marital_y.filter(df_marital_y.y == 'yes').groupBy('marital').agg(count('*').alias('Count'))
    
    marital_count = df_marital_y.groupBy('marital').agg(count('*').alias('Total_Count'))
    
    marital_y_percent = marital_y_count.join(marital_count, 'marital').withColumn('Percentage', (col('Count')/col('Total_Count'))*100)
    
    marital_y_percent.show()

    df_age_marital_y = df.select('age', 'marital', 'y')
    
    df_age_marital_y.show()
    
    age_marital_y_count = df_age_marital_y.filter(df_age_marital_y.y == 'yes').groupBy('age', 'marital').agg(count('*').alias('Count'))
    
    age_marital_count = df_age_marital_y.groupBy('age', 'marital').agg(count('*').alias('Total_Count'))
    
    age_marital_y_percent = age_marital_y_count.join(age_marital_count, ['age', 'marital']).withColumn('Percentage', (col('Count')/col('Total_Count'))*100)
    
    url = "jdbc:postgresql://postgres:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
        }

    # Write the DataFrame to the database
    df_age.write.jdbc(url=url, table="max_min_avg_age", mode="overwrite", properties=properties)
    age_marital_y_percent.write.jdbc(url=url, table="age_marital_y_percent", mode="overwrite", properties=properties)

    age_marital_y_percent.show()







    