
# 1. Write a PySpark code to read a CSV file named "employees.csv" containing the following columns: 
# "employee_id", "name", "age", "department". Display the top 10 records from the DataFrame.
 
 
from pyspark.sql import SparkSession

# Create a SparkSession

spark = SparkSession.builder.getOrCreate()

# Read the CSV file into a DataFrame

df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Display the top 10 records

df.show(10)


# 2.  Given a PySpark DataFrame named "sales_data" with columns "product_name" and "revenue", write a code to 
calculate the total revenue for each product and display the result in descending order.


from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.window import Window
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Calculate total revenue for each product
df = sales_data.groupBy("product_name").agg(sum("revenue").alias("total_revenue"))

# Sort the DataFrame in descending order by total revenue
df = df.orderBy(desc("total_revenue"))

# Display the result
df.show()


#3. Write a PySpark code to read a JSON file named "students.json" containing student records with the following schema:
 "name" (string), "age" (integer), "grade" (string). Filter the DataFrame to include only students whose age is greater than 18.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the JSON file into a DataFrame

df = spark.read.json("students.json")

# Filter the DataFrame to include only students whose age is greater than 18

filtered_df = df.filter(col("age") > 18)

# Display the result
filtered_df.show()

#4. Consider a PySpark DataFrame named "transactions" with columns "transaction_id", "user_id", and "amount".
 #Write a code to calculate the average transaction amount for each user and display the result.


from pyspar.sql import SparkSession
from pyspark.sql.funtions import col

#create a sparkSession

spark= SparkSession.bulder.getOrCreate()

#calculate the average transaction amount for each user and display the result.

df= transactions.groupBy("user_id").agg(avg("amount").alias("average_amount")

# display the reuslt
df.show()

#5. Given a PySpark DataFrame named "logs" with columns "timestamp" (timestamp) and "event" (string), write a code to count the number  
#of events that occurred in each hour and display the result sorted by the hour.

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour
from pyspark.sql.functions import count

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Count the number of events that occurred in each hour
df = logs.withColumn("hour", hour("timestamp")).groupBy("hour").agg(count("event").alias("event_count"))

# Sort the DataFrame by the hour column
df = df.orderBy("hour")

# Display the result
df.show()





