import psycopg2
from pyspark.sql import SparkSession
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Spark PostgreSQL Integration") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/sparkdb"
properties = {
    "user": "sparkuser",
    "password": "sparkpassword",
    "driver": "org.postgresql.Driver"
}

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="sparkdb", user="sparkuser", password="sparkpassword", host="localhost", port="5432"
)

# Read data from PostgreSQL using Spark
try:
    df = spark.read.jdbc(url=jdbc_url, table="sample_data", properties=properties)
    df.createOrReplaceTempView("sample_data")
except Exception as e:
    print(f"Error reading data from PostgreSQL: {e}")



#--------------------------------CRUD------------------------------------
# Create Operation: Insert new data
try:
    print("\n----------------CREATE------------")
    total_records = spark.sql("SELECT COUNT(*) FROM sample_data").collect()[0][0]
    print(f"Total records before insert: {total_records}")
    
    # Creating a DataFrame to insert
    data = [("South America", "Brazil", "Mango"), 
            ("Africa", "Brazil", "Pineapple")]
    columns = ["region", "country", "item"]
    new_data_df = spark.createDataFrame(data, columns)
    
    # Appending new data to PostgreSQL
    new_data_df.write.jdbc(url=jdbc_url, table="sample_data", mode="append", properties=properties)
    
    total_records_after_insert = spark.sql("SELECT COUNT(*) FROM sample_data").collect()[0][0]
    print("New records inserted successfully.")
    print(f"Total records after insert: {total_records_after_insert}")
except Exception as e:
    print(f"Error inserting data: {e}")

# Read Operation: Read and display data using Spark SQL
try:
    print("\n----------------READ------------")
    spark.sql("SELECT * FROM sample_data LIMIT 10").show()
except Exception as e:
    print(f"Error reading data: {e}")

# Update Operation: Update data using SQL query
try:
    print("\n----------------UPDATE------------")
    cursor = conn.cursor()

    # Display records before update
    print("Records before update:")
    spark.sql("SELECT * FROM sample_data WHERE country = 'Brazil'").show()
    
    # Perform update
    update_query = """
        UPDATE sample_data
        SET item = 'Papaya'
        WHERE country = 'Brazil'
    """
    cursor.execute(update_query)
    conn.commit()
    
    # Display records after update
    print("\nRecords after update:")
    spark.sql("SELECT * FROM sample_data WHERE country = 'Brazil'").show()

    cursor.close()
except Exception as e:
    print(f"Error updating data: {e}")

# Delete Operation: Delete data using SQL query
try:
    print("\n----------------DELETE------------")
    total_records_before_delete = spark.sql("SELECT COUNT(*) FROM sample_data").collect()[0][0]
    print(f"Total records before delete: {total_records_before_delete}")

    cursor = conn.cursor()
    delete_query = """
        DELETE FROM sample_data
        WHERE country = 'Brazil'
    """
    cursor.execute(delete_query)
    conn.commit()
    cursor.close()
    
    total_records_after_delete = spark.sql("SELECT COUNT(*) FROM sample_data").collect()[0][0]
    print("Data deleted successfully.")
    print(f"Total records after delete: {total_records_after_delete}")
except Exception as e:
    print(f"Error deleting data: {e}")

# Performance measurements
try:
    print("\n----------------Performance measurements------------")
    # Measure time for query without WHERE clause
    start_time = time.time()
    spark.sql("SELECT * FROM sample_data LIMIT 5").show()
    end_time = time.time()
    print("Time taken without WHERE clause:", end_time - start_time, "seconds\n")

    # Measure time for query with WHERE clause
    start_time = time.time()
    spark.sql("SELECT * FROM sample_data WHERE country = 'France' LIMIT 5").show()
    end_time = time.time()
    print("Time taken with WHERE clause:", end_time - start_time, "seconds")
except Exception as e:
    print(f"Error measuring performance: {e}")

# Close the connection
conn.close()

# Stop the Spark session
spark.stop()
