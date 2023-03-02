import pyspark as psk
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyin
import mysql.connector as mysql
import warnings

# suppress warnings
warnings.filterwarnings('ignore')

# create a SparkSession
spark = SparkSession\
    .builder\
    .appName("PYCONSOLE")\
    .config("spark.jars","/Users/roy/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar")\
    .getOrCreate()

# define MySQL connection parameters
mysql_hostname = "localhost"
mysql_port = 3306
mysql_database = "creditcard_capstone"
mysql_username = "root"
mysql_password = "ShaShi3493*"

# define a function to execute SQL queries and return a PySpark DataFrame

query = "(SELECT * FROM cdw_sapp_branch bc \
      JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN)"

def execute_query(query):
    df = spark.read.format("jdbc")\
        .option("url", f"jdbc:mysql://{mysql_hostname}:{mysql_port}/{mysql_database}")\
            .option("driver", "/Users/roy/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar")\
                .option("dbtable", f"({query})")\
                    .option("user", mysql_username)\
                        .option("password", mysql_password)\
                            .load()
    return df
def execute_query(query):
    try:
        result = spark.sql(query).toPandas()
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None


# define a function to validate user input
def validate_input(prompt, expected_type):
    while True:
        user_input = pyin.inputStr(prompt)
        try:
            validated_input = expected_type(user_input)
            return validated_input
        except ValueError:
            print(f"Invalid input. Please enter a {expected_type.__name__}.")

# define a loop for user interaction
while True:
    print("Select an option:")
    print("1. Display transactions by zip code and month/year")
    print("2. Display number and total value of transactions by type")
    print("3. Display total number and value of transactions by state")
    print("4. Exit")

    # validate user input
    choice = validate_input("Enter your choice: ", int)
    if choice == 1:
        #1)    Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
       
        zip_code = validate_input("Enter the zip code: ", int)
        year = validate_input("Enter the year (YYYY): ", int)
        month = validate_input("Enter the month (MM): ", int)
        result = execute_query(f"SELECT * FROM CDW_SAPP_CUSTOMER, CDW_SAPP_BRANCH WHERE CUST_ZIP = {zip_code} AND YEAR(date) = {year} AND MONTH(date) = {month} ORDER BY date DESC")
        if result:
            pd_result = result.toPandas()
            print(pd_result)
        else:
            print("No transactions found.")
    elif choice == 2:
        #2)    Used to display the number and total values of transactions for a given type.
      
        transaction_type = validate_input("Enter the transaction type: ", str)
        result = execute_query(f"SELECT TRANSACTION_VALUE, TRANSACTION_TYPE FROM '{query}' where TRANSACTION_TYPE  = '{transaction_type}'")
        if result:
            pd_result = result.toPandas()
            print(pd_result)
        else:
            print("No transactions found.")
    elif choice == 3:
        #3)    Used to display the total number and total values of transactions for branches in a given state.
    
        state = validate_input("Enter the state: ", str)
        result = execute_query(f"SELECT bc.state, COUNT(*) as count, SUM(TRANSACTION_VALUE) as total_value FROM cdw_sapp_branch bc JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE WHERE BRANCH_STATE = '{state}'")
        if result:
            pd_result = result.toPandas()
            print(pd_result)
        else:
            print("No transactions found.")
    elif choice == 4:
        # exit
        break
    else:
        print("Invalid choice.")

        