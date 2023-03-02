import warnings
import pyinputplus as pyin
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# create SparkSession
spark = SparkSession.builder \
    .appName("Bank Transactions") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# suppress pyspark warnings
warnings.filterwarnings("ignore", category=UserWarning, message=".*Please use.*")

# define a function to execute SQL queries and return results
def execute_query(query):
    return spark.sql(query).toPandas()

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
    print("1. Display transactions by zip code and date")
    print("2. Display transaction count and total value by type")
    print("3. Display transaction count and total value by branch state")
    print("4. Exit")

    # validate user input
    choice = validate_input("Enter your choice: ", int)
    if choice == 1:
        # display transactions by zip code and date
        zip_code = validate_input("Enter the zip code: ", int)
        year = validate_input("Enter the year: ", int)
        month = validate_input("Enter the month: ", int)
        result = execute_query(f"SELECT * FROM transactions WHERE zip_code = {zip_code} AND year = {year} AND month = {month} ORDER BY day DESC")
        if not result.empty:
            print(result)
        else:
            print("No transactions found.")
    elif choice == 2:
        # display transaction count and total value by type
        transaction_type = validate_input("Enter the transaction type: ", str)
        result = execute_query(f"SELECT COUNT(*) AS transaction_count, SUM(amount) AS total_value FROM transactions WHERE type = '{transaction_type}'")
        if not result.empty:
            print(result)
        else:
            print("No transactions found.")
    elif choice == 3:
        # display transaction count and total value by branch state
        state = validate_input("Enter the state: ", str)
        result = execute_query(f"SELECT COUNT(*) AS transaction_count, SUM(amount) AS total_value FROM transactions JOIN branches ON transactions.branch_id = branches.id WHERE branches.state = '{state}'")
        if not result.empty:
            print(result)
        else:
            print("No transactions found.")
    elif choice == 4:
        # exit
        break
    else:
        print("Invalid choice.")
        
# stop SparkSession
spark.stop()
