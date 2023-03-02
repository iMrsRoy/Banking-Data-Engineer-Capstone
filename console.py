import mysql.connector as mysql
import pyinputplus as pyip
import pandas as pd
import pyinputplus as pyin
import pandas.io.sql as psql
import warnings

# define MySQL connection parameters
mysql_hostname = "localhost"
mysql_port = 3306
mysql_database = "creditcard_capstone"
mysql_username = "root"
mysql_password = "ShaShi3493*"
# connect to MySQL database
cnx = mysql.connect(
    user=mysql_username,
    password=mysql_password,
    host=mysql_hostname,
    database=mysql_database)

query = "(SELECT * FROM cdw_sapp_branch bc \
      JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN)"

def execute_query(query):
    return mysql(query).toPandas()

# define a function to execute SQL queries and return results
def execute_query(query):
    cursor = cnx.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result


query = "(SELECT * FROM cdw_sapp_branch bc JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN)"


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


