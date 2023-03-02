# Import required libraries
import configparser
import pymysql
import pyspark
from pyspark.sql.functions import desc
from pyspark.sql import SQLContext 
sqlContext = SQLContext()

# Load configuration from config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

# Connect to database
conn = pymysql.connect(
    host=config['database']['host'],
    user=config['database']['username'],
    password=config['database']['password'],
    database='creditcard_capstone'
)

# Load data into Spark dataframes
customerdf = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/creditcard_capstone").option("dbtable", "customer").load()
creditdf = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/creditcard_capstone").option("dbtable", "credit_card").load()
branchdf = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/creditcard_capstone").option("dbtable", "branch").load()

# Define functions for displaying transaction data
def display_transactions(zipcode, month, year):
    transactions = customerdf.join(creditdf, 'CREDIT_CARD_NO', 'inner').select('TRANSACTION_VALUE', zipcode, 'DAY', 'MONTH', 'YEAR').where((customerdf.ZIP_CODE == zipcode) & (creditdf.MONTH == month) & (creditdf.YEAR == year)).sort(desc('DAY'))
    transactions.show()

def display_values(transaction_type):
    values = creditdf.select(transaction_type, 'TRANSACTION_VALUE').groupBy(transaction_type).sum('TRANSACTION_VALUE')
    values.show()

def display_transactions_by_state(state):
    transactions = branchdf.join(creditdf, 'BRANCH_CODE', 'inner').select('TRANSACTION_VALUE', 'BRANCH_STATE').where(branchdf.BRANCH_STATE == state).groupBy('BRANCH_STATE').sum('TRANSACTION_VALUE')
    transactions.show()

# Main program loop
while True:
    print("Select an option:")
    print("1. Display transactions by ZIP code")
    print("2. Display transactions by type")
    print("3. Display transactions by state")
    print("0. Exit")

    option = input()

    if option == '1':
        zipcode = input("Enter ZIP code: ")
        month = input("Enter month (1-12): ")
        year = input("Enter year (yyyy): ")
        display_transactions(zipcode, month, year)
    elif option == '2':
        transaction_type = input("Enter transaction type: ")
        display_values(transaction_type)
    elif option == '3':
        state = input("Enter state: ")
        display_transactions_by_state(state)
    elif option == '0':
        break
    else:
        print("Invalid option")

# Create a SparkSession
spark = SparkSession.builder.appName("CreditCardApp").getOrCreate()

jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/creditcard_capstone").option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "ShaShi3493*") \
    .load()

customer_df = jdbcDF.toPandas()
cust_ssn = list(customer_df['SSN'])


def check_account_details():
    customer = customer_df.loc[customer_df['SSN'] == SSN]
    if customer.empty:
        print("No customer found with the given ID.")
    else:
        print(customer)


def modify_account_details(customer_df, new_details):
    customer = customer_df.loc[customer_df['SSN'] == SSN]
    if customer.empty:
        print("No customer found with the given ID.")
    else:
        customer_df.loc[customer_df['customer_id'] == customer_id] = new_details
        print("Customer details updated successfully.")

def generate_monthly_bill(credit_card_no, month, year):
    transactions_df = pd.read_sql_query(f"SELECT * FROM transaction WHERE credit_card_number='{credit_card_no}' AND MONTH(date)={month} AND YEAR(date)={year}", jdbcDF)
    if transactions_df.empty:
        print("No transactions found for the given credit card number and date.")
    else:
        total_amount = transactions_df['amount'].sum()
        print(f"Credit card number: {credit_card_no}")
        print(f"Month: {month}")
        print(f"Year: {year}")
        print(f"Total amount: {total_amount}")

# Close the SparkSession
spark.stop()
