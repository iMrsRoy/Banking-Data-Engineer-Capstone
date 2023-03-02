import mysql.connector

# define MySQL connection parameters
mysql_hostname = "localhost"
mysql_port = 3306
mysql_database = "creditcard_capstone"
mysql_username = "root"
mysql_password = "ShaShi3493*"
# connect to MySQL database
cnx = mysql.connector.connect(user=mysql_username, password=mysql_password,
                              host=mysql_hostname, database=mysql_database)

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

# define a function to validate user input
def validate_input(prompt, expected_type):
    while True:
        user_input = input(prompt)
        try:
            validated_input = expected_type(user_input)
            return validated_input
        except ValueError:
            print(f"Invalid input. Please enter a {expected_type.__name__}.")

# define a loop for user interaction
while True:
    print("Select an option:")
    print("1. View all records")
    print("2. View a specific record by ID")
    print("3. Add a new record")
    print("4. Update an existing record")
    print("5. Delete a record")
    print("6. Exit")

    # validate user input
    choice = validate_input("Enter your choice: ", int)
    if choice == 1:
        # view all records
        result = execute_query("SELECT * FROM my_table")
        for row in result:
            print(row)
    elif choice == 2:
        # view a specific record by ID
        id = validate_input("Enter the ID of the record you want to view: ", int)
        result = execute_query(f"SELECT * FROM my_table WHERE id = {id}")
        if result:
            print(result[0])
        else:
            print("Record not found.")
    elif choice == 3:
        # add a new record
        name = input("Enter the name: ")
        age = validate_input("Enter the age: ", int)
        execute_query(f"INSERT INTO my_table (name, age) VALUES ('{name}', {age})")
        cnx.commit()
        print("Record added.")
    elif choice == 4:
        # update an existing record
        id = validate_input("Enter the ID of the record you want to update: ", int)
        result = execute_query(f"SELECT * FROM my_table WHERE id = {id}")
        if result:
            print(f"Current record: {result[0]}")
            name = input("Enter the new name (leave blank to keep current value): ")
            age = input("Enter the new age (leave blank to keep current value): ")
            if name == "":
                name = result[0][1]
            if age == "":
                age = result[0][2]
            execute_query(f"UPDATE my_table SET name = '{name}', age = {age} WHERE id = {id}")
            cnx.commit()
            print("Record updated.")
        else:
            print("Record not found.")
    elif choice == 5:
        # delete a record
        id = validate_input("Enter the ID of the record you want to delete: ", int)
        execute_query(f"DELETE FROM my_table WHERE id = {id}")
        cnx.commit()
        print("Record deleted.")
    elif choice == 6:
        # exit
        break
    else:
        print("Invalid choice.")
        
# close MySQL connection

