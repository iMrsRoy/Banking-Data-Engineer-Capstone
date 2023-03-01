import pandas as pd

def main():
    # read CSV file into a Pandas DataFrame
    df = pd.read_csv("path/to/input/file.csv")
    
    # display options to the user
    print("Welcome to the data viewer program!")
    print(f"Loaded data from file: path/to/input/file.csv")
    print("Select an option:")
    print("1. View data")
    print("2. Filter data")
    print("3. Exit")
    
    while True:
        # prompt the user for input
        choice = input("Enter your choice (1-3): ")
        
        # option 1: view data
        if choice == "1":
            print(df)
        
        # option 2: filter data
        elif choice == "2":
            # prompt the user for a filter condition
            column_name = input("Enter column name to filter by: ")
            operator = input("Enter operator (==, >, <, >=, <=, !=): ")
            value = input("Enter value to compare to: ")
            
            # filter the data and display the result
            filtered_df = df.query(f"{column_name} {operator} {value}")
            print(filtered_df)
        
        # option 3: exit
        elif choice == "3":
            print("Exiting program...")
            break
        
        # invalid input
        else:
            print("Invalid choice. Please enter a number between 1 and 3.")
    
if __name__ == "__main__":
    main()
