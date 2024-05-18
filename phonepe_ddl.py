import mysql.connector
from mysql.connector import Error

try:
    # Establish a connection to the MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='phonephe'
    )
    mycursor = mydb.cursor(buffered=True) 
    print("Database connection successful.")
except Error as e:
    print(f"Error: {e}")
    print("DB connection failed.")

def phonepe_tables():
    queries = [
        '''
        CREATE TABLE IF NOT EXISTS aggregate_transaction (
            state VARCHAR(225) DEFAULT NULL,
            year INT(11) DEFAULT NULL,
            Quater INT(11) DEFAULT NULL,
            transaction_type VARCHAR(225) DEFAULT NULL,
            transaction_count INT(11) DEFAULT NULL,
            transaction_amount FLOAT DEFAULT NULL
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS aggregate_user (
            State VARCHAR(225) DEFAULT NULL,
            Year INT(11) DEFAULT NULL,
            Quater INT(11) DEFAULT NULL,
            Registered_Users VARCHAR(225) DEFAULT NULL,
            App_Opens VARCHAR(225) DEFAULT NULL
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS map_tran (
            State VARCHAR(225) DEFAULT NULL,
            Year INT(11) DEFAULT NULL,
            Quater INT(11) DEFAULT NULL,
            District VARCHAR(225) DEFAULT NULL,
            Transaction_count VARCHAR(225) DEFAULT NULL,
            Transaction_amount VARCHAR(225) DEFAULT NULL
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS map_user (
            State VARCHAR(225) DEFAULT NULL,
            Year INT(11) DEFAULT NULL,
            Quater INT(11) DEFAULT NULL,
            Registered_Users VARCHAR(225) DEFAULT NULL,
            App_Opens VARCHAR(225) DEFAULT NULL,
            District VARCHAR(225) DEFAULT NULL
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS top_tran (
            State VARCHAR(225) DEFAULT NULL,
            Year INT(11) DEFAULT NULL,
            Quater INT(11) DEFAULT NULL,
            pincode INT(11) DEFAULT NULL,
            count INT(11) DEFAULT NULL,
            amount FLOAT DEFAULT NULL
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS top_user (
            State VARCHAR(225) DEFAULT NULL,
            Year INT(11) DEFAULT NULL,
            Quater INT(11) DEFAULT NULL,
            pincode INT(11) DEFAULT NULL,
            Registered_user INT(11) DEFAULT NULL
        );
        ''',
        '''
    CREATE TABLE IF NOT EXISTS mobile_users (
        state VARCHAR(225),
        year VARCHAR(225),
        quater INT,
        brand VARCHAR(225),
        brand_count INT,
        brand_percentage FLOAT
    )
'''




    ]

    for query in queries:
        try:
            mycursor.execute(query)
            mydb.commit()
            print(f"Table created successfully or already exists.")
        except Error as e:
            print(f"Error: {e}")
            print("Failed to create table.")

# Call the function to create the tables
phonepe_tables()

# Close the cursor and connection
mycursor.close()
mydb.close()
