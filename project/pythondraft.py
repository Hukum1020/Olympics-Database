import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error

# Database connection function
def create_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="nik101102"  # Replace with your actual password
        )
        if connection.is_connected():
            print("Connected to MySQL server")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Create a new database
def create_new_database(connection):
    cursor = connection.cursor()
    try:
        db_name = input("Enter the name of the new database: ").strip()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")
        print(f"Database '{db_name}' is now in use.")
    except Error as e:
        print(f"Error while creating the database: {e}")

# Use an existing database
def use_existing_database(connection):
    cursor = connection.cursor()
    try:
        db_name = input("Enter the name of the existing database: ").strip()
        
        # Check if the database exists
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        database_names = [db[0] for db in databases]
        
        if db_name not in database_names:
            print(f"Error: The database '{db_name}' does not exist.")
            return
        
        # Use the database
        cursor.execute(f"USE {db_name}")
        print(f"Using database '{db_name}'.")
    except Error as e:
        print(f"Error while using the database: {e}")

# Create tables based on provided schema
def create_tables(connection):
    cursor = connection.cursor()
    
    # Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    # Drop tables if they already exist to avoid duplication
    tables = ['Sport_result', 'Participation', 'Athlete', 'Team', 'Games']
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    
    # Define SQL commands for creating tables
    create_team_table = """
    CREATE TABLE Team (
        Team VARCHAR(100) PRIMARY KEY,
        Country_Code VARCHAR(3)
    );
    """
    
    create_athlete_table = """
    CREATE TABLE Athlete (
        ID INT PRIMARY KEY,
        Name_Surname VARCHAR(255) NOT NULL,
        Gender CHAR(1),
        Age INT,
        Height FLOAT,
        Weight FLOAT,
        Team VARCHAR(100),
        FOREIGN KEY (Team) REFERENCES Team(Team)
    );
    """
    
    create_games_table = """
    CREATE TABLE Games (
        Games VARCHAR(100) PRIMARY KEY,
        Year_ INT,
        Season ENUM('Summer', 'Winter'),
        City VARCHAR(100)
    );
    """
    
    create_participation_table = """
    CREATE TABLE Participation (
        Athlete_id INT NOT NULL,
        Games VARCHAR(100),
        PRIMARY KEY (Athlete_id, Games),
        FOREIGN KEY (Athlete_id) REFERENCES Athlete(ID),
        FOREIGN KEY (Games) REFERENCES Games(Games)
    );
    """
    
    create_sport_result_table = """
    CREATE TABLE Sport_result (
        Athlete_id INT NOT NULL,
        Event_name VARCHAR(255),
        Sport_name VARCHAR(255),
        Medal ENUM('GOLD', 'Silver', 'Bronze', 'NA'),
        PRIMARY KEY (Athlete_id, Event_name, Sport_name, Medal),
        FOREIGN KEY (Athlete_id) REFERENCES Athlete(ID)
    );
    """
    
    # Execute table creation commands in correct order
    cursor.execute(create_team_table)
    cursor.execute(create_athlete_table)
    cursor.execute(create_games_table)
    cursor.execute(create_participation_table)
    cursor.execute(create_sport_result_table)
    
    connection.commit()
    print("Tables created successfully")
    
# Load data into tables
def load_data(connection, filepath):
    cursor = connection.cursor()
    
    # Load CSV data into DataFrame
    df = pd.read_csv(filepath)
    
    # Drop duplicates and fill NaN values if necessary
    df = df.drop_duplicates()
    df = df.fillna({'Medal': 'NA'})
    
    # Ensure correct data types
    df['Medal'] = df['Medal'].replace({np.nan: 'NA'})
    df['Gender'] = df['Gender'].astype(str)
    
    # Dictionaries to keep track of inserted records to avoid duplicates
    team_dict = {}
    athlete_dict = {}
    games_dict = {}
    
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Insert into Team table
        team_name = row['Team']
        country_code = row['NOC']
        if team_name not in team_dict:
            cursor.execute("""
                INSERT INTO Team (Team, Country_Code)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Team=Team
            """, (team_name, country_code))
            team_dict[team_name] = team_name  # Using team name as primary key

        # Insert into Athlete table
        athlete_id = row['ID']
        if athlete_id not in athlete_dict:
            cursor.execute("""
                INSERT INTO Athlete (ID, Name_Surname, Gender, Age, Height, Weight, Team)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE ID=ID
            """, (
                athlete_id,
                row['Name'],
                row['Gender'],
                row['Age'],
                row['Height'],
                row['Weight'],
                team_name
            ))
            athlete_dict[athlete_id] = athlete_id

        # Insert into Games table
        games_name = row['Games']
        if games_name not in games_dict:
            cursor.execute("""
                INSERT INTO Games (Games, Year_, Season, City)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Games=Games
            """, (
                games_name,
                row['Year'],
                row['Season'],
                row['City']
            ))
            games_dict[games_name] = games_name  # Using games name as primary key

        # Insert into Participation table
        cursor.execute("""
            INSERT IGNORE INTO Participation (Athlete_id, Games)
            VALUES (%s, %s)
        """, (athlete_id, games_name))

        # Insert into Sport_result table
        medal = row['Medal'] if row['Medal'] in ['GOLD', 'Silver', 'Bronze'] else 'NA'
        cursor.execute("""
            INSERT IGNORE INTO Sport_result (Athlete_id, Event_name, Sport_name, Medal)
            VALUES (%s, %s, %s, %s)
        """, (
            athlete_id,
            row['Games'],
            row['Event'],
            medal
        ))
    
    connection.commit()
    print("Data loaded into tables")

# Run SQL queries
def run_query(connection, query):
    if not connection.is_connected():
        connection.reconnect()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            print(row)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Main command-line interface
def main():
    connection = create_connection()
    
    if connection is not None:
        while True:
            print("\nChoose an option:")
            print("1. Create a new database")
            print("2. Use an existing database")
            print("3. Create tables")
            print("4. Load data from CSV")
            print("5. Run SQL query")
            print("6. Exit")
            
            choice = input("Enter your choice: ")
            
            if choice == "1":
                create_new_database(connection)
            elif choice == "2":
                use_existing_database(connection)
            elif choice == "3":
                create_tables(connection)
            elif choice == "4":
                filepath = input("\nEnter the CSV file path: ")
                load_data(connection, filepath)
            elif choice == "5":
                query = input("\nEnter your SQL query: ")
                run_query(connection, query)
            elif choice == "6":
                connection.close()
                print("Connection closed.")
                break
            else:
                print("\nInvalid choice, please try again.")

if __name__ == "__main__":
    main()