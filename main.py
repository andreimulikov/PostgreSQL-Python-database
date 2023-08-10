import psycopg2
from config import host, user, password, db_name

try:
    # connect to exist database
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    connection.autocommit = True

    # the cursor for performing database operations
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT version();"
        )

        print(f"Server version: {cursor.fetchone()}")

    # create a new table
    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE united_players(
            id serial PRIMARY KEY,
            first_name varchar(50) NOT NULL,
            last_name varchar(50) NOT NULL,
            position varchar(50) NOT NULL,
            number int NOT NULL,
            date_of_birth date NOT NULL,
            country varchar(50) NOT NULL);"""
        )

        print("[INFO] Table created successfully")

    # insert data into the table
    with connection.cursor() as cursor:
        postgres_insert_query = """
        INSERT INTO united_players (first_name, last_name, position, number, date_of_birth, country)
        VALUES (%s,%s,%s,%s,%s,%s)"""
        records_to_insert = [
            ('Tom', 'Heaton', 'Goalkeeper', '22', '1986-04-15', 'England'),
            ('Andre', 'Onana', 'Goalkeeper', '24', '1996-04-02', 'Cameroon'),
            ('Dean', 'Henderson', 'Goalkeeper', '26', '1997-03-12', 'England'),
            ('Victor', 'Lindelof', 'Defender', '2', '1994-07-17', 'Sweden'),
            ('Eric', 'Bailly', 'Defender', '3', '1994-04-12', 'Ivory Coast'),
            ('Harry', 'Maguire', 'Defender', '5', '1993-03-05', 'England'),
            ('Lisandro', 'Martinez', 'Defender', '6', '1998-01-18', 'Argentina'),
            ('Tyrell', 'Malacia', 'Defender', '12', '1999-08-17', 'Netherlands'),
            ('Raphael', 'Varane', 'Defender', '19', '1993-04-25', 'France'),
            ('Diogo', 'Dalot', 'Defender', '20', '1999-03-18', 'Portugal'),
            ('Luke', 'Shaw', 'Defender', '23', '1995-07-12', 'England'),
            ('Jonny', 'Evans', 'Defender', '27', '1988-01-03', 'Northern Ireland'),
            ('Aaron', 'Wan-Bissaka', 'Defender', '29', '1997-11-26', 'England'),
            ('Brandon', 'Williams', 'Defender', '33', '2000-09-03', 'England'),
            ('Alvaro', 'Fernandez', 'Defender', '42', '2003-03-23', 'Spain'),
            ('Teden', 'Mengi', 'Defender', '43', '2002-04-30', 'England'),
            ('Mason', 'Mount', 'Midfielder', '7', '1999-01-10', 'England'),
            ('Bruno', 'Fernandes', 'Midfielder', '8', '1994-09-08', 'Portugal'),
            ('Christian', 'Eriksen', 'Midfielder', '14', '1992-02-14', 'Denmark'),
            ('Amad', 'Diallo', 'Midfielder', '16', '2002-07-11', 'Ivory Coast'),
            ('Fred', 'de Paula Santos', 'Midfielder', '17', '1993-03-05', 'Brazil'),
            ('Carlos', 'Casemiro', 'Midfielder', '18', '1992-02-23', 'Brazil'),
            ('Facundo', 'Pellistri', 'Midfielder', '28', '2001-12-20', 'Uruguay'),
            ('Donny', 'Van de Beek', 'Midfielder', '34', '1997-04-18', 'Netherlands'),
            ('Kobbie', 'Mainoo', 'Midfielder', '37', '2005-04-19', 'England'),
            ('Scott', 'McTominay', 'Midfielder', '39', '1996-12-08', 'Scotland'),
            ('Hannibal', 'Mejbri', 'Midfielder', '46', '2003-01-21', 'Tunisia'),
            ('Anthony', 'Martial', 'Forward', '9', '1995-12-05', 'France'),
            ('Marcus', 'Rashford', 'Forward', '10', '1997-10-31', 'England'),
            ('Mason', 'Greenwood', 'Forward', '11', '2001-10-01', 'England'),
            ('Antony', 'dos Santos', 'Forward', '21', '2000-02-24', 'Brazil'),
            ('Jadon', 'Sancho', 'Forward', '25', '2000-05-25', 'England'),
            ('Shola', 'Shoretire', 'Forward', '47', '2004-02-02', 'England'),
            ('Alejandro', 'Garnacho', 'Forward', '49', '2004-07-01', 'Argentina'),
            ('Rasmus', 'Hojlund', 'Forward', '100', '2003-02-04', 'Denmark')
        ]
        cursor.executemany(postgres_insert_query, records_to_insert)

        print("[INFO] Data was successfully inserted")

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM united_players;"
        )

        print(cursor.fetchall())

except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
