import mysql.connector.errors as mysqlerrors
import pandas as pd

CREATE_DVD_TABLE = """
CREATE TABLE DVDs (
    d_id INT AUTO_INCREMENT PRIMARY KEY,  
    title VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_ci NOT NULL,           
    director VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_ci NOT NULL,        
    stock INT DEFAULT 2,
    CONSTRAINT unique_dvd UNIQUE (title, director)
);
"""

CREATE_USER_TABLE = """
CREATE TABLE Users (
    u_id INT AUTO_INCREMENT PRIMARY KEY,  
    name VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_ci NOT NULL,            
    age INT NOT NULL CHECK (age > 0),     
    borrow_count INT DEFAULT 0 CHECK (borrow_count >= 0), 
    CONSTRAINT unique_user UNIQUE (name, age)
);
"""

CREATE_BORROW_TABLE = """
CREATE TABLE BorrowRecords (
    record_id INT AUTO_INCREMENT PRIMARY KEY, 
    d_id INT NOT NULL,                        
    u_id INT NOT NULL,                        
    status ENUM('borrowed', 'returned') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_ci DEFAULT 'borrowed', 
    rating INT DEFAULT NULL CHECK (rating BETWEEN 1 AND 5), 
    FOREIGN KEY (d_id) REFERENCES DVDs(d_id) ON DELETE CASCADE,
    FOREIGN KEY (u_id) REFERENCES Users(u_id) ON DELETE CASCADE
);
"""


CREATE_TABLES = [CREATE_DVD_TABLE, CREATE_USER_TABLE, CREATE_BORROW_TABLE]

DROP_TABLES = [
    "DROP TABLE IF EXISTS BorrowRecords",
    "DROP TABLE IF EXISTS Users",
    "DROP TABLE IF EXISTS DVDs",
]


def drop_tables(cursor):
    for drop_sql in DROP_TABLES:
        try:
            cursor.execute(drop_sql)
        except mysqlerrors.Error as err:
            print(f"Error dropping table: {err.msg}")


def create_tables(cursor):
    for create_sql in CREATE_TABLES:
        try:
            cursor.execute(create_sql)
        except mysqlerrors.Error as err:
            print(f"Error creating table: {err.msg}")


def initialize_from_csv(cursor):
    file_path = "data.csv"
    data = pd.read_csv(file_path)

    unique_d = data[["d_id", "d_title", "d_name"]].drop_duplicates().sort_values("d_id")
    unique_u = data[["u_id", "u_name", "u_age"]].drop_duplicates().sort_values("u_id")

    for _, row in unique_d.iterrows():
        try:
            sql = "INSERT INTO DVDs (title, director) VALUES (%s, %s)"
            cursor.execute(sql, (row["d_title"], row["d_name"]))
        except mysqlerrors.DataError as e:
            print(e.msg)

    for _, row in unique_u.iterrows():
        sql = "INSERT INTO Users (name, age) VALUES (%s, %s)"
        cursor.execute(sql, (row["u_name"], row["u_age"]))

    for _, row in data.iterrows():
        sql = "INSERT INTO BorrowRecords (d_id, u_id, rating, status) VALUES (%s, %s, %s, 'returned')"
        cursor.execute(sql, (row["d_id"], row["u_id"], row["rating"]))


def select_users(cursor):
    query = """
        SELECT 
            Users.u_id AS 'id', 
            Users.name AS 'name', 
            Users.age AS 'age', 
            AVG(BorrowRecords.rating) AS 'avg.rating', 
            COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt'
        FROM 
            Users
        LEFT JOIN 
            BorrowRecords ON Users.u_id = BorrowRecords.u_id and BorrowRecords.status = 'returned'
        GROUP BY 
            Users.u_id
        ORDER BY 
            Users.u_id
    """
    cursor.execute(query)
    return cursor.fetchall()


def select_dvds(cursor):
    query = """
        SELECT 
            DVDs.d_id AS 'id', 
            DVDs.title AS 'title', 
            DVDs.director AS 'director', 
            AVG(BorrowRecords.rating) AS 'avg.rating', 
            COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt', 
            DVDs.stock AS 'quantity'
        FROM 
            DVDs
        LEFT JOIN 
            BorrowRecords ON DVDs.d_id = BorrowRecords.d_id and BorrowRecords.status = 'returned'
        GROUP BY 
            DVDs.d_id
        ORDER BY 
            AVG(BorrowRecords.rating) DESC, 
            COUNT(BorrowRecords.record_id) DESC,
            DVDs.d_id ASC
    """
    cursor.execute(query)
    return cursor.fetchall()


def select_borrowed_dvds_by_user(cursor, user_id):
    query = """
                SELECT 
                    DVDs.d_id AS 'id', 
                    DVDs.title AS 'title', 
                    DVDs.director AS 'director', 
                    (SELECT AVG(rating) FROM BorrowRecords WHERE d_id = DVDs.d_id) AS 'avg.rating'
                FROM 
                    BorrowRecords
                JOIN 
                    DVDs ON BorrowRecords.d_id = DVDs.d_id
                WHERE 
                    BorrowRecords.u_id = %s AND BorrowRecords.status = 'borrowed'
                GROUP BY 
                    DVDs.d_id
                ORDER BY 
                    DVDs.d_id ASC
            """
    cursor.execute(query, (user_id,))
    return cursor.fetchall()


def search_dvd_by_title(cursor, search_query):
    sql = """
            SELECT 
                DVDs.d_id AS 'id', 
                DVDs.title AS 'title', 
                DVDs.director AS 'director', 
                AVG(BorrowRecords.rating) AS 'avg.rating', 
                COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt', 
                DVDs.stock AS 'quantity'
            FROM 
                DVDs
            LEFT JOIN 
                BorrowRecords ON DVDs.d_id = BorrowRecords.d_id and BorrowRecords.status = 'returned'
            WHERE 
                LOWER(DVDs.title) LIKE LOWER(%s)
            GROUP BY 
                DVDs.d_id
            ORDER BY 
                AVG(BorrowRecords.rating) DESC, 
                COUNT(BorrowRecords.record_id) DESC,
                DVDs.d_id ASC
        """
    cursor.execute(sql, (search_query,))
    return cursor.fetchall()


def search_director_query(cursor, search_query):
    sql = """
            SELECT 
                DVDs.director AS 'director', 
                AVG(BorrowRecords.rating) AS 'director_rating', 
                COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt', 
                CONCAT(
                    '[', 
                    GROUP_CONCAT(DISTINCT DVDs.title ORDER BY DVDs.title ASC SEPARATOR ', '), 
                    ']'
                ) AS 'all_movies'
            FROM 
                DVDs
            LEFT JOIN 
                BorrowRecords ON DVDs.d_id = BorrowRecords.d_id and BorrowRecords.status = 'returned'
            WHERE 
                LOWER(DVDs.director) LIKE LOWER(%s)
            GROUP BY 
                DVDs.director
            ORDER BY 
                AVG(BorrowRecords.rating) DESC, 
                COUNT(BorrowRecords.record_id) DESC,
                DVDs.director ASC
        """
    cursor.execute(sql, (search_query,))
    return cursor.fetchall()


def get_highest_rated_dvd(cursor, user_id):
    query = """
        SELECT 
            DVDs.d_id AS 'id', 
            DVDs.title AS 'title', 
            DVDs.director AS 'director', 
            AVG(BorrowRecords.rating) AS 'avg_rating', 
            COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt', 
            DVDs.stock AS 'quantity'
                FROM 
                    DVDs
                LEFT JOIN 
                    BorrowRecords ON DVDs.d_id = BorrowRecords.d_id
                WHERE 
                    DVDs.d_id NOT IN (
                        SELECT d_id 
                        FROM BorrowRecords 
                        WHERE u_id = %s AND rating IS NOT NULL
                    )
                GROUP BY 
                    DVDs.d_id
                ORDER BY 
                    AVG(BorrowRecords.rating) DESC, 
                    COUNT(BorrowRecords.record_id) DESC, 
                    DVDs.d_id ASC
                LIMIT 1
            """
    cursor.execute(query, (user_id,))
    return cursor.fetchone()


def get_most_borrowed_dvd(cursor, user_id):
    query = """
        SELECT 
            DVDs.d_id AS 'id', 
                    DVDs.title AS 'title', 
                    DVDs.director AS 'director', 
                    AVG(BorrowRecords.rating) AS 'avg_rating', 
                    COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt', 
                    DVDs.stock AS 'quantity'
                FROM 
                    DVDs
                LEFT JOIN 
                    BorrowRecords ON DVDs.d_id = BorrowRecords.d_id
                WHERE 
                    DVDs.d_id NOT IN (
                        SELECT d_id 
                        FROM BorrowRecords 
                        WHERE u_id = %s AND rating IS NOT NULL
                    )
                GROUP BY 
                    DVDs.d_id
                ORDER BY 
                    COUNT(BorrowRecords.record_id) DESC, 
                    AVG(BorrowRecords.rating) DESC, 
                    DVDs.d_id ASC
                LIMIT 1
            """
    cursor.execute(query, (user_id,))
    return cursor.fetchone()


def check_dvd(cursor, d_id):
    cursor.execute("SELECT d_id FROM DVDs WHERE d_id = %s", (d_id,))
    return cursor.fetchone() is not None


def check_user(cursor, u_id):
    cursor.execute("SELECT u_id FROM Users WHERE u_id = %s", (u_id,))
    return cursor.fetchone() is not None


def check_stock(cursor, d_id):
    cursor.execute("SELECT stock FROM DVDs WHERE d_id = %s", (d_id,))
    return cursor.fetchone()[0]


def check_borrowed_by_user(cursor, user_id):
    query = """
        SELECT record_id 
        FROM BorrowRecords 
        WHERE u_id = %s AND status = 'borrowed'
    """
    cursor.execute(query, (user_id,))
    return cursor.fetchone() is not None


def check_if_dvd_borrowed_by_user(cursor, user_id, dvd_id):
    query = """
                SELECT record_id 
                FROM BorrowRecords 
        WHERE u_id = %s AND d_id = %s AND status = 'borrowed'
    """
    cursor.execute(query, (user_id, dvd_id))
    return cursor.fetchone() is not None


def check_borrow_count(cursor, u_id):
    cursor.execute("SELECT borrow_count FROM Users WHERE u_id = %s", (u_id,))
    return cursor.fetchone()[0]


def insert_user(cursor, name, age):
    cursor.execute("INSERT INTO Users (name, age) VALUES (%s, %s)", (name, age))


def insert_dvd(cursor, title, director):
    cursor.execute(
        "INSERT INTO DVDs (title, director) VALUES (%s, %s)", (title, director)
    )


def insert_borrow_record(cursor, DVD_id, user_id):
    borrow_dvd_query = "INSERT INTO BorrowRecords (d_id, u_id) VALUES (%s, %s)"
    cursor.execute(borrow_dvd_query, (DVD_id, user_id))


def update_stock_increment(cursor, DVD_id):
    update_stock_increment_query = "UPDATE DVDs SET stock = stock + 1 WHERE d_id = %s"
    cursor.execute(update_stock_increment_query, (DVD_id,))


def update_stock_decrement(cursor, DVD_id):
    update_stock_decrement_query = "UPDATE DVDs SET stock = stock - 1 WHERE d_id = %s"
    cursor.execute(update_stock_decrement_query, (DVD_id,))


def update_borrow_count_increment(cursor, user_id):
    update_borrow_count_increment_query = (
        "UPDATE Users SET borrow_count = borrow_count + 1 WHERE u_id = %s"
    )
    cursor.execute(update_borrow_count_increment_query, (user_id,))


def update_borrow_count_decrement(cursor, user_id):
    update_borrow_count_decrement_query = (
        "UPDATE Users SET borrow_count = borrow_count - 1 WHERE u_id = %s"
    )
    cursor.execute(update_borrow_count_decrement_query, (user_id,))


def update_borrow_record(cursor, rating, user_id, DVD_id):
    query = """
                SELECT record_id 
                FROM BorrowRecords 
                WHERE u_id = %s AND d_id = %s AND status = 'borrowed'
            """
    cursor.execute(query, (user_id, DVD_id))
    borrow_record = cursor.fetchone()
    if borrow_record:
        update_query = """
                    UPDATE BorrowRecords 
                    SET status = 'returned', rating = %s 
                    WHERE record_id = %s
                """
        cursor.execute(update_query, (rating, borrow_record[0]))


def delete_dvd(cursor, d_id):
    cursor.execute("DELETE FROM DVDs WHERE d_id = %s", (d_id,))


def delete_user(cursor, u_id):
    cursor.execute("DELETE FROM Users WHERE u_id = %s", (u_id,))
