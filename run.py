from mysql.connector import connect, errorcode
from mysql.connector.abstracts import MySQLConnectionAbstract
from messages import Messages
import queries
import mysql.connector.errors as mysqlerrors
from utils import print_records, print_records_recommend
import pandas as pd


connection: MySQLConnectionAbstract = connect(
    host="astronaut.snu.ac.kr",
    port=7000,
    user="DB2021_18641",
    password="DB2021_18641",
    db="DB2021_18641",
    charset="utf8mb4",
    collation="utf8mb4_bin",
)

# todo: case insensitive하게 변경
# 글자수 제한 변경
# 양의 "정수" 만 받도록


def drop_tables(cursor):
    for drop_sql in queries.DROP_TABLES:
        try:
            cursor.execute(drop_sql)
        except mysqlerrors.Error as err:
            print(f"Error dropping table: {err.msg}")


def create_tables(cursor):
    for create_sql in queries.CREATE_TABLES:
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
            if e.errno == errorcode.ER_DATA_TOO_LONG:
                print(f"Error: The title '{row['d_title']}' is too long.")
            else:
                print(e.msg)

    for _, row in unique_u.iterrows():
        sql = "INSERT INTO Users (name, age) VALUES (%s, %s)"
        cursor.execute(sql, (row["u_name"], row["u_age"]))

    for _, row in data.iterrows():
        sql = "INSERT INTO BorrowRecords (d_id, u_id, rating, status) VALUES (%s, %s, %s, 'returned')"
        cursor.execute(sql, (row["d_id"], row["u_id"], row["rating"]))


def initialize_database():
    cursor = connection.cursor()
    try:
        drop_tables(cursor)
        create_tables(cursor)
        initialize_from_csv(cursor)
        connection.commit()
        print("Database successfully initialized")
    except Exception as e:
        print(f"Error during database initialization: {e}")
    finally:
        cursor.close()


def reset():
    cursor = connection.cursor()
    try:
        drop_tables(cursor)
        create_tables(cursor)
        initialize_from_csv(cursor)
        connection.commit()
        print("Database successfully reset")
    except Exception as e:
        print(f"Error during database reset: {e}")
    finally:
        cursor.close()


def print_DVDs():
    # YOUR CODE GOES HERE
    # print msg
    cursor = connection.cursor(dictionary=True)
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
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        if not results:
            results = []
        print_records(results, headers)
    except Exception as e:
        print(e)
        return
    finally:
        cursor.close()


def print_users():
    cursor = connection.cursor(dictionary=True)
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
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        if not results:
            results = []
        print_records(results, headers)
    except Exception as e:
        print(e)
        return
    finally:
        cursor.close()


def insert_DVD():
    title = input("DVD title: ")
    director = input("DVD director: ")
    # YOUR CODE GOES HERE
    cursor = connection.cursor()

    try:
        if len(title) < 1 or len(title) > 100:
            print(Messages.get_message("E1"))
            return

        if len(director) < 1 or len(director) > 40:
            print(Messages.get_message("E2"))
            return

        sql = "INSERT INTO DVDs (title, director) VALUES (%s, %s)"
        cursor.execute(sql, (title, director))
        connection.commit()
        print(Messages.get_message("S3"))
    except mysqlerrors.IntegrityError:
        print(Messages.get_message("E3", title=title, director=director))
    except mysqlerrors.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
    pass


def remove_DVD():
    DVD_id = input("DVD ID: ")
    # YOUR CODE GOES HERE
    # print msg
    cursor = connection.cursor()

    try:
        check_dvd = "SELECT d_id FROM DVDs WHERE d_id = %s"
        cursor.execute(check_dvd, (DVD_id,))
        if not cursor.fetchone():
            print(Messages.get_message("E5", d_id=DVD_id))
            return

        check_borrowed = """
            SELECT record_id 
            FROM BorrowRecords 
            WHERE d_id = %s AND status = 'borrowed'
        """
        cursor.execute(check_borrowed, (DVD_id,))
        if cursor.fetchone() is not None:
            print(Messages.get_message("E6"))
            return

        delete_dvd = "DELETE FROM DVDs WHERE d_id = %s"
        cursor.execute(delete_dvd, (DVD_id,))
        connection.commit()
        print(Messages.get_message("S5"))

    except mysqlerrors.Error as err:
        print(f"Error: {err}")
        connection.rollback()
    finally:
        cursor.close()


def insert_user():
    name = input("User name: ")
    age = input("User age: ")
    # YOUR CODE GOES HERE
    # print msg

    try:
        cursor = connection.cursor()
        if len(name) < 1 or len(name) > 30:
            print(Messages.get_message("E4"))
            return

        if not age.isdigit() or int(age) <= 0:
            print(Messages.get_message("E14"))
            return

        age = int(age)

        sql = "INSERT INTO Users (name, age) VALUES (%s, %s)"
        cursor.execute(sql, (name, age))
        connection.commit()
        print(Messages.get_message("S2"))
    except mysqlerrors.IntegrityError:
        print(Messages.get_message("E13", name=name, age=age))
    except mysqlerrors.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
    pass


def remove_user():
    user_id = input("User ID: ")
    # YOUR CODE GOES HERE
    # print msg
    cursor = connection.cursor()

    try:
        check_user = "SELECT u_id FROM Users WHERE u_id = %s"
        cursor.execute(check_user, (user_id,))
        if not cursor.fetchone():
            print(Messages.get_message("E7", u_id=user_id))
            return

        check_borrowed = """
            SELECT record_id 
            FROM BorrowRecords 
            WHERE u_id = %s AND status = 'borrowed'
        """
        cursor.execute(check_borrowed, (user_id,))
        if cursor.fetchone() is not None:
            print(Messages.get_message("E8"))
            return

        delete_user = "DELETE FROM Users WHERE u_id = %s"
        cursor.execute(delete_user, (user_id,))
        connection.commit()
        print(Messages.get_message("S4"))

    except mysqlerrors.Error as err:
        print(f"Error: {err}")
        connection.rollback()
    finally:
        cursor.close()


def checkout_DVD():
    DVD_id = input("DVD ID: ")
    user_id = input("User ID: ")
    cursor = connection.cursor()
    try:
        check_dvd = "SELECT stock FROM DVDs WHERE d_id = %s"
        cursor.execute(check_dvd, (DVD_id,))
        dvd = cursor.fetchone()
        if not dvd:
            print(Messages.get_message("E5", d_id=DVD_id))
            return

        check_user = "SELECT borrow_count FROM Users WHERE u_id = %s"
        cursor.execute(check_user, (user_id,))
        user = cursor.fetchone()
        if not user:
            print(Messages.get_message("E7", u_id=user_id))
            return

        if dvd[0] == 0:
            print(Messages.get_message("E9"))
            return

        check_borrowed_same_dvd = """
            SELECT record_id 
            FROM BorrowRecords 
            WHERE u_id = %s AND d_id = %s AND status = 'borrowed'
        """
        cursor.execute(check_borrowed_same_dvd, (user_id, DVD_id))
        if cursor.fetchone() is not None:
            print(Messages.get_message("E15"))
            return

        if user[0] >= 3:
            print(Messages.get_message("E10", u_id=user_id))
            return

        borrow_dvd = "INSERT INTO BorrowRecords (d_id, u_id) VALUES (%s, %s)"
        cursor.execute(borrow_dvd, (DVD_id, user_id))

        update_stock = "UPDATE DVDs SET stock = stock - 1 WHERE d_id = %s"
        cursor.execute(update_stock, (DVD_id,))

        update_borrow_count = (
            "UPDATE Users SET borrow_count = borrow_count + 1 WHERE u_id = %s"
        )
        cursor.execute(update_borrow_count, (user_id,))

        connection.commit()
        print(Messages.get_message("S6"))

    except mysqlerrors.Error as err:
        print(f"Error: {err}")
        connection.rollback()
    finally:
        cursor.close()


def return_and_rate_DVD():
    DVD_id = input("DVD ID: ")
    user_id = input("User ID: ")
    rating = input("Ratings (1~5): ")
    try:
        cursor = connection.cursor()

        check_dvd = "SELECT * FROM DVDs WHERE d_id = %s"
        cursor.execute(check_dvd, (DVD_id,))
        dvd = cursor.fetchone()
        if not dvd:
            print(Messages.get_message("E5", d_id=DVD_id))
            return

        check_user = "SELECT * FROM Users WHERE u_id = %s"
        cursor.execute(check_user, (user_id,))
        user = cursor.fetchone()
        if not user:
            print(Messages.get_message("E7", u_id=user_id))
            return

        if not rating.isdigit() or not (1 <= int(rating) <= 5):
            print(Messages.get_message("E11"))
            return

        check_borrowed = """
            SELECT record_id 
            FROM BorrowRecords 
            WHERE u_id = %s AND d_id = %s AND status = 'borrowed'
        """
        cursor.execute(check_borrowed, (user_id, DVD_id))
        borrow_record = cursor.fetchone()
        if not borrow_record:
            print(Messages.get_message("E12"))
            return

        update_borrow_record = """
            UPDATE BorrowRecords 
            SET status = 'returned', rating = %s 
            WHERE record_id = %s
        """
        cursor.execute(update_borrow_record, (rating, borrow_record[0]))

        update_stock = "UPDATE DVDs SET stock = stock + 1 WHERE d_id = %s"
        cursor.execute(update_stock, (DVD_id,))

        update_borrow_count = (
            "UPDATE Users SET borrow_count = borrow_count - 1 WHERE u_id = %s"
        )

        cursor.execute(update_borrow_count, (user_id,))

        connection.commit()
        print(Messages.get_message("S7"))

    except mysqlerrors.Error as err:
        print(f"Error: {err}")
        connection.rollback()
    finally:
        cursor.close()


def print_borrowing_status_for_user():
    user_id = input("User ID: ")

    cursor = connection.cursor(dictionary=True)
    try:
        check_user = "SELECT u_id FROM Users WHERE u_id = %s"
        cursor.execute(check_user, (user_id,))
        if not cursor.fetchone():
            print(Messages.get_message("E7", u_id=user_id))
            return

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
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        if not results:
            results = []
        print_records(results, headers)
    except mysqlerrors.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


def search_DVD():
    query = input("Query: ")
    # YOUR CODE GOES HERE
    # print msg
    cursor = connection.cursor(dictionary=True)
    search_query = f"%{query}%"
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
    try:
        cursor.execute(sql, (search_query,))
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        if not results:
            print(Messages.get_message("E16"))
            return
        print_records(results, headers)
    except mysqlerrors.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


def search_Director():
    query = input("Query: ")
    cursor = connection.cursor(dictionary=True)
    search_query = f"%{query}%"
    sql = """
        SELECT 
            DVDs.director AS 'director', 
            AVG(BorrowRecords.rating) AS 'director.rating', 
            COUNT(BorrowRecords.record_id) AS 'cumul_rent_cnt', 
            GROUP_CONCAT(DISTINCT DVDs.title ORDER BY DVDs.title ASC) AS 'all_movies'
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
    try:
        cursor.execute(sql, (search_query,))
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        if not results:
            print(Messages.get_message("E16"))
            return
        print_records(results, headers)
    except mysqlerrors.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = input("User ID: ")
    # YOUR CODE GOES HERE
    # print msg
    cursor = connection.cursor(dictionary=True)
    try:
        # Check if user exists
        check_user = "SELECT u_id FROM Users WHERE u_id = %s"
        cursor.execute(check_user, (user_id,))
        if not cursor.fetchone():
            print(Messages.get_message("E7", u_id=user_id))
            return

        query_highest_rated = """
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
        cursor.execute(query_highest_rated, (user_id,))
        highest_rated_dvd = cursor.fetchone()

        # Query to get the most borrowed DVD
        query_most_borrowed = """
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
        cursor.execute(query_most_borrowed, (user_id,))
        most_borrowed_dvd = cursor.fetchone()

        # Print the results
        print_records_recommend(
            [highest_rated_dvd],
            ["id", "title", "director", "avg_rating", "cumul_rent_cnt", "quantity"],
            "Rating-based",
        )
        print_records_recommend(
            [most_borrowed_dvd],
            ["id", "title", "director", "avg_rating", "cumul_rent_cnt", "quantity"],
            "Popularity-based",
        )

    except mysqlerrors.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
    pass


def recommend_user_based():
    user_id = input("User ID: ")
    # YOUR CODE GOES HER
    # print msg
    # 안해
    pass


def main():
    while True:
        print("============================================================")
        print("1. initialize database")
        print("2. print all DVDs")
        print("3. print all users")
        print("4. insert a new DVD")
        print("5. remove a DVD")
        print("6. insert a new user")
        print("7. remove a user")
        print("8. check out a DVD")
        print("9. return and rate a DVD")
        print("10. print borrowing status of a user")
        print("11. search DVDs")
        print("12. search directors")
        print("13. recommend a DVD for a user using popularity-based method")
        print("14. recommend a DVD for a user using user-based collaborative filtering")
        print("15. exit")
        print("16. reset database")
        print("============================================================")
        menu = int(input("Select your action: "))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_DVDs()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_DVD()
        elif menu == 5:
            remove_DVD()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            checkout_DVD()
        elif menu == 9:
            return_and_rate_DVD()
        elif menu == 10:
            print_borrowing_status_for_user()
        elif menu == 11:
            search_DVD()
        elif menu == 12:
            search_Director()
        elif menu == 13:
            recommend_popularity()
        elif menu == 14:
            recommend_user_based()
        elif menu == 15:
            print("Bye!")
            break
        elif menu == 16:
            reset()
        else:
            print("Invalid action")


if __name__ == "__main__":
    main()
