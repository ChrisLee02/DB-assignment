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
    collation="utf8mb4_0900_as_ci",
)


def initialize_database():
    cursor = connection.cursor()
    try:
        queries.drop_tables(cursor)
        queries.create_tables(cursor)
        queries.initialize_from_csv(cursor)
        connection.commit()
        print("Database successfully initialized")
    except Exception as e:
        print(f"Error during database initialization: {e}")
    finally:
        cursor.close()


def reset():
    cursor = connection.cursor()
    try:
        queries.drop_tables(cursor)
        queries.create_tables(cursor)
        queries.initialize_from_csv(cursor)
        connection.commit()
        print("Database successfully reset")
    except Exception as e:
        print(f"Error during database reset: {e}")
    finally:
        cursor.close()


def print_DVDs():
    cursor = connection.cursor(dictionary=True)

    try:
        results = queries.select_dvds(cursor)
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

    try:
        results = queries.select_users(cursor)
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
    cursor = connection.cursor()

    try:
        if len(title) < 1 or len(title) > 100:
            print(Messages.get_message("E1"))
            return

        if len(director) < 1 or len(director) > 50:
            print(Messages.get_message("E2"))
            return

        queries.insert_dvd(cursor, title, director)
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
    cursor = connection.cursor()

    try:
        if not queries.check_dvd(cursor, DVD_id):
            print(Messages.get_message("E5", d_id=DVD_id))
            return

        if queries.check_borrowed(cursor, DVD_id):
            print(Messages.get_message("E6"))
            return

        queries.delete_dvd(cursor, DVD_id)
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

    try:
        cursor = connection.cursor()
        if len(name) < 1 or len(name) > 50:
            print(Messages.get_message("E4"))
            return

        if not age.isdigit() or int(age) <= 0:
            print(Messages.get_message("E14"))
            return

        age = int(age)

        queries.insert_user(cursor, name, age)
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
    cursor = connection.cursor()

    try:
        if not queries.check_user(cursor, user_id):
            print(Messages.get_message("E7", u_id=user_id))
            return

        if queries.check_borrowed_by_user(cursor, user_id):
            print(Messages.get_message("E8"))
            return

        queries.delete_user(cursor, user_id)
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
        if not queries.check_dvd(cursor, DVD_id):
            print(Messages.get_message("E5", d_id=DVD_id))
            return

        if not queries.check_user(cursor, user_id):
            print(Messages.get_message("E7", u_id=user_id))
            return

        if queries.check_stock(cursor, DVD_id) == 0:
            print(Messages.get_message("E9"))
            return

        if queries.check_if_dvd_borrowed_by_user(cursor, user_id, DVD_id):
            print(Messages.get_message("E15"))
            return

        if queries.check_borrow_count(cursor, user_id) >= 3:
            print(Messages.get_message("E10", u_id=user_id))
            return

        queries.insert_borrow_record(cursor, DVD_id, user_id)
        queries.update_stock_decrement(cursor, DVD_id)
        queries.update_borrow_count_increment(cursor, user_id)

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

        if not queries.check_dvd(cursor, DVD_id):
            print(Messages.get_message("E5", d_id=DVD_id))
            return

        if not queries.check_user(cursor, user_id):
            print(Messages.get_message("E7", u_id=user_id))
            return

        if not rating.isdigit() or not (1 <= int(rating) <= 5):
            print(Messages.get_message("E11"))
            return

        if not queries.check_if_dvd_borrowed_by_user(cursor, user_id, DVD_id):
            print(Messages.get_message("E12"))
            return

        queries.update_borrow_record(cursor, rating, user_id, DVD_id)

        queries.update_stock_increment(cursor, DVD_id)
        queries.update_borrow_count_decrement(cursor, user_id)

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
        if not queries.check_user(cursor, user_id):
            print(Messages.get_message("E7", u_id=user_id))
            return

        results = queries.select_borrowed_dvds_by_user(cursor, user_id)
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
    cursor = connection.cursor(dictionary=True)
    search_query = f"%{query}%"

    try:
        results = queries.search_dvd_by_title(cursor, search_query)
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

    try:
        results = queries.search_director_query(cursor, search_query)
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
    user_id = input("User ID: ")
    cursor = connection.cursor(dictionary=True)
    try:
        if not queries.check_user(cursor, user_id):
            print(Messages.get_message("E7", u_id=user_id))
            return

        highest_rated_dvd = queries.get_highest_rated_dvd(cursor, user_id)
        most_borrowed_dvd = queries.get_most_borrowed_dvd(cursor, user_id)

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
