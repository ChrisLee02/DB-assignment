from lark import Lark, LarkError
from database import Database
from parser import MyTransformer
from messages import MessageHandler, MessageKeys
import os


def run_tests():
    # SQL 파서 로드
    with open("grammar.lark") as file:
        sql_parser = Lark(file.read(), start="command", lexer="basic")

    # Database 및 Transformer 인스턴스 생성

    create_table_test_queries = [
        # 1. Valid table creation - students table
        "CREATE TABLE students (student_id INT NOT NULL, student_name CHAR(20), student_age INT, PRIMARY KEY (student_id));",
        "SHOW TABLES;",
        "DESCRIBE students;",
        # 2. Valid table creation - departments table (for foreign key tests)
        "CREATE TABLE departments (tmp int, dept_id INT NOT NULL, dept_name CHAR(20), PRIMARY KEY (dept_id));",
        "SHOW TABLES;",
        "DESCRIBE departments;",
        # 3. Valid table creation - employees table (needed for foreign key tests)
        "CREATE TABLE employees (emp_id INT NOT NULL, emp_name CHAR(20), PRIMARY KEY (emp_id));",
        "SHOW TABLES;",
        "DESCRIBE employees;",
        # 4. Valid table creation - projects table (needed for foreign key tests)
        "CREATE TABLE projects (proj_id INT NOT NULL, proj_name CHAR(20), PRIMARY KEY (proj_id));",
        "SHOW TABLES;",
        "DESCRIBE projects;",
        # 5. Column name duplication error - DuplicateColumnDefError
        "CREATE TABLE duplicate_columns (id INT, id CHAR(10), PRIMARY KEY (id));",
        # 6. Multiple primary key definitions error - DuplicatePrimaryKeyDefError
        "CREATE TABLE multiple_pks (id INT, name CHAR(10), PRIMARY KEY (id), PRIMARY KEY (name));",
        # 7. CHAR length less than 1 error - CharLengthError
        "CREATE TABLE invalid_char_length (id INT, name CHAR(0));",
        # 8. Non-existent column as primary key - PrimaryKeyColumnDefError
        "CREATE TABLE non_existing_pk (id INT, name CHAR(10), PRIMARY KEY (age));",
        # 9. Non-existent column as foreign key - ForeignKeyColumnDefError
        "CREATE TABLE foreign_key_no_column (id INT, dept_id INT, FOREIGN KEY (non_existing_column) REFERENCES departments(dept_id));",
        # 10. Foreign key references a non-existing table - ReferenceExistenceError
        "CREATE TABLE foreign_key_no_table (id INT, dept_id INT, FOREIGN KEY (dept_id) REFERENCES nonexistent_table(id));",
        # 11. Foreign key and referenced column have different types - ReferenceTypeError
        "CREATE TABLE foreign_key_wrong_type (id INT, dept_id CHAR(10), FOREIGN KEY (dept_id) REFERENCES departments(dept_id));",
        # 12. Foreign key references a non-primary key column - ReferenceNonPrimaryKeyError
        "CREATE TABLE foreign_key_non_primary (id INT, dept_id INT, FOREIGN KEY (dept_id) REFERENCES departments(tmp));",
        # 13. Foreign key references only part of a composite primary key - ReferenceNonPrimaryKeyError
        "CREATE TABLE partial_composite_pk (id_one INT, id_two INT, description CHAR(20), PRIMARY KEY (id_one, id_two));",
        "CREATE TABLE ref_partial_composite (fk_id_one INT, FOREIGN KEY (fk_id_one) REFERENCES partial_composite_pk(id_one));",
        # 14. Attempt to create a table with an existing name - TableExistenceError
        "CREATE TABLE students (student_id INT NOT NULL, student_name CHAR(20), student_age INT, PRIMARY KEY (student_id));",
        # 15. Foreign key correctly references composite primary key
        "CREATE TABLE complex_table (id_one INT NOT NULL, id_two INT NOT NULL, description CHAR(20), PRIMARY KEY (id_one, id_two));",
        "CREATE TABLE ref_complex_table (fk_id_one INT, fk_id_two INT, primary key(fk_id_one, fk_id_two), FOREIGN KEY (fk_id_one, fk_id_two) REFERENCES complex_table(id_one, id_two));",
        "SHOW TABLES;",
        "DESCRIBE complex_table;",
        "DESCRIBE ref_complex_table;",
        # 16. DROP TABLE success - DropSuccess
        "CREATE TABLE temp_table (id INT, PRIMARY KEY (id));",
        "SHOW TABLES;",
        "DROP TABLE temp_table;",
        "SHOW TABLES;",
        # 17. Non-existent table drop - NoSuchTable
        "DROP TABLE nonexistent_table;",
        # 18. Drop table that is referenced by another table - DropReferencedTableError
        "CREATE TABLE parent_table (id INT, PRIMARY KEY (id));",
        "CREATE TABLE child_table (child_id INT, FOREIGN KEY (child_id) REFERENCES parent_table(id));",
        "DROP TABLE parent_table;",
        # 19. Insertion into non-existent table - NoSuchTable
        "INSERT INTO nonexistent_table (id, name) VALUES (1, 'Test');",
        # 20. Select from non-existent table - SelectTableExistenceError
        "SELECT * FROM nonexistent_table;",
    ]

    show_and_desc_table = [
        """CREATE TABLE students (
            student_id INt,
            name CHAR(50),
            age INt,
            major CHAR(30),
            PRIMARY KEY (student_id)
        );""",
        "SHOW TABLES;",
        "DESCRIBE students;",
        "desc students;",
        "explain students;",
    ]

    insert_into_table = [
        "CREATE TABLE a (a int, b int, PRIMARY KEY (a));",
        "INSERT INTO a(b, a) VALUES(1, 2);",
        "SELECT * FROM a;",
    ]

    select_table = [
        """CREATE TABLE students (
            student_id INt,
            name CHAR(50),
            age INt,
            major CHAR(30),
            PRIMARY KEY (student_id)
        );""",
        "SELECT * FROM students;",
        """
        INSERT INTO students (student_id, name, age, major) VALUES (1, 'Alice', 20, 'Computer Science');
        INSERT INTO students (student_id, name, age, major) VALUES (2, 'Bob', 22, 'Mathematics');
        INSERT INTO students (student_id, name, age, major) VALUES (3, 'Charlie', 21, 'Physics');
        """,
        "SELECT * FROM students;",
    ]
    # Scenario 1: FK Constraint Drop Test
    fk_constraint_drop_test = [
        "CREATE TABLE parent_table (id INT, PRIMARY KEY (id));",
        "CREATE TABLE child_table (child_id INT, FOREIGN KEY (child_id) REFERENCES parent_table(id));",
        "DROP TABLE parent_table;",  # This should fail due to FK constraint
        "DROP TABLE child_table;",  # Drop the child table first
        "DROP TABLE parent_table;",  # Retry dropping the parent table (should succeed)
        "SHOW TABLES;",  # Verify the table is dropped
    ]

    # Scenario 2: Table Already Exists Error Test
    table_exists_error_test = [
        "CREATE TABLE existing_table (id INT, PRIMARY KEY (id));",
        "CREATE TABLE existing_table (id INT, PRIMARY KEY (id));",  # This should fail (TableExistenceError)
        "DROP TABLE existing_table;",  # Drop the existing table
        "CREATE TABLE existing_table (id INT, PRIMARY KEY (id));",  # Retry creating the table (should succeed)
        "SHOW TABLES;",  # Verify the table is created again
        "DESCRIBE existing_table;",  # Verify the structure
    ]

    # Scenario 3: Nullable Column Test
    nullable_column_test = [
        "CREATE TABLE nullable_table (id INT, name CHAR(20), PRIMARY KEY (id));",
        "INSERT INTO nullable_table (id, name) VALUES (1, NULL);",  # Insert with NULL value
        "INSERT INTO nullable_table (id, name) VALUES (1, null);",  # Insert with NULL value
        "INSERT INTO nullable_table (id, name) VALUES (1, NuLl);",  # Insert with NULL value
        "INSERT INTO nullable_table (id, name) VALUES (2, 'TestName');",  # Insert without NULL value
        "SELECT * FROM nullable_table;",  # Verify both rows are correctly inserted
    ]

    # 테스트 케이스 리스트 업데이트
    test_cases = [
        create_table_test_queries,
        show_and_desc_table,
        insert_into_table,
        select_table,
        fk_constraint_drop_test,
        table_exists_error_test,
        nullable_column_test,
    ]

    for test_queries in test_cases:
        for idx, query in enumerate(test_queries):
            database = Database()
            transformer = MyTransformer(database)
            print("=====================================")
            print(f"{idx + 1}th: Running query: {query}")
            try:
                parsedQuery = sql_parser.parse(query)
                transformer.transform(parsedQuery)
            except LarkError as e:
                print(e)
                MessageHandler.print_error(MessageKeys.SYNTAX_ERROR)
            print("=====================================")

        database.close()
        os.remove("myDB.db")  # 파일 삭제
        print("Database reset: DB file removed.")


run_tests()
