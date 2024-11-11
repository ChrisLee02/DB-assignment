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
    "INSERT INTO nullable_table (name) VALUES ('hi');",  # Insert with NULL value
    "INSERT INTO nullable_table (dltkqf) VALUES ('hi');",  # Insert with NULL value
    "INSERT INTO nullable_table (id, name) VALUES (1, NULL);",  # Insert with NULL value
    "INSERT INTO nullable_table (id, name) VALUES (1, null);",  # Insert with NULL value
    "INSERT INTO nullable_table (id, name) VALUES (1, NuLl);",  # Insert with NULL value
    "INSERT INTO nullable_table (id, name) VALUES (2, 'TestName');",  # Insert without NULL value
    "SELECT * FROM nullable_table;",  # Verify both rows are correctly inserted
]
# delete test
delete_test = [
    "create table a (b int);",
    "insert into a values(1);",
    "select * from a;",
    "delete from a;",
    "select * from a;",
    "delete from a where a.b = 0;",
    "delete from a where a.b is null;",
    "delete from a where 1 = 0;",
    "delete from a where b = 2011-12-19;",
    "delete from a where b is not null;",
    "delete from a where b is not null and b = 1;",
    "delete from a where b = 2 and b = 1;",
]
# Delete Test Cases for Project 1-3
delete_test_GPT = [
    # Creating necessary tables
    "create table students (id char(10) not null, name char(20), primary key (id));",
    "create table lectures (id int not null, name char(20), capacity int, primary key (id));",
    "create table apply (s_id char(10) not null, l_id int not null, apply_date date, primary key (s_id, l_id), foreign key (s_id) references students (id), foreign key (l_id) references lectures (id));",
    # Inserting initial values into the tables
    "insert into students values('S001', 'Alice');",
    "insert into students values('S002', 'Bob');",
    "insert into lectures values(1, 'Math', 30);",
    "insert into lectures values(2, 'Physics', 25);",
    "insert into apply values('S001', 1, '2024-11-01');",
    "insert into apply values('S002', 2, '2024-11-02');",
    # Selecting all records to verify insertion
    "select * from students;",
    "select * from lectures;",
    "select * from apply;",
    # Deleting records and testing various scenarios with simple conditions
    "delete from students where id = 'S001';",  # Delete a specific student
    "select * from students;",  # Verify the deletion
    "delete from lectures where id = 3;",  # Delete a non-existent lecture (no effect)
    "select * from lectures;",  # Verify no changes
    "delete from apply where s_id = 'S002' and l_id = 2;",  # Delete specific apply record with AND condition
    "select * from apply;",  # Verify the deletion
    "delete from students;",  # Delete all students
    "select * from students;",  # Verify all students are deleted
    "delete from lectures where name is not null;",  # Delete all lectures using condition
    "select * from lectures;",  # Verify all lectures are deleted
    # Additional delete scenarios with two simple conditions
    "delete from apply where apply_date = '2024-11-01' and l_id = 1;",  # Delete apply record by date and l_id
    "delete from apply where s_id is not null or l_id = 2;",  # Delete records with OR condition
    "select * from apply;",  # Verify remaining records in apply table
    # Simple condition scenarios
    "delete from apply where apply_date = '2024-11-01';",  # Delete apply record by date
    "delete from apply where s_id is null;",  # Delete where s_id is null (no effect as no null value exists)
    "delete from apply where l_id = 1;",  # Delete apply record with specific l_id
    "select * from apply;",  # Verify remaining records in apply table
    # Additional delete scenarios with parentheses to verify parsing
    "delete from apply where (apply_date = '2024-11-01' and l_id = 1);",  # Delete with AND condition in parentheses
    "delete from apply where (s_id = 'S002' or l_id = 1);",  # Delete with OR condition in parentheses
    "delete from apply where (s_id = 'S001' and (l_id = 1 or apply_date = '2024-11-01'));",  # Nested parentheses
    "select * from apply;",  # Verify remaining records in apply table
]
# Delete Test Cases for Project 1-3
# Delete Test Cases for Project 1-3
# Scenario 1: Basic Delete of Specific Record
delete_test_case_1 = [
    "create table students (id char(10) not null, name char(20), primary key (id));",
    "insert into students values('S001', 'Alice');",
    "insert into students values('S002', 'Bob');",
    "select * from students;",  # Expecting 2 records: S001, S002
    "delete from students where id = 'S001';",  # Delete a specific student
    "delete from students where id = 1;",  # Delete a specific student
    "select * from students;",  # Verify the deletion, expecting 1 record: S002
]
# Scenario 2: Delete Non-existent Record
delete_test_case_2 = [
    "create table lectures (id int not null, name char(20), capacity int, primary key (id));",
    "insert into lectures values(1, 'Math', 30);",
    "insert into lectures values(2, 'Physics', 25);",
    "select * from lectures;",  # Expecting 2 records: 1 (Math), 2 (Physics)
    "delete from lectures where id = 3;",  # Delete a non-existent lecture (no effect)
    "select * from lectures;",  # Verify no changes, expecting 2 records: 1 (Math), 2 (Physics)
]
# Scenario 3: Delete with AND Condition
delete_test_case_3 = [
    "create table apply (s_id char(10) not null, l_id int not null, apply_date date, primary key (s_id, l_id), foreign key (s_id) references students (id), foreign key (l_id) references lectures (id));",
    "insert into apply values('S001', 1, '2024-11-01');",
    "insert into apply values('S002', 2, '2024-11-02');",
    "select * from apply;",  # Expecting 2 records: (S001, 1), (S002, 2)
    "delete from apply where s_id = 'S002' and l_id = 2;",  # Delete specific apply record with AND condition
    "select * from apply;",  # Verify the deletion, expecting 1 record: (S001, 1)
]
# Scenario 4: Delete All Records
delete_test_case_4 = [
    "create table students (id char(10) not null, name char(20), primary key (id));",
    "insert into students values('S001', 'Alice');",
    "insert into students values('S002', 'Bob');",
    "select * from students;",  # Expecting 2 records: S001, S002
    "delete from students;",  # Delete all students
    "select * from students;",  # Verify all students are deleted, expecting 0 records
]
# Scenario 5: Delete with NOT NULL Condition
delete_test_case_5 = [
    "create table lectures (id int not null, name char(20), capacity int, primary key (id));",
    "insert into lectures values(1, 'Math', 30);",
    "insert into lectures values(2, 'Physics', 25);",
    "select * from lectures;",  # Expecting 2 records: 1 (Math), 2 (Physics)
    "delete from lectures where name is not null;",  # Delete all lectures using condition
    "select * from lectures;",  # Verify all lectures are deleted, expecting 0 records
]
# Scenario 6: Delete with Parentheses and Nested Conditions
delete_test_case_6 = [
    "create table apply (s_id char(10) not null, l_id int not null, apply_date date, primary key (s_id, l_id), foreign key (s_id) references students (id), foreign key (l_id) references lectures (id));",
    "insert into apply values('S003', 3, '2024-11-03');",
    "select * from apply;",  # Expecting 1 record: (S003, 3)
    "delete from apply where (apply_date = '2024-11-03' and l_id = 3);",  # Delete with AND condition in parentheses
    "select * from apply;",  # Verify deletion, expecting 0 records
    "insert into apply values('S003', 3, '2024-11-03');",  # Re-insert to test OR condition
    "delete from apply where (s_id = 'S003' or l_id = 1);",  # Delete with OR condition in parentheses
    "select * from apply;",  # Verify deletion, expecting 0 records
    "insert into apply values('S003', 3, '2024-11-03');",  # Re-insert to test nested parentheses
    "delete from apply where (s_id = 'S003' and (l_id = 3 or apply_date = '2024-11-03'));",  # Nested parentheses
    "select * from apply;",  # Verify deletion, expecting 0 records
]
# Combined delete test cases
delete_test_combined = (
    delete_test_case_1
    + delete_test_case_2
    + delete_test_case_3
    + delete_test_case_4
    + delete_test_case_5
    + delete_test_case_6
)
select_test = [
    "select * from tableA;",
    """select colA, colB, tableA.colC from tableA join tableB 
    on tableA.colA = tableB.colB;""",
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
select_test_full = [
    """ select customer_name, borrower.loan_number, amount
        from borrower
        join loan on borrower.loan_number = loan.loan_number
        join loan on borrower.loan_number = loan.loan_number
        where branch_name = 'Perryridge'
        order by customer_name asc;""",
    """select * from loan; """,
]
sql_test_cases = [
    # 1. borrower 테이블 생성
    """
CREATE TABLE borrower (
    customer_name CHAR(50),
    loan_number CHAR(10),
    date_of_borrow DATE
);
""",
    # 2. loan 테이블 생성
    """
CREATE TABLE loan (
    loan_number CHAR(10),
    amount INT,
    branch_name CHAR(50),
    loan_date DATE
);
""",
    # 3. borrower 데이터 삽입 (하나씩)
    """
INSERT INTO borrower (customer_name, loan_number, date_of_borrow)
VALUES ('Alice', 'LN001', 2023-01-15);
""",
    """
INSERT INTO borrower (customer_name, loan_number, date_of_borrow)
VALUES ('Bob', 'LN002', 2023-02-10);
""",
    """
INSERT INTO borrower (customer_name, loan_number, date_of_borrow)
VALUES ('Charlie', 'LN003', 2023-03-05);
""",
    """
INSERT INTO borrower (customer_name, loan_number, date_of_borrow)
VALUES ('Daisy', 'LN004', 2023-01-20);
""",
    # 4. loan 데이터 삽입 (하나씩)
    """
INSERT INTO loan (loan_number, amount, branch_name, loan_date)
VALUES ('LN001', 5000, 'Perryridge', 2023-01-10);
""",
    """
INSERT INTO loan (loan_number, amount, branch_name, loan_date)
VALUES ('LN002', 2000, 'Downtown', 2023-02-15);
""",
    """
INSERT INTO loan (loan_number, amount, branch_name, loan_date)
VALUES ('LN003', 7000, 'Perryridge', 2023-03-01);
""",
    """
INSERT INTO loan (loan_number, amount, branch_name, loan_date)
VALUES ('LN004', 3000, 'Uptown', 2023-01-25);
""",
    # 5. SELECT 쿼리 테스트 (JOIN, WHERE, ORDER BY)
    "desc borrower;",
    "desc loan;",
    """
SELECT customer_name, borrower.loan_number, amount, date_of_borrow
FROM borrower
JOIN loan ON borrower.loan_number = loan.loan_number
WHERE branch_name = 'Perryridge'
ORDER BY customer_name ASC;
""",
]
