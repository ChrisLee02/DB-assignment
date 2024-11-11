from lark import Lark, LarkError
from database import Database
from parser import MyTransformer
from messages import MessageHandler, MessageKeys
import test_case
import os

# 추가 테스트 케이스
additional_test_cases = [
    # 테스트 케이스 10: 3개 테이블 생성 후 INSERT 및 기본 SELECT
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE lectures (
            id INT NOT NULL,
            name CHAR(20),
            capacity INT,
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            apply_date DATE,
            PRIMARY KEY (s_id, l_id),
            FOREIGN KEY (s_id) REFERENCES students(id),
            FOREIGN KEY (l_id) REFERENCES lectures(id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S002', 'Bob');
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (101, 'Math', 50);
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (102, 'Physics', 30);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S001', 101, 2023-01-15);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S002', 102, 2023-02-20);
        """,
        """
        SELECT * FROM students;
        """,
        """
        SELECT * FROM lectures;
        """,
        """
        SELECT * FROM apply;
        """,
    ],
    # 테스트 케이스 11: JOIN - 모든 테이블 조인 후 필터링 및 정렬
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE lectures (
            id INT NOT NULL,
            name CHAR(20),
            capacity INT,
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            apply_date DATE,
            PRIMARY KEY (s_id, l_id),
            FOREIGN KEY (s_id) REFERENCES students(id),
            FOREIGN KEY (l_id) REFERENCES lectures(id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S002', 'Bob');
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S003', 'Charlie');
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (101, 'Math', 50);
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (102, 'Physics', 30);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S001', 101, 2023-01-15);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S002', 102, 2023-02-20);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S003', 101, 2023-03-10);
        """,
        """
        SELECT students.name, lectures.name,lectures.capacity, apply.apply_date
        FROM students
        JOIN apply ON students.id = apply.s_id
        JOIN lectures ON apply.l_id = lectures.id
        WHERE lectures.capacity > 30
        ORDER BY apply_date DESC;
        """,
    ],
    # 테스트 케이스 12: WHERE 및 ORDER BY - 여러 조건 필터링
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE lectures (
            id INT NOT NULL,
            name CHAR(20),
            capacity INT,
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            apply_date DATE,
            PRIMARY KEY (s_id, l_id),
            FOREIGN KEY (s_id) REFERENCES students(id),
            FOREIGN KEY (l_id) REFERENCES lectures(id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S002', 'Bob');
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (101, 'Math', 50);
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (102, 'Physics', 30);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S001', 101, 2023-01-15);
        """,
        """
        INSERT INTO apply (s_id, l_id, apply_date)
        VALUES ('S002', 102, 2023-02-20);
        """,
        """
        SELECT students.name, apply.apply_date
        FROM students
        JOIN apply ON students.id = apply.s_id
        WHERE apply.apply_date > 2023-01-01
        ORDER BY name ASC;
        """,
    ],
    # 테스트 케이스 13: JOIN 실패 - 존재하지 않는 컬럼
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE lectures (
            id INT NOT NULL,
            name CHAR(20),
            capacity INT,
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            apply_date DATE,
            PRIMARY KEY (s_id, l_id),
            FOREIGN KEY (s_id) REFERENCES students(id),
            FOREIGN KEY (l_id) REFERENCES lectures(id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        INSERT INTO lectures (id, name, capacity)
        VALUES (101, 'Math', 50);
        """,
        """
        SELECT students.name, lectures.name
        FROM students
        JOIN apply ON students.id = apply.nonexistent_column;
        """,
    ],
]


test_cases = [
    # 테스트 케이스 1: 정상 INSERT 후 SELECT로 검증
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        SELECT * FROM students;
        """,
    ],
    # 테스트 케이스 2: 존재하지 않는 테이블에 INSERT
    [
        """
        INSERT INTO nonexistent_table (id, name)
        VALUES ('S002', 'Bob');
        """
    ],
    # 테스트 케이스 3: INSERT 컬럼 개수 불일치
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        INSERT INTO students (id)
        VALUES ('S003', 'Charlie');
        """,
    ],
    # 테스트 케이스 4: NOT NULL 제약 조건 위반
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES (NULL, 'Daisy');
        """,
    ],
    # 테스트 케이스 5: DELETE 후 SELECT로 검증
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        DELETE FROM students WHERE id = 'S001';
        """,
        """
        SELECT * FROM students;
        """,
    ],
    # 테스트 케이스 6: DELETE 실패 - 존재하지 않는 테이블
    [
        """
        DELETE FROM nonexistent_table WHERE id = 'S001';
        """
    ],
    # 테스트 케이스 7: JOIN - 정상적인 두 테이블 조인
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            PRIMARY KEY (s_id, l_id)
        );
        """,
        """
        INSERT INTO students (id, name)
        VALUES ('S001', 'Alice');
        """,
        """
        INSERT INTO apply (s_id, l_id)
        VALUES ('S001', 101);
        """,
        """
        SELECT students.id, students.name, apply.l_id
        FROM students
        JOIN apply ON students.id = apply.s_id;
        """,
    ],
    # 테스트 케이스 8: JOIN 실패 - 존재하지 않는 컬럼
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            PRIMARY KEY (s_id, l_id)
        );
        """,
        """
        SELECT students.id, apply.l_id
        FROM students
        JOIN apply ON students.nonexistent_column = apply.s_id;
        """,
    ],
    # 테스트 케이스 9: JOIN 실패 - 타입 불일치
    [
        """
        CREATE TABLE students (
            id CHAR(10) NOT NULL,
            name CHAR(20),
            PRIMARY KEY (id)
        );
        """,
        """
        CREATE TABLE apply (
            s_id CHAR(10) NOT NULL,
            l_id INT NOT NULL,
            PRIMARY KEY (s_id, l_id)
        );
        """,
        """
        SELECT students.id, apply.l_id
        FROM students
        JOIN apply ON students.id = apply.l_id;
        """,
    ],
]


def run_tests():
    # SQL 파서 로드
    with open("grammar.lark") as file:
        sql_parser = Lark(file.read(), start="command", lexer="basic")

    # Database 및 Transformer 인스턴스 생성

    for test_queries in additional_test_cases:
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
    # for idx, query in enumerate(test_case.sql_test_cases):
    #     database = Database()
    #     transformer = MyTransformer(database)
    #     print("=====================================")
    #     print(f"{idx + 1}th: Running query: {query}")
    #     try:
    #         parsedQuery = sql_parser.parse(query)
    #         transformer.transform(parsedQuery)
    #     except LarkError as e:
    #         print(e)
    #         MessageHandler.print_error(MessageKeys.SYNTAX_ERROR)
    #     print("=====================================")
    # database.close()
    # os.remove("myDB.db")  # 파일 삭제
    # print("Database reset: DB file removed.")


run_tests()
