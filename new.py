from lark import Lark, LarkError, Transformer
from berkeleydb import db
import pickle


class MessageHandler:
    @staticmethod
    def print_error(message_type, **kwargs):
        messages = {
            "SyntaxError": "Syntax error",
            "DuplicateColumnDefError": "Create table has failed: column definition is duplicated",
            "DuplicatePrimaryKeyDefError": "Create table has failed: primary key definition is duplicated",
            "ReferenceTypeError": "Create table has failed: foreign key references wrong type",
            "ReferenceNonPrimaryKeyError": "Create table has failed: foreign key references non primary key column",
            "ReferenceExistenceError": "Create table has failed: foreign key references non existing table or column",
            "PrimaryKeyColumnDefError": f"Create table has failed: cannot define non-existing column '{kwargs.get('col_name')}' as primary key",
            "ForeignKeyCol1umnDefError": f"Create table has failed: cannot define non-existing column '{kwargs.get('col_name')}' as foreign key",
            "TableExistenceError": "Create table has failed: table with the same name already exists",
            "CharLengthError": "Char length should be over 0",
            "NoSuchTable": f"{kwargs.get('command_name')} has failed: no such table",
            "DropReferencedTableError": f"Drop table has failed: '{kwargs.get('table_name')}' is referenced by another table",
            "SelectTableExistenceError": f"Select has failed: '{kwargs.get('table_name')}' does not exist",
        }
        print(messages.get(message_type, "Unknown error"))

    @staticmethod
    def print_success(message_type, **kwargs):
        messages = {
            "CreateTableSuccess": f"{kwargs.get('table_name')} table is created",
            "DropSuccess": f"{kwargs.get('table_name')} table is dropped",
            "InsertResult": "The row is inserted",
        }
        print(messages.get(message_type, "Unknown message"))

class ErrorHandler:
    @staticmethod
    def handle_no_such_table(database, table_name, command_name):
        if not database.db.get(f"table_schema:{table_name}".encode()):
            MessageHandler.print_error("NoSuchTable", command_name=command_name)
            return True
        return False
    

class ForeignKeyMetadata:
    def __init__(
        self,
        child_table_name,
        child_column_name_list,
        referenced_parent_table,
        referenced_parent_column_list,
        on_delete_action="no action",
    ):
        self.child_table_name = child_table_name.lower()
        self.child_column_name_list = child_column_name_list
        self.referenced_parent_table = referenced_parent_table.lower()
        self.referenced_parent_column_list = referenced_parent_column_list
        self.on_delete_action = on_delete_action.lower()

    def describe(self):
        return (
            f"FK: {self.child_table_name}.{self.child_column_name_list} -> {self.referenced_parent_table}.{self.referenced_parent_column_list} "
            f"(ON DELETE {self.on_delete_action})"
        )


class TableMetadata:
    def __init__(
        self,
        table_name,
        columns,
        pk_constraints: list[list] = None,
        fk_constraints=None,
    ):
        self.table_name = table_name.lower()  # 테이블 이름을 소문자로 변환해 저장
        self.columns = {
            col["name"].lower(): col for col in columns
        }  # 컬럼 정보도 소문자로 저장
        self.pk_sets = (
            pk_constraints[0]["key_list"] if pk_constraints else []
        )  # 기본 키 정보 초기화
        self.fk_constraints = fk_constraints or []  # 외래 키 정보 초기화

    def get_column(self, column_name):
        return self.columns.get(column_name.lower())

    def describe(self):
        header = "column_name     | type       | null | key"
        separator = "-----------------------------------------------------------------"
        rows = [
            f"{col['name']:<15} | {col['type']:<10} | {'N' if col.get('not_null', False) else 'Y':<4} | {'PRI/FOR' if col['name'] in self.pk_sets and any(col['name'] in fk['key_list'] for fk in self.fk_constraints) else ('PRI' if col['name'] in self.pk_sets else ('FOR' if any(col['name'] in fk['key_list'] for fk in self.fk_constraints) else '')):<5}"
            for col in self.columns.values()
        ]
        return "\n".join(
            [header, separator]
            + rows
            + [
                separator,
                (
                    f"{len(rows)} row in set"
                    if len(rows) == 1
                    else f"{len(rows)} rows in set"
                ),
            ]
        )





# DuplicateColumnDefError
# DuplicatePrimaryKeyDefError
# ReferenceTypeError
# ReferenceNonPrimaryKeyError
# ReferenceExistenceError
# PrimaryKeyColumnDefError
# ForeignKeyColumnDefError
# TableExistenceError
# CharLengthError
class Database:
    def __init__(self):
        self.db = db.DB()
        self.db.open("myDB.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)

    # todo: 테이블 접근하는 함수 추상화.

    def get_table_metadata(self, table_name):
        table_meta_data_serialized = self.db.get(f"table_schema:{table_name}".encode())
        if not table_meta_data_serialized:
            return None
        return pickle.loads(table_meta_data_serialized)


    def create_table(
        self,
        table_name: str,
        columns: list,
        pk_constraints: list[list] | None = None,
        fk_constraints=None,
    ):
        table_name = table_name.lower()

        # Error handling before trying to create the table

        # Handling duplicate column definitions - DuplicateColumnDefError
        column_names = [col["name"].lower() for col in columns]
        if len(column_names) != len(set(column_names)):
            MessageHandler.print_error("DuplicateColumnDefError")
            return

        # Handling multiple primary key definitions - DuplicatePrimaryKeyDefError
        if pk_constraints != None and len(pk_constraints) > 1:
            MessageHandler.print_error("DuplicatePrimaryKeyDefError")
            return

        # Handling table with the same name already exists - TableExistenceError
        if self.db.get(f"table_schema:{table_name}".encode()):
            MessageHandler.print_error("TableExistenceError")
            return

        for col in columns:
            # Handling char type length less than 1 - CharLengthError
            if col["type"].startswith("char"):
                char_length = int(col["type"][5:-1])
                if char_length < 1:
                    MessageHandler.print_error("CharLengthError")
                    return

        for pk in pk_constraints or []:
            # Handling non-existing column defined as primary key - PrimaryKeyColumnDefError
            for col_name in pk["key_list"]:
                if col_name.lower() not in column_names:
                    MessageHandler.print_error(
                        "PrimaryKeyColumnDefError", col_name=col_name
                    )
                    return

        for fk in fk_constraints or []:
            # Handling non-existing column defined as foreign key - ForeignKeyColumnDefError
            for col_name in fk["key_list"]:
                if col_name.lower() not in column_names:
                    MessageHandler.print_error(
                        "ForeignKeyColumnDefError", col_name=col_name
                    )
                    return

        # Reference existence and type check for foreign keys
        for fk in fk_constraints:
            for key_idx, fk_col_name in enumerate(fk["key_list"]):
                ref_table_name = fk["ref_table"].lower()
                ref_table_data = self.db.get(f"table_schema:{ref_table_name}".encode())
                # Handling foreign key references non-existing table - ReferenceExistenceError
                if not ref_table_data:
                    MessageHandler.print_error("ReferenceExistenceError")
                    return
                ref_table_metadata = pickle.loads(ref_table_data)
                ref_col_name = fk["other_key_list"][key_idx]

                fk_col = [
                    col for col in columns if col["name"].lower() == fk_col_name.lower()
                ][0]

                ref_col_name = fk["other_key_list"][key_idx]
                ref_col = ref_table_metadata.get_column(ref_col_name)

                if not ref_col:
                    # Handling foreign key references non-existing column - ReferenceExistenceError
                    MessageHandler.print_error("ReferenceExistenceError")
                    return
                # Type check
                if fk_col["type"] != ref_col["type"]:
                    # Handling foreign key references wrong type - ReferenceTypeError
                    MessageHandler.print_error("ReferenceTypeError")
                    return

        # Check if the referenced column is a primary key for the entire key list
        for fk in fk_constraints or []:
            # Handling foreign key references non-primary key column - ReferenceNonPrimaryKeyError
            if set(fk["other_key_list"]) != set(ref_table_metadata.pk_sets):
                MessageHandler.print_error("ReferenceNonPrimaryKeyError")
                return

        table_metadata = TableMetadata(
            table_name, columns, pk_constraints, fk_constraints
        )
        # Update primary key columns to be not null
        for pk in pk_constraints or []:
            for col_name in pk["key_list"]:
                if col_name.lower() in table_metadata.columns:
                    table_metadata.columns[col_name.lower()]["not_null"] = True

        fk_metadata_list_data = self.db.get("foreign_key_metadata".encode())
        fk_metadata_list = (
            pickle.loads(fk_metadata_list_data) if fk_metadata_list_data else []
        )
        for fk in fk_constraints or []:
            print(fk)
            fk_metadata_list.append(
                ForeignKeyMetadata(
                    table_name, fk["key_list"], fk["ref_table"], fk["other_key_list"]
                )
            )

        self.db.put(f"table_schema:{table_name}".encode(), pickle.dumps(table_metadata))
        self.db.put(f"table_data:{table_name}".encode(), pickle.dumps([]))
        self.db.put("foreign_key_metadata".encode(), pickle.dumps(fk_metadata_list))

        MessageHandler.print_success("CreateTableSuccess", table_name=table_name)

    def print_fk_metadata(self):
        fk_metadata_list_data = self.db.get("foreign_key_metadata".encode())
        fk_metadata_list = (
            pickle.loads(fk_metadata_list_data) if fk_metadata_list_data else []
        )
        [print(i.describe()) for i in fk_metadata_list]

    def drop_table(self, table_name: str):
        table_name = table_name.lower()

        if(ErrorHandler.handle_no_such_table(self, table_name, "Drop")):
            return

        fk_metadata_list_data = self.db.get("foreign_key_metadata".encode())
        fk_metadata_list = (
            pickle.loads(fk_metadata_list_data) if fk_metadata_list_data else []
        )
        filtered_by_ref_table_fk_metadata_list = list(
            filter(lambda x: x.referenced_parent_table == table_name, fk_metadata_list)
        )
        if len(filtered_by_ref_table_fk_metadata_list) > 0:
            MessageHandler.print_error(
                "DropReferencedTableError", table_name=table_name
            )
            return

        deleted_fk_metadata_list = list(
            filter(lambda x: not x.child_table_name == table_name, fk_metadata_list)
        )

        self.db.put(
            "foreign_key_metadata".encode(), pickle.dumps(deleted_fk_metadata_list)
        )
        self.db.delete(f"table_schema:{table_name}".encode())
        self.db.delete(f"table_data:{table_name}".encode())

        MessageHandler.print_success("DropSuccess", table_name=table_name)

    def describe_table(self, table_name: str, command_name):
        table_name = table_name.lower()
        if ErrorHandler.handle_no_such_table(self, table_name, command_name):
            return

        table_data = self.db.get(f"table_schema:{table_name}".encode())
        table_metadata: TableMetadata = pickle.loads(table_data)
        print(table_metadata.describe())

    def show_tables(self):
        cursor = self.db.cursor()
        header = "----------------------"
        rows = []
        record = cursor.first()
        while record:
            key, _ = record
            if key.decode().startswith("table_schema:"):
                table_name = key.decode().split("table_schema:")[1]
                rows.append(table_name)
            record = cursor.next()
        cursor.close()

        if not rows:
            print("No tables found.")
        else:
            print(header)
            print("\n".join(rows))
            print(header)
            print(
                f"{len(rows)} row in set"
                if len(rows) == 1
                else f"{len(rows)} rows in set"
            )

    def insert_into_table(
        self, table_name: str, values: list, column_sequence: list = None
    ):
        table_name = table_name.lower()
        if(ErrorHandler.handle_no_such_table(self, table_name, "Insert into")):
            return
       
        table_meta_data_serialized = self.db.get(f"table_schema:{table_name}".encode())
        table_meta_data: TableMetadata = pickle.loads(table_meta_data_serialized)

        columns_list = table_meta_data.columns
        rows: list = pickle.loads(self.db.get(f"table_data:{table_name}".encode()))
        # 딕셔너리 내부 순서는 신경쓸 필요가 없다. select 시점에 메타데이터의 순서대로 꺼내올 것.
        row = dict()

        if column_sequence == None:
            column_sequence = [columns_list[i]["name"] for i in columns_list]

        for idx, column_name in enumerate(column_sequence):
            type: str = table_meta_data.get_column(column_name)["type"]
            value = values[idx]
            if type.startswith("char"):
                char_length = int(type[5:-1])
                row[column_name] = value[:char_length]
            else:
                row[column_name] = value

        rows.append(row)
        self.db.put(f"table_data:{table_name}".encode(), pickle.dumps(rows))
        MessageHandler.print_success("InsertResult")

    def select_from_table(self, table_name: str):
        # Placeholder for select implementation
        table_name = table_name.lower()
        table_meta_data_serialized = self.db.get(f"table_schema:{table_name}".encode())
        if not table_meta_data_serialized:
            MessageHandler.print_error(
                "SelectTableExistenceError", table_name=table_name
            )
            return

        table_meta_data: TableMetadata = pickle.loads(table_meta_data_serialized)

        columns_list = table_meta_data.columns
        column_name_list = [columns_list[i]["name"] for i in columns_list]
        rows: list = pickle.loads(self.db.get(f"table_data:{table_name}".encode()))
        # 헤더와 구분선 생성
        header = " | ".join(f"{col_name:<15}" for col_name in column_name_list)
        separator = "-" * len(header)

        # 헤더 출력
        print(header)
        print(separator)

        # 각 행 출력 (열 길이에 맞춰 정렬)
        for row in rows:
            row_values = " | ".join(
                f"{str(row.get(col, 'NULL')):<15}" for col in column_name_list
            )
            print(row_values)

        print(separator)
        print(f"{len(rows)} row in set" if len(rows) == 1 else f"{len(rows)} rows in set")

        return

    def close(self):
        self.db.close()


def get_first_child_by_rule(tree, rule_name):
    for child in tree.children:
        if hasattr(child, "data") and child.data == rule_name:
            return child
    return None


class MyTransformer(Transformer):
    def __init__(self, database: Database):
        self.database = database

    def create_table_query(self, items):
        table_name = items[2].children[0].lower()
        column_list = []
        
        pk_constraints = (
            []
        )  
        # DuplicatePrimaryKeyDefError should be handled inside of database class
        fk_constraints = []
        for table_element in items[3].children[1:-1]:
            if table_element.children[0].data == "column_definition":
                column_definition = get_first_child_by_rule(
                    table_element, "column_definition"
                )
                column_name = get_first_child_by_rule(column_definition, "column_name")
                data_type = get_first_child_by_rule(column_definition, "data_type")
                name = column_name.children[0].lower()
                type = None
                if data_type.children[0].lower() == "char":
                    type = "char" + "(" + data_type.children[2].value + ")"
                else:
                    type = data_type.children[0].lower()

                is_not_null = column_definition.children[2] != None

                column_list.append(
                    {"name": name, "type": type, "null": not is_not_null}
                )

            else:
                key_list = []
                other_key_list = []
                if (
                    table_element.children[0].children[0].data
                    == "primary_key_constraint"
                ):
                    table_constraint_definition = get_first_child_by_rule(
                        table_element, "table_constraint_definition"
                    )
                    primary_key_constraint = get_first_child_by_rule(
                        table_constraint_definition, "primary_key_constraint"
                    )
                    column_name_list = get_first_child_by_rule(
                        primary_key_constraint, "column_name_list"
                    )
                    for i in column_name_list.children[1:-1]:
                        key_list.append(i.children[0].lower())
                    pk_constraints.append({"key_list": key_list})
                else:
                    table_constraint_definition = get_first_child_by_rule(
                        table_element, "table_constraint_definition"
                    )
                    referential_constraint = get_first_child_by_rule(
                        table_constraint_definition, "referential_constraint"
                    )
                    column_name_list = get_first_child_by_rule(
                        referential_constraint, "column_name_list"
                    )
                    other_table = get_first_child_by_rule(
                        referential_constraint, "table_name"
                    )
                    other_column_name_list = referential_constraint.children[5]

                    for i in column_name_list.children[1:-1]:
                        key_list.append(i.children[0].lower())

                    for i in other_column_name_list.children[1:-1]:
                        other_key_list.append(i.children[0].lower())
                    fk_constraints.append(
                        {
                            "key_list": key_list,
                            "other_key_list": other_key_list,
                            "ref_table": other_table.children[0].lower(),
                        }
                    )
        self.database.create_table(
            table_name, column_list, pk_constraints, fk_constraints
        )

    def select_query(self, items):
        from_clause = items[2].children[0]
        table_reference_list = from_clause.children[1]
        # naive implementation for now,
        # should be generallized in 1-3
        referred_table = table_reference_list.children[0]
        referred_table_name_tree = referred_table.children[0]
        
        referred_table_name = referred_table_name_tree.children[0].lower()
        self.database.select_from_table(referred_table_name)

    def drop_table_query(self, items):
        self.database.drop_table(items[2].children[0])

    def show_tables_query(self, items):
        self.database.show_tables()

    def describe_query(self, items):
        self.database.describe_table(items[1].children[0], "Describe")
    
    def explain_query(self, items):
        self.database.describe_table(items[1].children[0], "Explain")

    def desc_query(self, items):
        self.database.describe_table(items[1].children[0], "Desc")


    def insert_query(self, items):

        # for now,just implemented select * from a;

        table_name = items[2].children[0].lower()
        values_list_wrapped = items[5].children[1:-1]
        # handle date as str internally.

        values = [
            (
                int(i.children[0].value)
                if i.children[0].type == "INT"
                else i.children[0].value.strip("'\"")
            )
            for i in values_list_wrapped
        ]

        column_name_list_tree = items[3]
        if column_name_list_tree == None:
            self.database.insert_into_table(table_name, values)
        else:
            column_name_list = column_name_list_tree.children[1:-1]
            column_sequence = []
            for column_name_tree in column_name_list:
                column_name = column_name_tree.children[0].lower()
                column_sequence.append(column_name)
            self.database.insert_into_table(table_name, values, column_sequence)

    

    def exit_command(self, items):
        self.database.close()
        exit()


# Print the prompt in the required format
def print_prompt():
    print("DB_2021-18641>", end=" ")


# Handle multi-line input, returning the joined string
# The input ends when a semicolon is located at the end of a line
def handleInput():
    print_prompt()
    multi_lines: list[str] = []
    while True:
        input_line = input().strip()
        multi_lines.append(input_line)
        if input_line.endswith(";"):
            break
    return " ".join(multi_lines)


# Load sql parser declared in grammar.lark
with open("grammar.lark") as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

# Create an Instance of MyTransformer and Database
database = Database()
transformer = MyTransformer(database)


# End-to-End 테스트 함수
def run_tests():
    create_table_test_queries = [
        # 1. Valid table creation - students table
        "CREATE TABLE students (student_id INT NOT NULL, student_name CHAR(20), student_age INT, PRIMARY KEY (student_id));",
        # 2. Valid table creation - departments table (for foreign key tests)
        "CREATE TABLE departments (tmp int, dept_id INT NOT NULL, dept_name CHAR(20), PRIMARY KEY (dept_id));",
        # 3. Valid table creation - employees table (needed for foreign key tests)
        "CREATE TABLE employees (emp_id INT NOT NULL, emp_name CHAR(20), PRIMARY KEY (emp_id));",
        # 4. Valid table creation - projects table (needed for foreign key tests)
        "CREATE TABLE projects (proj_id INT NOT NULL, proj_name CHAR(20), PRIMARY KEY (proj_id));",
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
        "CREATE TABLE ref_complex_table (fk_id_one INT, fk_id_two INT, FOREIGN KEY (fk_id_one, fk_id_two) REFERENCES complex_table(id_one, id_two));",
        # 16. SHOW TABLES query
        "SHOW TABLES;",
        # 17. DESCRIBE students table
        "DESCRIBE students;",
        # Additional test queries to cover missing cases:
        # 18. Foreign key references a non-existing column in the referenced table - ReferenceExistenceError
        "CREATE TABLE fk_refs_nonexistent_column (id INT, dept_id INT, FOREIGN KEY (dept_id) REFERENCES departments(nonexistent_column));",
        # 19. Foreign key and referenced column are both CHAR but with different lengths - ReferenceTypeError
        "CREATE TABLE fk_char_length_mismatch (id INT, code CHAR(5), FOREIGN KEY (code) REFERENCES departments(dept_name));",
        # 20. Valid table creation without specifying a primary key
        "CREATE TABLE no_primary_key_table (id INT, name CHAR(20));",
        # 21. Case-insensitive table and column names test
        "CREATE TABLE case_insensitive_test (ID INT, Dept_ID INT, FOREIGN KEY (dept_id) REFERENCES DEPARTMENTS(Dept_ID));",
        # 22. Table with multiple foreign keys (should be valid)
        "CREATE TABLE employee_projects (employee_id INT, project_id INT, dept_id INT, FOREIGN KEY (employee_id) REFERENCES employees(emp_id), FOREIGN KEY (project_id) REFERENCES projects(proj_id));",
        # 23. Foreign key column that allows NULL values (should be valid)
        "CREATE TABLE nullable_fk (id INT, dept_id INT, FOREIGN KEY (dept_id) REFERENCES departments(dept_id));",
    ]

    show_and_desc_table = [
        "SHOW TABLES;",
        "DESCRIBE students;",
        "desc students;",
        "explain students;",
    ]

    insert_into_table = [
        # 'create table a (a int, b int);',
        "insert into a(b, a) values(1, 2);"
    ]

    select_table = [
        """CREATE TABLE students (
            student_id INt,
            name CHAR(50),
            age INt,
            major CHAR(30)
        );""",
        
        """
        INSERT INTO students (student_id, name, age, major) VALUES (1, 'Alice', 20, 'Computer Science');
        INSERT INTO students (student_id, name, age, major) VALUES (2, 'Bob', 22, 'Mathematics');
        INSERT INTO students (student_id, name, age, major) VALUES (3, 'Charlie', 21, 'Physics');
        """
        
        "select * from students;",
    ]

    test_queries = select_table

    for idx, query in enumerate(test_queries):
        print(f"{idx}th: Running query: {query}")
        try:
            parsedQuery = sql_parser.parse(query)
            transformer.transform(parsedQuery)
        except LarkError as e:
            print(e)
            MessageHandler.print_error("SyntaxError")


def main_function():
    while True:

        inputQuery: str = handleInput()

        # Remove last element which is empty string due to last semicolon
        splittedQueries = inputQuery.split(";")[:-1]

        # Parse and transform each query
        # Break the loop if a LarkError occurs (syntax error)
        for query in splittedQueries:
            try:
                parsedQuery = sql_parser.parse(query + ";")
                transformer.transform(parsedQuery)
            except LarkError as e:
                MessageHandler.print_error("SyntaxError")
                break


run_tests()

database.print_fk_metadata()

# Main loop
# Commented out for testing purposes

main_function()
