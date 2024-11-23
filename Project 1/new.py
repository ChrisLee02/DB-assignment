from lark import Lark, LarkError, Transformer
from berkeleydb import db
import pickle


class MessageKeys:
    SYNTAX_ERROR = "SyntaxError"
    DUPLICATE_COLUMN_DEF_ERROR = "DuplicateColumnDefError"
    DUPLICATE_PRIMARY_KEY_DEF_ERROR = "DuplicatePrimaryKeyDefError"
    REFERENCE_TYPE_ERROR = "ReferenceTypeError"
    REFERENCE_NON_PRIMARY_KEY_ERROR = "ReferenceNonPrimaryKeyError"
    REFERENCE_EXISTENCE_ERROR = "ReferenceExistenceError"
    PRIMARY_KEY_COLUMN_DEF_ERROR = "PrimaryKeyColumnDefError"
    FOREIGN_KEY_COLUMN_DEF_ERROR = "ForeignKeyColumnDefError"
    TABLE_EXISTENCE_ERROR = "TableExistenceError"
    CHAR_LENGTH_ERROR = "CharLengthError"
    NO_SUCH_TABLE = "NoSuchTable"
    DROP_REFERENCED_TABLE_ERROR = "DropReferencedTableError"
    SELECT_TABLE_EXISTENCE_ERROR = "SelectTableExistenceError"
    CREATE_TABLE_SUCCESS = "CreateTableSuccess"
    DROP_SUCCESS = "DropSuccess"
    INSERT_RESULT = "InsertResult"


# Define constants for message values
class MessageValues:
    SYNTAX_ERROR = "Syntax error"
    DUPLICATE_COLUMN_DEF_ERROR = (
        "Create table has failed: column definition is duplicated"
    )
    DUPLICATE_PRIMARY_KEY_DEF_ERROR = (
        "Create table has failed: primary key definition is duplicated"
    )
    REFERENCE_TYPE_ERROR = "Create table has failed: foreign key references wrong type"
    REFERENCE_NON_PRIMARY_KEY_ERROR = (
        "Create table has failed: foreign key references non primary key column"
    )
    REFERENCE_EXISTENCE_ERROR = (
        "Create table has failed: foreign key references non existing table or column"
    )
    PRIMARY_KEY_COLUMN_DEF_ERROR = "Create table has failed: cannot define non-existing column '{col_name}' as primary key"
    FOREIGN_KEY_COLUMN_DEF_ERROR = "Create table has failed: cannot define non-existing column '{col_name}' as foreign key"
    TABLE_EXISTENCE_ERROR = (
        "Create table has failed: table with the same name already exists"
    )
    CHAR_LENGTH_ERROR = "Char length should be over 0"
    NO_SUCH_TABLE = "{command_name} has failed: no such table"
    DROP_REFERENCED_TABLE_ERROR = (
        "Drop table has failed: '{table_name}' is referenced by another table"
    )
    SELECT_TABLE_EXISTENCE_ERROR = "Select has failed: '{table_name}' does not exist"
    CREATE_TABLE_SUCCESS = "{table_name} table is created"
    DROP_SUCCESS = "{table_name} table is dropped"
    INSERT_RESULT = "The row is inserted"


class MessageHandler:
    @staticmethod
    def print_error(message_type, **kwargs):
        messages = {
            MessageKeys.SYNTAX_ERROR: MessageValues.SYNTAX_ERROR,
            MessageKeys.DUPLICATE_COLUMN_DEF_ERROR: MessageValues.DUPLICATE_COLUMN_DEF_ERROR,
            MessageKeys.DUPLICATE_PRIMARY_KEY_DEF_ERROR: MessageValues.DUPLICATE_PRIMARY_KEY_DEF_ERROR,
            MessageKeys.REFERENCE_TYPE_ERROR: MessageValues.REFERENCE_TYPE_ERROR,
            MessageKeys.REFERENCE_NON_PRIMARY_KEY_ERROR: MessageValues.REFERENCE_NON_PRIMARY_KEY_ERROR,
            MessageKeys.REFERENCE_EXISTENCE_ERROR: MessageValues.REFERENCE_EXISTENCE_ERROR,
            MessageKeys.PRIMARY_KEY_COLUMN_DEF_ERROR: MessageValues.PRIMARY_KEY_COLUMN_DEF_ERROR,
            MessageKeys.FOREIGN_KEY_COLUMN_DEF_ERROR: MessageValues.FOREIGN_KEY_COLUMN_DEF_ERROR,
            MessageKeys.TABLE_EXISTENCE_ERROR: MessageValues.TABLE_EXISTENCE_ERROR,
            MessageKeys.CHAR_LENGTH_ERROR: MessageValues.CHAR_LENGTH_ERROR,
            MessageKeys.NO_SUCH_TABLE: MessageValues.NO_SUCH_TABLE,
            MessageKeys.DROP_REFERENCED_TABLE_ERROR: MessageValues.DROP_REFERENCED_TABLE_ERROR,
            MessageKeys.SELECT_TABLE_EXISTENCE_ERROR: MessageValues.SELECT_TABLE_EXISTENCE_ERROR,
        }
        message_template = messages.get(message_type, "Unknown error")
        try:
            message = message_template.format(**kwargs)
        except KeyError as e:
            missing_key = e.args[0]
            print(
                f"Error: Missing keyword argument '{missing_key}' for message formatting."
            )
            return
        print(message)

    @staticmethod
    def print_success(message_type, **kwargs):
        messages = {
            MessageKeys.CREATE_TABLE_SUCCESS: MessageValues.CREATE_TABLE_SUCCESS,
            MessageKeys.DROP_SUCCESS: MessageValues.DROP_SUCCESS,
            MessageKeys.INSERT_RESULT: MessageValues.INSERT_RESULT,
        }
        message_template = messages.get(message_type, "Unknown message")
        try:
            message = message_template.format(**kwargs)
        except KeyError as e:
            missing_key = e.args[0]
            print(
                f"Error: Missing keyword argument '{missing_key}' for message formatting."
            )
            return
        print(message)


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


class Formatter:
    @staticmethod
    def format_table(headers, rows):
        # Calculate column widths
        col_widths = [len(header) for header in headers]
        for row in rows:
            for idx, cell in enumerate(row):
                col_widths[idx] = max(col_widths[idx], len(str(cell)))

        # Build the format string
        format_str = " | ".join(f"{{:<{w}}}" for w in col_widths)

        # Format header
        header_line = format_str.format(*headers)
        # Compute total width of the header line
        total_width = len(header_line)
        # Build the separator line
        separator_line = "-" * total_width

        # Format rows
        row_lines = [format_str.format(*[str(cell) for cell in row]) for row in rows]

        # Combine all parts in the required order:
        # separator, header, rows, separator
        table = "\n".join([separator_line, header_line] + row_lines + [separator_line])

        return table

    @staticmethod
    def format_footer(row_count):
        return (
            f"{row_count} row in set" if row_count == 1 else f"{row_count} rows in set"
        )

    @staticmethod
    def format_table_list(table_names):
        if not table_names:
            return "No tables found."
        else:
            # Calculate the maximum length of table names
            max_length = max(len(name) for name in table_names)
            format_str = f"{{:<{max_length}}}"

            # Build separator line
            total_width = max_length
            separator_line = "-" * total_width

            # Format rows
            row_lines = [format_str.format(name) for name in table_names]

            # Combine all parts
            table = "\n".join([separator_line] + row_lines + [separator_line])
            footer = Formatter.format_footer(len(table_names))
            return "\n".join([table, footer])


class TableMetadata:
    def __init__(
        self,
        table_name,
        columns,
        pk_constraints: list = None,
        fk_constraints=None,
    ):
        self.table_name = table_name.lower()  # Store table name in lowercase
        self.columns = {
            col["name"].lower(): col for col in columns
        }  # Store column info in lowercase
        self.pk_sets = (
            pk_constraints[0]["key_list"] if pk_constraints else []
        )  # Initialize primary key info
        self.fk_constraints = fk_constraints or []  # Initialize foreign key info

    def get_column(self, column_name):
        return self.columns.get(column_name.lower())

    def describe(self):
        headers = ["column_name", "type", "null", "key"]
        rows = []
        for col in self.columns.values():
            col_name = col["name"]
            col_type = col["type"]
            col_null = "N" if col.get("not_null", False) else "Y"
            col_key = ""
            if col_name in self.pk_sets and any(
                col_name in fk["key_list"] for fk in self.fk_constraints
            ):
                col_key = "PRI/FOR"
            elif col_name in self.pk_sets:
                col_key = "PRI"
            elif any(col_name in fk["key_list"] for fk in self.fk_constraints):
                col_key = "FOR"

            rows.append([col_name, col_type, col_null, col_key])

        table_str = Formatter.format_table(headers, rows)
        footer = Formatter.format_footer(len(rows))
        return "\n".join([table_str, footer])


class Database:
    def __init__(self):
        self.db = db.DB()
        self.db.open("myDB.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)

    # Abstracted methods for database access
    def get_table_metadata(self, table_name):
        table_meta_data_serialized = self.db.get(f"table_schema:{table_name}".encode())
        if not table_meta_data_serialized:
            return None
        return pickle.loads(table_meta_data_serialized)

    def put_table_metadata(self, table_name, table_metadata):
        self.db.put(f"table_schema:{table_name}".encode(), pickle.dumps(table_metadata))

    def delete_table_metadata(self, table_name):
        self.db.delete(f"table_schema:{table_name}".encode())

    def get_table_data(self, table_name):
        table_data_serialized = self.db.get(f"table_data:{table_name}".encode())
        if not table_data_serialized:
            return None
        return pickle.loads(table_data_serialized)

    def put_table_data(self, table_name, data):
        self.db.put(f"table_data:{table_name}".encode(), pickle.dumps(data))

    def delete_table_data(self, table_name):
        self.db.delete(f"table_data:{table_name}".encode())

    def get_foreign_key_metadata(self):
        fk_metadata_serialized = self.db.get("foreign_key_metadata".encode())
        if not fk_metadata_serialized:
            return []
        return pickle.loads(fk_metadata_serialized)

    def put_foreign_key_metadata(self, fk_metadata_list):
        self.db.put("foreign_key_metadata".encode(), pickle.dumps(fk_metadata_list))

    def delete_foreign_key_metadata(self):
        self.db.delete("foreign_key_metadata".encode())

    def table_exists(self, table_name):
        return self.db.get(f"table_schema:{table_name}".encode()) is not None

    def create_table(
        self,
        table_name: str,
        columns: list,
        pk_constraints: list = None,
        fk_constraints=None,
    ):
        table_name = table_name.lower()

        # Error handling before trying to create the table

        # Handling duplicate column definitions - DuplicateColumnDefError
        column_names = [col["name"].lower() for col in columns]
        if len(column_names) != len(set(column_names)):
            MessageHandler.print_error(MessageKeys.DUPLICATE_COLUMN_DEF_ERROR)
            return

        # Handling multiple primary key definitions - DuplicatePrimaryKeyDefError
        if pk_constraints is not None and len(pk_constraints) > 1:
            MessageHandler.print_error(MessageKeys.DUPLICATE_PRIMARY_KEY_DEF_ERROR)
            return

        # Handling table with the same name already exists - TableExistenceError
        if self.table_exists(table_name):
            MessageHandler.print_error(MessageKeys.TABLE_EXISTENCE_ERROR)
            return

        for col in columns:
            # Handling char type length less than 1 - CharLengthError
            if col["type"].startswith("char"):
                char_length = int(col["type"][5:-1])
                if char_length < 1:
                    MessageHandler.print_error(MessageKeys.CHAR_LENGTH_ERROR)
                    return

        for pk in pk_constraints or []:
            # Handling non-existing column defined as primary key - PrimaryKeyColumnDefError
            for col_name in pk["key_list"]:
                if col_name.lower() not in column_names:
                    MessageHandler.print_error(
                        MessageKeys.PRIMARY_KEY_COLUMN_DEF_ERROR, col_name=col_name
                    )
                    return

        for fk in fk_constraints or []:
            # Handling non-existing column defined as foreign key - ForeignKeyColumnDefError
            for col_name in fk["key_list"]:
                if col_name.lower() not in column_names:
                    MessageHandler.print_error(
                        MessageKeys.FOREIGN_KEY_COLUMN_DEF_ERROR, col_name=col_name
                    )
                    return

        # Reference existence and type check for foreign keys
        for fk in fk_constraints or []:
            for key_idx, fk_col_name in enumerate(fk["key_list"]):
                ref_table_name = fk["ref_table"].lower()
                ref_table_metadata = self.get_table_metadata(ref_table_name)
                # Handling foreign key references non-existing table - ReferenceExistenceError
                if not ref_table_metadata:
                    MessageHandler.print_error(MessageKeys.REFERENCE_EXISTENCE_ERROR)
                    return

                fk_col = next(
                    (
                        col
                        for col in columns
                        if col["name"].lower() == fk_col_name.lower()
                    ),
                    None,
                )

                ref_col_name = fk["other_key_list"][key_idx]
                ref_col = ref_table_metadata.get_column(ref_col_name)

                if not ref_col:
                    # Handling foreign key references non-existing column - ReferenceExistenceError
                    MessageHandler.print_error(MessageKeys.REFERENCE_EXISTENCE_ERROR)
                    return
                # Type check
                if fk_col["type"] != ref_col["type"]:
                    # Handling foreign key references wrong type - ReferenceTypeError
                    MessageHandler.print_error(MessageKeys.REFERENCE_TYPE_ERROR)
                    return

            # Check if the referenced column is a primary key for the entire key list
            if set(fk["other_key_list"]) != set(ref_table_metadata.pk_sets):
                MessageHandler.print_error(MessageKeys.REFERENCE_NON_PRIMARY_KEY_ERROR)
                return

        table_metadata = TableMetadata(
            table_name, columns, pk_constraints, fk_constraints
        )
        # Update primary key columns to be not null
        for pk in pk_constraints or []:
            for col_name in pk["key_list"]:
                if col_name.lower() in table_metadata.columns:
                    table_metadata.columns[col_name.lower()]["not_null"] = True

        fk_metadata_list = self.get_foreign_key_metadata()
        for fk in fk_constraints or []:
            fk_metadata_list.append(
                ForeignKeyMetadata(
                    table_name, fk["key_list"], fk["ref_table"], fk["other_key_list"]
                )
            )

        self.put_table_metadata(table_name, table_metadata)
        self.put_table_data(table_name, [])
        self.put_foreign_key_metadata(fk_metadata_list)

        MessageHandler.print_success(
            MessageKeys.CREATE_TABLE_SUCCESS, table_name=table_name
        )

    def print_fk_metadata(self):
        fk_metadata_list = self.get_foreign_key_metadata()
        [print(i.describe()) for i in fk_metadata_list]

    def drop_table(self, table_name: str):
        table_name = table_name.lower()

        if not self.get_table_metadata(table_name):
            MessageHandler.print_error(MessageKeys.NO_SUCH_TABLE, command_name="Drop")
            return

        fk_metadata_list = self.get_foreign_key_metadata()
        # Check if the table is referenced by any foreign keys
        if any(fk.referenced_parent_table == table_name for fk in fk_metadata_list):
            MessageHandler.print_error(
                MessageKeys.DROP_REFERENCED_TABLE_ERROR, table_name=table_name
            )
            return

        # Remove foreign keys where the child table is being dropped
        fk_metadata_list = [
            fk for fk in fk_metadata_list if fk.child_table_name != table_name
        ]
        self.put_foreign_key_metadata(fk_metadata_list)

        self.delete_table_metadata(table_name)
        self.delete_table_data(table_name)

        MessageHandler.print_success(MessageKeys.DROP_SUCCESS, table_name=table_name)

    def describe_table(self, table_name: str, command_name):
        table_name = table_name.lower()

        table_metadata: TableMetadata = self.get_table_metadata(table_name)
        if not table_metadata:
            MessageHandler.print_error(
                MessageKeys.NO_SUCH_TABLE, command_name=command_name
            )
            return

        print(table_metadata.describe())

    def show_tables(self):
        cursor = self.db.cursor()
        table_names = []
        record = cursor.first()
        while record:
            key, _ = record
            if key.decode().startswith("table_schema:"):
                table_name = key.decode().split("table_schema:")[1]
                table_names.append(table_name)
            record = cursor.next()
        cursor.close()

        output = Formatter.format_table_list(table_names)
        print(output)

    def insert_into_table(
        self, table_name: str, values: list, column_sequence: list = None
    ):
        table_name = table_name.lower()
        table_metadata: TableMetadata = self.get_table_metadata(table_name)
        if not table_metadata:
            MessageHandler.print_error(
                MessageKeys.NO_SUCH_TABLE, command_name="Insert into"
            )
            return

        columns_dict = table_metadata.columns
        rows: list = self.get_table_data(table_name) or []
        row = dict()

        if column_sequence is None:
            column_sequence = [columns_dict[i]["name"] for i in columns_dict]

        for idx, column_name in enumerate(column_sequence):
            type_str: str = table_metadata.get_column(column_name)["type"]
            value = values[idx]
            if type_str.startswith("char"):
                char_length = int(type_str[5:-1])
                row[column_name] = value[:char_length]
            else:
                row[column_name] = value

        rows.append(row)
        self.put_table_data(table_name, rows)
        MessageHandler.print_success(MessageKeys.INSERT_RESULT)

    def select_from_table(self, table_name: str):
        table_name = table_name.lower()

        table_metadata: TableMetadata = self.get_table_metadata(table_name)

        if not table_metadata:
            MessageHandler.print_error(
                MessageKeys.SELECT_TABLE_EXISTENCE_ERROR, table_name=table_name
            )
            return

        columns_dict = table_metadata.columns
        column_name_list = [columns_dict[i]["name"] for i in columns_dict]
        rows_data: list = self.get_table_data(table_name) or []

        headers = column_name_list
        rows = []
        for row_data in rows_data:
            row = [str(row_data.get(col, "NULL")) for col in column_name_list]
            rows.append(row)

        if rows:
            table_str = Formatter.format_table(headers, rows)
            footer = Formatter.format_footer(len(rows))
            print("\n".join([table_str, footer]))
        else:
            print("Empty set")

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

        pk_constraints = []
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
                    {"name": name, "type": type, "not_null": is_not_null}
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
    multi_lines = []
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


# End-to-End test function
def run_tests():
    create_table_test_queries = [
        # 1. Valid table creation - students table
        "CREATE TABLE students (student_id INT NOT NULL, student_name CHAR(20), student_age INT, PRIMARY KEY (student_id));",
        # 2. SHOW TABLES
        "SHOW TABLES;",
        # 3. DESCRIBE students
        "DESCRIBE students;",
    ]

    test_queries = create_table_test_queries

    for idx, query in enumerate(test_queries):
        print(f"{idx}th: Running query: {query}")
        try:
            parsedQuery = sql_parser.parse(query)
            transformer.transform(parsedQuery)
        except LarkError as e:
            print(e)
            MessageHandler.print_error(MessageKeys.SYNTAX_ERROR)


def main_function():
    while True:
        inputQuery = handleInput()
        splittedQueries = inputQuery.split(";")[:-1]
        for query in splittedQueries:
            try:
                parsedQuery = sql_parser.parse(query + ";")
                transformer.transform(parsedQuery)
            except LarkError as e:
                MessageHandler.print_error(MessageKeys.SYNTAX_ERROR)
                break


# Run tests
run_tests()

# Print foreign key metadata (if needed)
# database.print_fk_metadata()

# Main loop (Uncomment for interactive mode)
main_function()
