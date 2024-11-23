from lark import Lark, LarkError, Transformer
from berkeleydb import db
import pickle


# Print the prompt in the required format
def print_prompt():
    print("DB_2021-18641>", end=" ")


# Print the parsed query name in the required format
def print_formatted(query_name):
    print_prompt()
    print(f"'{query_name}' requested")


# Print 'Syntax error' in the required format
def print_error():
    print_prompt()
    print("Syntax error")


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


# MyTransformer class extents lark.Transformer.
# Handles each case of command with corresponding action
class MyTransformer(Transformer):
    def create_table_query(self, items):
        print_formatted("CREATE TABLE")

    def select_query(self, items):
        print_formatted("SELECT")

    def drop_table_query(self, items):
        print_formatted("DROP TABLE")

    def describe_query(self, items):
        print_formatted("DESCRIBE")

    def desc_query(self, items):
        print_formatted("DESC")

    def explain_query(self, items):
        print_formatted("EXPLAIN")

    def show_tables_query(self, items):
        print_formatted("SHOW TABLES")

    def insert_query(self, items):
        print_formatted("INSERT")

    def delete_query(self, items):
        print_formatted("DELETE")

    def update_query(self, items):
        print_formatted("UPDATE")

    # for exit command, exit program
    def exit_command(self, items):
        myDB.close()
        exit()


# Load sql parser declared in grammar.lark
with open("grammar.lark") as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

# Create an Instance of MyTransformer
transformer = MyTransformer()
myDB = db.DB()
myDB.open("myDB.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)

# Main loop
# Receive input query(queries) and split each query into list for executing
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
            print_error()
            break
