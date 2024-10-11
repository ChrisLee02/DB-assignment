from lark import Lark, LarkError
from database import Database
from parser import MyTransformer
from messages import MessageHandler, MessageKeys

# Load sql parser declared in grammar.lark
with open("grammar.lark") as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

# Create an Instance of Database and MyTransformer
database = Database()
transformer = MyTransformer(database)


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

# Main loop
# Receive input query(queries) and split each query into list for executing
while True:
    inputQuery = handleInput()

    # Remove last element which is empty string due to last semicolon
    splittedQueries = inputQuery.split(";")[:-1]
    
    # Parse and transform each query
    # Break the loop if a LarkError occurs (syntax error)
    for query in splittedQueries:
        try:
            parsedQuery = sql_parser.parse(query + ";")
            transformer.transform(parsedQuery)
        except LarkError as e:
            MessageHandler.print_error(MessageKeys.SYNTAX_ERROR)
            break
