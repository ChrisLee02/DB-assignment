from lark import Lark, LarkError
from database import Database
from parser import MyTransformer
from messages import MessageHandler, MessageKeys

with open("grammar.lark") as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

database = Database()
transformer = MyTransformer(database)


def print_prompt():
    print("DB_2021-18641>", end=" ")


def handleInput():
    print_prompt()
    multi_lines = []
    while True:
        input_line = input().strip()
        multi_lines.append(input_line)
        if input_line.endswith(";"):
            break

    return " ".join(multi_lines)


def main_function():
    while True:
        inputQuery = handleInput()
        if not inputQuery:
            break
        splittedQueries = inputQuery.split(";")[:-1]
        for query in splittedQueries:
            try:
                parsedQuery = sql_parser.parse(query + ";")
                transformer.transform(parsedQuery)
            except LarkError as e:
                MessageHandler.print_error(MessageKeys.SYNTAX_ERROR)
                break


main_function()
