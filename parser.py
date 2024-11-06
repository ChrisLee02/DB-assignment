from lark import Transformer
from database import Database
from utils import get_first_child_by_rule
from error import (
    ColumnNotFoundError,
    IncomparableTypeError,
)  # Importing custom error classes


# MyTransformer class extends lark.Transformer.
# Handles each case of command with corresponding action
# from parsed tree, extract data and format it as available for function's argument
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
        table_name = items[2].children[0].lower()
        values_list_wrapped = items[5].children[1:-1]

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

    def delete_query(self, items):
        table_name = items[2].children[0].lower()
        # Check if there is a WHERE clause
        where_clause = None
        if items[3] is not None:
            where_clause = get_first_child_by_rule(items[3], "boolean_expr")
            condition = self.parse_where_clause(where_clause)
        else:
            condition = None

        self.database.delete_from_table(table_name, condition)

    def parse_where_clause(self, where_clause):
        try:
            # Recursive parsing logic to convert boolean_expr to a lambda condition
            print(where_clause)
            if where_clause.data == "boolean_expr":
                terms = [
                    self.parse_where_clause(child)
                    for child in where_clause.children
                    if child.data == "boolean_term"
                ]
                return lambda row: any(term(row) for term in terms)
            elif where_clause.data == "boolean_term":
                factors = [
                    self.parse_where_clause(child)
                    for child in where_clause.children
                    if child.data == "boolean_factor"
                ]
                return lambda row: all(factor(row) for factor in factors)
            elif where_clause.data == "boolean_factor":
                if where_clause.children[0] is None:
                    return self.parse_where_clause(where_clause.children[1])
                else:
                    factor = self.parse_where_clause(where_clause.children[1])
                    return lambda row: not factor(row)
            elif where_clause.data == "boolean_test":
                return self.parse_where_clause(where_clause.children[0])
            elif where_clause.data == "parenthesized_boolean_expr":
                return self.parse_where_clause(where_clause.children[1])
            elif where_clause.data == "predicate":
                return self.parse_where_clause(where_clause.children[0])
            elif where_clause.data == "comparison_predicate":
                left_operand = self.parse_operand(where_clause.children[0])
                right_operand = self.parse_operand(where_clause.children[2])
                operator = where_clause.children[1].value
                return self.create_comparison_lambda(
                    left_operand, right_operand, operator
                )
            elif where_clause.data == "null_predicate":
                # Parse null_predicate for IS NULL or IS NOT NULL
                column = self.parse_operand(where_clause.children[0])
                is_not_null = (
                    len(where_clause.children) > 2
                    and where_clause.children[1].data == "NOT"
                )
                if is_not_null:
                    return lambda row: column(row) is not None
                else:
                    return lambda row: column(row) is None
            else:
                raise ValueError(f"Unhandled tree node: {where_clause.data}")
        except (ColumnNotFoundError, IncomparableTypeError) as e:
            raise e
        except Exception as e:
            raise ValueError(f"Error while parsing WHERE clause: {str(e)}")

    def parse_operand(self, operand):
        try:
            print(operand)
            # Parse the operand to distinguish between column_name and literal value
            if operand.data == "column_name":
                column_name = operand.children[0].value
                return lambda row: row[column_name]
            elif operand.data == "comparable_value":
                value = operand.children[0].value
                return lambda _: value  # 모든 값을 람다 함수로 반환하여 일관성 유지
            else:
                raise ValueError(f"Unhandled operand type: {operand.data}")
        except KeyError as e:
            raise ColumnNotFoundError
        except Exception as e:
            raise ValueError(f"Error while parsing operand: {str(e)}")

    def create_comparison_lambda(self, left, right, operator):
        def comparison(row):
            try:
                left_value = left(row)
                right_value = right(row)

                if operator == "=":
                    return left_value == right_value
                elif operator == "!=":
                    return left_value != right_value
                elif operator == "<":
                    return left_value < right_value
                elif operator == ">":
                    return left_value > right_value
                elif operator == "<=":
                    return left_value <= right_value
                elif operator == ">=":
                    return left_value >= right_value
                else:
                    raise ValueError(f"Unhandled comparison operator: {operator}")
            except KeyError:
                raise ColumnNotFoundError
            except TypeError:
                raise IncomparableTypeError

        return comparison

    def exit_command(self, items):
        self.database.close()
        exit()
