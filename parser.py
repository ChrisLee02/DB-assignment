import datetime
from lark import Token, Transformer, Tree
from database import Database
from utils import (
    BooleanCondition,
    Condition,
    NullCondition,
    ColumnReference,
    LiteralValue,
    get_first_child_by_rule,
)


# MyTransformer class extends lark.Transformer.
# Handles each case of command with corresponding action
# from parsed tree, extract data and format it as available for function's argument
class MyTransformer(Transformer):
    def __init__(self, database: Database):
        self.database = database

    def select_query_deprecated(self, items):
        from_clause = items[2].children[0]
        table_reference_list = from_clause.children[1]
        referred_table = table_reference_list.children[0]
        referred_table_name_tree = referred_table.children[0]

        referred_table_name = referred_table_name_tree.children[0].lower()
        self.database.select_from_table(referred_table_name)

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

    def parse_select_list(self, select_list):
        ret = []
        for select_item in select_list:
            table = (
                select_item.children[0]
                if select_item.children[0] is None
                else select_item.children[0].children[0].value
            )
            column = select_item.children[1].children[0].value

            ret.append(ColumnReference(table, column))
        return ret

    def parse_join_clause(self, join_clause):
        ret = {}
        ret["tables"] = []
        ret["conditions"] = []

        if join_clause is None:
            return ret

        join_clause = join_clause.children
        for join_item in join_clause:
            join_condition = join_item.children[3].children
            ret["tables"].append(join_item.children[1].children[0].lower())
            col_ref_1 = ColumnReference(
                join_condition[0].children[0], join_condition[1].children[0]
            )
            col_ref_2 = ColumnReference(
                join_condition[3].children[0], join_condition[4].children[0]
            )
            ret["conditions"].append(Condition(col_ref_1, "=", col_ref_2))
        return ret

    def select_query(self, items):
        # for item in items[1:]:
        #     print(item.pretty())
        #     print("==================")

        select_list = self.parse_select_list(items[1].children)

        from_clause_table = items[2].children[1].children[0].lower()

        join_clause = self.parse_join_clause(items[3])

        referred_tables = [from_clause_table] + join_clause["tables"]
        join_conditions = join_clause["conditions"]

        if items[4] is not None:
            where_clause = get_first_child_by_rule(items[4], "boolean_expr")
            where_condition = self.parse_where_clause(where_clause)
        else:
            where_condition = None

        # todo: 여기부터
        order_by_column = None
        order_by_direction = None

        if items[5] is not None:
            order_by_column = items[5].children[2].children[0].lower()
            order_by_direction = "asc"
            if items[5].children[3] is not None:
                order_by_direction = items[5].children[3].lower()

        self.database.select_from_table(
            select_list,
            referred_tables,
            join_conditions,
            where_condition,
            order_by_column,
            order_by_direction,
        )

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
        if where_clause.data == "boolean_expr":
            terms = [
                self.parse_where_clause(child)
                for child in where_clause.children
                if isinstance(child, Tree)
            ]
            if len(terms) == 1:
                return terms[0]
            else:
                return BooleanCondition(terms[0], "OR", terms[1])

        elif where_clause.data == "boolean_term":
            factors = [
                self.parse_where_clause(child)
                for child in where_clause.children
                if isinstance(child, Tree)
            ]
            if len(factors) == 1:
                return factors[0]
            else:
                return BooleanCondition(factors[0], "AND", factors[1])

        elif where_clause.data == "boolean_factor":
            if where_clause.children[0] is None:
                return self.parse_where_clause(where_clause.children[1])
            else:
                factor = self.parse_where_clause(where_clause.children[1])
                return BooleanCondition(factor, "NOT")

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

            return Condition(left_operand, operator, right_operand)

        elif where_clause.data == "null_predicate":
            table_name = None
            if where_clause.children[0] is not None:  # [table_name, column_name]
                table_name = where_clause.children[0].children[0].value.lower()

            column_name = where_clause.children[1].children[0].value.lower()

            null_operation = where_clause.children[2]
            is_not_null = null_operation.children[1] is not None
            return NullCondition(ColumnReference(table_name, column_name), is_not_null)

    def parse_operand(self, operand):
        # Parse the operand to distinguish between column_name and literal value
        if (
            operand.children[0] is not None
            and operand.children[0].data == "comparable_value"
        ):
            value = operand.children[0].children[0].value

            # Handle different types of comparable values
            if operand.children[0].children[0].type == "STR":
                # Strip quotation marks from the string value
                return LiteralValue(value.strip("'\""))
            elif operand.children[0].children[0].type == "DATE":
                # Convert string to datetime object
                parsed_date = datetime.datetime.strptime(value, "%Y-%m-%d")
                return LiteralValue(parsed_date)
            elif operand.children[0].children[0].type == "INT":
                # Convert string to integer
                return LiteralValue(int(value))

        else:
            table_name = None
            if operand.children[0] is not None:  # [table_name, column_name]
                table_name = operand.children[0].children[0].value.lower()

            column_name = operand.children[1].children[0].value.lower()
            return ColumnReference(table_name, column_name)

    def exit_command(self, items):
        self.database.close()
        exit()
