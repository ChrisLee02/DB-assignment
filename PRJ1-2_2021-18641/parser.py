from lark import Transformer
from database import Database
from utils import get_first_child_by_rule


# MyTransformer class extents lark.Transformer.
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

    def exit_command(self, items):
        self.database.close()
        exit()
