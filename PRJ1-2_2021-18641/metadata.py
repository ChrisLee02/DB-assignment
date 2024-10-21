from formatter import Formatter

# in this file, classes for describing metadata is defined.


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

    # just for debugging
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
        pk_constraints: list = None,
        fk_constraints=None,
    ):
        self.table_name = table_name.lower()
        self.columns = {col["name"].lower(): col for col in columns}
        self.pk_sets = pk_constraints[0]["key_list"] if pk_constraints else []
        self.fk_constraints = fk_constraints or []

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
