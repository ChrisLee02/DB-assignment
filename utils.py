# util funtion for readibility
def get_first_child_by_rule(tree, rule_name):
    for child in tree.children:
        if hasattr(child, "data") and child.data == rule_name:
            return child
    return None


class ColumnNotFoundError(Exception):
    pass


class IncomparableTypeError(Exception):
    pass


class TableNotSpecified(Exception):
    pass


class AmbiguousReference(Exception):
    pass


class ColumnReference:
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __str__(self):
        return f"{self.table}.{self.column}"


class LiteralValue:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"'{self.value}'" if isinstance(self.value, str) else str(self.value)


class Condition:
    def __init__(self, left_operand, operator, right_operand):
        self.left_operand = left_operand  # Could be ColumnReference or LiteralValue
        self.operator = operator
        self.right_operand = right_operand  # Could be ColumnReference or LiteralValue

    def __str__(self):
        return f"({str(self.left_operand)} {self.operator} {str(self.right_operand)})"


class NullCondition:
    def __init__(self, column_reference, is_not_null=False):
        self.column_reference = column_reference
        self.is_not_null = is_not_null

    def __str__(self):
        return (
            f"{str(self.column_reference)} IS {'NOT ' if self.is_not_null else ''}NULL"
        )


class BooleanCondition:
    def __init__(self, left, operator, right=None):
        self.left = left  # Could be Condition, NullCondition, or BooleanCondition
        self.operator = operator  # AND, OR, NOT 중 하나
        self.right = right  # AND, OR인 경우 오른쪽 표현식

    def __str__(self):
        if self.operator == "NOT":
            return f"(NOT {str(self.left)})"
        return f"({str(self.left)} {self.operator} {str(self.right)})"


def evaluate_condition(condition, row, from_tables):
    # todo: 지워야함.
    # print("evaluate:")
    # print(condition)
    # print(row)
    if isinstance(condition, Condition):
        left_value = get_value(condition.left_operand, row, from_tables)
        right_value = get_value(condition.right_operand, row, from_tables)

        # Type compatibility check
        if type(left_value) != type(right_value):
            raise IncomparableTypeError()

        # 공백 제거 처리 (오른쪽 공백만 제거)
        if isinstance(left_value, str):
            left_value = left_value.rstrip()
        if isinstance(right_value, str):
            right_value = right_value.rstrip()

        # Null handling
        if left_value is None or right_value is None:
            return False

        # Actual comparison
        if condition.operator == "=":
            return left_value == right_value
        elif condition.operator == "!=":
            return left_value != right_value
        elif condition.operator == "<":
            return left_value < right_value
        elif condition.operator == ">":
            return left_value > right_value
        elif condition.operator == "<=":
            return left_value <= right_value
        elif condition.operator == ">=":
            return left_value >= right_value


def get_value(operand, row, from_tables):
    if isinstance(operand, ColumnReference):
        if operand.table is not None:
            if operand.table not in from_tables:
                raise TableNotSpecified
            value = row.get(f"{operand.table}.{operand.column}", None)
            if value is None:
                raise ColumnNotFoundError
            return value
        else:
            matching_columns = [
                value
                for key, value in row.items()
                if key.endswith(f".{operand.column}")
            ]
            if len(matching_columns) == 0:
                raise ColumnNotFoundError
            elif len(matching_columns) > 1:
                raise AmbiguousReference
            return matching_columns[0]
    elif isinstance(operand, LiteralValue):
        return operand.value
    return None
