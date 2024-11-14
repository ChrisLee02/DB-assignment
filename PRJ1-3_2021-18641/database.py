import datetime
from berkeleydb import db
import pickle
from utils import (
    AmbiguousReference,
    BooleanCondition,
    ColumnNotFoundError,
    ColumnReference,
    Condition,
    IncomparableTypeError,
    LiteralValue,
    NullCondition,
    TableNotSpecified,
    evaluate_condition,
    get_value,
)
from messages import MessageHandler, MessageKeys
from metadata import TableMetadata, ForeignKeyMetadata
from formatter import Formatter
from itertools import product


class Database:
    def __init__(self):
        self.db = db.DB()
        self.db.open("myDB.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)

    # Abstracted functions to manipulate db file.
    def get_table_metadata(self, table_name):
        table_meta_data_serialized = self.db.get(f"table_schema:{table_name}".encode())
        if not table_meta_data_serialized:
            return None

        table_meta_data: TableMetadata = pickle.loads(table_meta_data_serialized)
        return table_meta_data

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

    # methods to implement DDL and DML
    def create_table(
        self,
        table_name: str,
        columns: list,
        pk_constraints: list = None,
        fk_constraints=None,
    ):
        table_name = table_name.lower()

        # error handling logic
        column_names = [col["name"].lower() for col in columns]
        if len(column_names) != len(set(column_names)):
            MessageHandler.print_error(MessageKeys.DUPLICATE_COLUMN_DEF_ERROR)
            return

        if pk_constraints is not None and len(pk_constraints) > 1:
            MessageHandler.print_error(MessageKeys.DUPLICATE_PRIMARY_KEY_DEF_ERROR)
            return

        if self.table_exists(table_name):
            MessageHandler.print_error(MessageKeys.TABLE_EXISTENCE_ERROR)
            return

        for col in columns:
            if col["type"].startswith("char"):
                char_length = int(col["type"][5:-1])
                if char_length < 1:
                    MessageHandler.print_error(MessageKeys.CHAR_LENGTH_ERROR)
                    return

        for pk in pk_constraints or []:
            for col_name in pk["key_list"]:
                if col_name.lower() not in column_names:
                    MessageHandler.print_error(
                        MessageKeys.PRIMARY_KEY_COLUMN_DEF_ERROR, col_name=col_name
                    )
                    return

        for fk in fk_constraints or []:
            for col_name in fk["key_list"]:
                if col_name.lower() not in column_names:
                    MessageHandler.print_error(
                        MessageKeys.FOREIGN_KEY_COLUMN_DEF_ERROR, col_name=col_name
                    )
                    return

        for fk in fk_constraints or []:
            for key_idx, fk_col_name in enumerate(fk["key_list"]):
                ref_table_name = fk["ref_table"].lower()
                ref_table_metadata = self.get_table_metadata(ref_table_name)
                if not ref_table_metadata:
                    MessageHandler.print_error(MessageKeys.REFERENCE_EXISTENCE_ERROR)
                    return

                # use generator for concise code
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
                    MessageHandler.print_error(MessageKeys.REFERENCE_EXISTENCE_ERROR)
                    return
                if fk_col["type"] != ref_col["type"]:
                    MessageHandler.print_error(MessageKeys.REFERENCE_TYPE_ERROR)
                    return

            if set(fk["other_key_list"]) != set(ref_table_metadata.pk_sets):
                MessageHandler.print_error(MessageKeys.REFERENCE_NON_PRIMARY_KEY_ERROR)
                return

        table_metadata = TableMetadata(
            table_name, columns, pk_constraints, fk_constraints
        )

        # set pk columns as not null
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

    def drop_table(self, table_name: str):
        table_name = table_name.lower()

        if not self.table_exists(table_name):
            MessageHandler.print_error(
                MessageKeys.NO_SUCH_TABLE, command_name="Drop table"
            )
            return

        fk_metadata_list = self.get_foreign_key_metadata()
        if any(fk.referenced_parent_table == table_name for fk in fk_metadata_list):
            MessageHandler.print_error(
                MessageKeys.DROP_REFERENCED_TABLE_ERROR, table_name=table_name
            )
            return

        fk_metadata_list = [
            fk for fk in fk_metadata_list if fk.child_table_name != table_name
        ]

        # its own metadata and data, and regarded foreign key information should be deleted
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

        headers = ["column_name", "type", "null", "key"]
        rows = []
        for col in table_metadata.columns.values():
            col_name = col["name"]
            col_type = col["type"]
            col_null = "N" if col.get("not_null", False) else "Y"
            col_key = ""
            if col_name in table_metadata.pk_sets and any(
                col_name in fk["key_list"] for fk in table_metadata.fk_constraints
            ):
                col_key = "PRI/FOR"
            elif col_name in table_metadata.pk_sets:
                col_key = "PRI"
            elif any(
                col_name in fk["key_list"] for fk in table_metadata.fk_constraints
            ):
                col_key = "FOR"

            rows.append([col_name, col_type, col_null, col_key])

        # just generate header, rows and footer information
        # formatter class formats with given data.
        table_str = Formatter.format_table(headers, rows)
        footer = Formatter.format_footer(len(rows))
        print("\n".join([table_str, footer]))

    def show_tables(self):
        # use berkeley db's cursor to look around entire key-value
        cursor = self.db.cursor()
        table_names = []
        record = cursor.first()
        while record:
            key, _ = record

            # filter by prefix
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
        # 에러처리
        # case 1. column_sequence가 주어진 경우
        # -> 컬럼과 값의 개수가 다른 경우 InsertTypeMismatchError
        # -> 지정된 컬럼과 값의 타입이 맞지 않는 경우 InsertTypeMismatchError
        # -> 칼럼에 명시되지 않은 값들은 null로 들어가는데, 이때 InsertColumnNonNullableError 처리

        # case 2. column_sequence가 없는 경우
        # -> 개수비교해서 다르면 InsertTypeMismatchError
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

        column_sequence_from_metadata = [columns_dict[i]["name"] for i in columns_dict]

        # if only values received, use schema's column sequence.
        if column_sequence is None:
            column_sequence = column_sequence_from_metadata

        # check cardinality of values with columns
        if len(column_sequence) != len(values):
            MessageHandler.print_error(MessageKeys.INSERT_TYPE_MISMATCH_ERROR)
            return

        for idx, col_name in enumerate(column_sequence):
            col = table_metadata.get_column(col_name)
            if not col:
                MessageHandler.print_error(
                    MessageKeys.INSERT_COLUMN_EXISTENCE_ERROR, col_name=col_name
                )
                return
            type_str: str = col["type"]
            col_not_null: bool = col["not_null"]
            value = values[idx]

            # 타입체크를 해야함.
            # case: value 값이 null인 경우와, 그렇지 않은 경우로 나눈다.
            if isinstance(value, str) and value.lower() == "null":
                if col_not_null:
                    MessageHandler.print_error(
                        MessageKeys.INSERT_COLUMN_NON_NULLABLE_ERROR, col_name=col_name
                    )
                    return
                # row[col_name] = "null"
                row[f"{table_name}.{col_name}"] = None
            else:
                if type_str.startswith("char"):
                    if not isinstance(value, str):
                        MessageHandler.print_error(
                            MessageKeys.INSERT_TYPE_MISMATCH_ERROR
                        )
                        return

                    char_length = int(type_str[5:-1])
                    # row[col_name] = value[:char_length].ljust(char_length)
                    row[f"{table_name}.{col_name}"] = value[:char_length].ljust(
                        char_length
                    )
                elif type_str == "date":
                    try:
                        parsed_date = datetime.datetime.strptime(value, "%Y-%m-%d")
                        # row[col_name] = parsed_date.strftime("%Y-%m-%d")
                        row[f"{table_name}.{col_name}"] = parsed_date
                    except ValueError:
                        MessageHandler.print_error(
                            MessageKeys.INSERT_TYPE_MISMATCH_ERROR
                        )
                        return
                else:
                    if not isinstance(value, int):
                        MessageHandler.print_error(
                            MessageKeys.INSERT_TYPE_MISMATCH_ERROR
                        )
                        return
                    # row[col_name] = value
                    row[f"{table_name}.{col_name}"] = value

        for col_name in column_sequence_from_metadata:
            if col_name not in column_sequence:
                col_meta = columns_dict[col_name]
                if col_meta["not_null"]:
                    MessageHandler.print_error(
                        MessageKeys.INSERT_COLUMN_NON_NULLABLE_ERROR, col_name=col_name
                    )
                    return
                row[col_name] = None
        rows.append(row)
        self.put_table_data(table_name, rows)
        MessageHandler.print_success(MessageKeys.INSERT_RESULT)

    def select_from_table(
        self,
        select_list: list[ColumnReference],
        referred_tables: list[str],
        join_conditions: list[Condition],
        where_condition,
        order_by_column: str,
        order_by_direction: str,
    ):
        try:
            table_metadata_list = {
                table_name: self.get_table_metadata(table_name.lower())
                for table_name in referred_tables
            }
            for table_name, metadata in table_metadata_list.items():
                if not metadata:
                    MessageHandler.print_error(
                        MessageKeys.SELECT_TABLE_EXISTENCE_ERROR, table_name=table_name
                    )
                    return

            if not select_list:
                select_list = []
                for table_name, metadata in table_metadata_list.items():
                    select_list.extend(
                        ColumnReference(table=table_name, column=col["name"])
                        for col in metadata.columns.values()
                    )

            table_dummy_data_row = {}
            for table_meta in table_metadata_list.values():
                table_dummy_data_row = {
                    **table_dummy_data_row,
                    **table_meta.get_dummy_row(),
                }
            # dummy를 활용해 select_list, join where condition을 검증
            # todo: select_list는
            for i in select_list:
                try:
                    get_value(i, table_dummy_data_row, referred_tables)
                except TableNotSpecified:
                    MessageHandler.print_error(
                        MessageKeys.SELECT_TABLE_EXISTENCE_ERROR, table_name=i.table
                    )
                    return
                except ColumnNotFoundError:
                    MessageHandler.print_error(
                        MessageKeys.SELECT_COLUMN_RESOLVE_ERROR, col_name=i.column
                    )
                    return
                except AmbiguousReference:
                    MessageHandler.print_error(
                        MessageKeys.SELECT_COLUMN_RESOLVE_ERROR, col_name=i.column
                    )
                    return

            try:
                for i in join_conditions:
                    evaluate_condition(i, table_dummy_data_row, referred_tables)
            except TableNotSpecified:
                MessageHandler.print_error(
                    MessageKeys.TABLE_NOT_SPECIFIED, clause_name="JOIN"
                )
                return
            except ColumnNotFoundError:
                MessageHandler.print_error(
                    MessageKeys.COLUMN_NOT_EXIST, clause_name="JOIN"
                )
                return
            except AmbiguousReference:
                MessageHandler.print_error(
                    MessageKeys.AMBIGUOUS_REFERENCE, clause_name="JOIN"
                )
                return

            try:
                evaluate_condition(
                    where_condition, table_dummy_data_row, referred_tables
                )
            except TableNotSpecified:
                MessageHandler.print_error(
                    MessageKeys.TABLE_NOT_SPECIFIED, clause_name="WHERE"
                )
                return
            except ColumnNotFoundError:
                MessageHandler.print_error(
                    MessageKeys.COLUMN_NOT_EXIST, clause_name="WHERE"
                )
                return
            except AmbiguousReference:
                MessageHandler.print_error(
                    MessageKeys.AMBIGUOUS_REFERENCE, clause_name="WHERE"
                )
                return

            table_data_list = {
                table_name: self.get_table_data(table_name.lower())
                for table_name in referred_tables
            }

            rows = table_data_list[referred_tables[0]]
            if len(referred_tables) > 1:
                rows = [
                    {key: value for row in combined_row for key, value in row.items()}
                    for combined_row in product(*table_data_list.values())
                ]

            try:
                # join
                if not join_conditions:
                    new_rows = []
                    for row in rows:
                        result = [
                            evaluate_condition(con, row, referred_tables)
                            for con in join_conditions
                        ]
                        if all(result):
                            new_rows.append(row)
                    rows = new_rows
            except TableNotSpecified:
                MessageHandler.print_error(
                    MessageKeys.TABLE_NOT_SPECIFIED, clause_name="JOIN"
                )
                return
            except ColumnNotFoundError:
                MessageHandler.print_error(
                    MessageKeys.COLUMN_NOT_EXIST, clause_name="JOIN"
                )
                return
            except AmbiguousReference:
                MessageHandler.print_error(
                    MessageKeys.AMBIGUOUS_REFERENCE, clause_name="JOIN"
                )
                return

            try:
                if where_condition is not None:
                    new_rows = []
                    for row in rows:
                        if evaluate_condition(where_condition, row, referred_tables):
                            new_rows.append(row)
                    rows = new_rows
            except TableNotSpecified:
                MessageHandler.print_error(
                    MessageKeys.TABLE_NOT_SPECIFIED, clause_name="WHERE"
                )
                return
            except ColumnNotFoundError:
                MessageHandler.print_error(
                    MessageKeys.COLUMN_NOT_EXIST, clause_name="WHERE"
                )
                return
            except AmbiguousReference:
                MessageHandler.print_error(
                    MessageKeys.AMBIGUOUS_REFERENCE, clause_name="WHERE"
                )
                return

            try:
                if order_by_column is not None:
                    # column 유효성 검증
                    get_value(
                        ColumnReference(None, order_by_column),
                        table_dummy_data_row,
                        referred_tables,
                    )
                    rows.sort(
                        key=lambda row: get_value(
                            ColumnReference(None, order_by_column),
                            row,
                            referred_tables,
                        ),
                        reverse=(order_by_direction == "desc"),
                    )
            except TableNotSpecified:
                MessageHandler.print_error(
                    MessageKeys.TABLE_NOT_SPECIFIED, clause_name="ORDER BY"
                )
                return
            except ColumnNotFoundError:
                MessageHandler.print_error(
                    MessageKeys.COLUMN_NOT_EXIST, clause_name="ORDER BY"
                )
                return
            except AmbiguousReference:
                MessageHandler.print_error(
                    MessageKeys.AMBIGUOUS_REFERENCE, clause_name="ORDER BY"
                )
                return
            # Formatting table to display
            table_str = Formatter.format_table_select(
                select_list, rows, referred_tables
            )
            footer = Formatter.format_footer(len(rows))
            print("\n".join([table_str, footer]))

        except IncomparableTypeError:
            MessageHandler.print_error(MessageKeys.INCOMPARABLE_ERROR)

    def delete_from_table(self, table_name: str, condition):
        try:
            table_name = table_name.lower()
            table_metadata: TableMetadata = self.get_table_metadata(table_name)
            if not table_metadata:
                MessageHandler.print_error(
                    MessageKeys.NO_SUCH_TABLE, command_name="Delete from"
                )
                return

            rows = self.get_table_data(table_name) or []

            # If condition is None, delete all rows
            if condition is None:
                deleted_count = len(rows)
                rows = []
            else:
                # Use dummy row in metadata to check incomparability when table is empty
                evaluate_condition(
                    condition, table_metadata.get_dummy_row(), [table_name]
                )

                # Delete rows that satisfy the condition
                new_rows = []
                deleted_count = 0
                for row in rows:
                    if not evaluate_condition(condition, row, [table_name]):
                        new_rows.append(row)
                    else:
                        deleted_count += 1
                rows = new_rows

            # Update table with new rows
            self.put_table_data(table_name, rows)

            # Success message with correct count handling
            MessageHandler.print_success(MessageKeys.DELETE_RESULT, count=deleted_count)

        except ColumnNotFoundError:
            MessageHandler.print_error(
                MessageKeys.COLUMN_NOT_EXIST, clause_name="WHERE"
            )
        except IncomparableTypeError:
            MessageHandler.print_error(MessageKeys.INCOMPARABLE_ERROR)
        except TableNotSpecified:
            MessageHandler.print_error(
                MessageKeys.TABLE_NOT_SPECIFIED, clause_name="WHERE"
            )
        except AmbiguousReference:
            MessageHandler.print_error(
                MessageKeys.AMBIGUOUS_REFERENCE, clause_name="WHERE"
            )

    def close(self):
        self.db.close()
