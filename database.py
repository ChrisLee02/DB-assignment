from berkeleydb import db
import pickle
from messages import MessageHandler, MessageKeys
from metadata import TableMetadata, ForeignKeyMetadata
from formatter import Formatter


class Database:
    def __init__(self):
        self.db = db.DB()
        self.db.open("myDB.db", dbtype=db.DB_HASH, flags=db.DB_CREATE)

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

        # 에러 처리
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

        # 외래 키 참조 체크
        for fk in fk_constraints or []:
            for key_idx, fk_col_name in enumerate(fk["key_list"]):
                ref_table_name = fk["ref_table"].lower()
                ref_table_metadata = self.get_table_metadata(ref_table_name)
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

        # 기본 키 컬럼을 not null로 설정
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

        if not self.get_table_metadata(table_name):
            MessageHandler.print_error(MessageKeys.NO_SUCH_TABLE, command_name="Drop")
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
            col_name = col['name']
            col_type = col['type']
            col_null = 'N' if col.get('not_null', False) else 'Y'
            col_key = ''
            if col_name in table_metadata.pk_sets and any(col_name in fk['key_list'] for fk in table_metadata.fk_constraints):
                col_key = 'PRI/FOR'
            elif col_name in table_metadata.pk_sets:
                col_key = 'PRI'
            elif any(col_name in fk['key_list'] for fk in table_metadata.fk_constraints):
                col_key = 'FOR'

            rows.append([col_name, col_type, col_null, col_key])

        table_str = Formatter.format_table(headers, rows)
        footer = Formatter.format_footer(len(rows))
        print("\n".join([table_str, footer]))

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
            if isinstance(value, str) and value.lower() == 'null':
                row[column_name] = 'null'
            elif type_str.startswith("char"):
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

        table_str = Formatter.format_table(headers, rows)
        footer = Formatter.format_footer(len(rows))
        print("\n".join([table_str, footer]))
    
    def close(self):
        self.db.close()
