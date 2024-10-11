class MessageKeys:
    SYNTAX_ERROR = "SyntaxError"
    DUPLICATE_COLUMN_DEF_ERROR = "DuplicateColumnDefError"
    DUPLICATE_PRIMARY_KEY_DEF_ERROR = "DuplicatePrimaryKeyDefError"
    REFERENCE_TYPE_ERROR = "ReferenceTypeError"
    REFERENCE_NON_PRIMARY_KEY_ERROR = "ReferenceNonPrimaryKeyError"
    REFERENCE_EXISTENCE_ERROR = "ReferenceExistenceError"
    PRIMARY_KEY_COLUMN_DEF_ERROR = "PrimaryKeyColumnDefError"
    FOREIGN_KEY_COLUMN_DEF_ERROR = "ForeignKeyColumnDefError"
    TABLE_EXISTENCE_ERROR = "TableExistenceError"
    CHAR_LENGTH_ERROR = "CharLengthError"
    NO_SUCH_TABLE = "NoSuchTable"
    DROP_REFERENCED_TABLE_ERROR = "DropReferencedTableError"
    SELECT_TABLE_EXISTENCE_ERROR = "SelectTableExistenceError"
    CREATE_TABLE_SUCCESS = "CreateTableSuccess"
    DROP_SUCCESS = "DropSuccess"
    INSERT_RESULT = "InsertResult"


class MessageValues:
    SYNTAX_ERROR = "Syntax error"
    DUPLICATE_COLUMN_DEF_ERROR = (
        "Create table has failed: column definition is duplicated"
    )
    DUPLICATE_PRIMARY_KEY_DEF_ERROR = (
        "Create table has failed: primary key definition is duplicated"
    )
    REFERENCE_TYPE_ERROR = "Create table has failed: foreign key references wrong type"
    REFERENCE_NON_PRIMARY_KEY_ERROR = (
        "Create table has failed: foreign key references non primary key column"
    )
    REFERENCE_EXISTENCE_ERROR = (
        "Create table has failed: foreign key references non existing table or column"
    )
    PRIMARY_KEY_COLUMN_DEF_ERROR = "Create table has failed: cannot define non-existing column '{col_name}' as primary key"
    FOREIGN_KEY_COLUMN_DEF_ERROR = "Create table has failed: cannot define non-existing column '{col_name}' as foreign key"
    TABLE_EXISTENCE_ERROR = (
        "Create table has failed: table with the same name already exists"
    )
    CHAR_LENGTH_ERROR = "Char length should be over 0"
    NO_SUCH_TABLE = "{command_name} has failed: no such table"
    DROP_REFERENCED_TABLE_ERROR = (
        "Drop table has failed: '{table_name}' is referenced by another table"
    )
    SELECT_TABLE_EXISTENCE_ERROR = "Select has failed: '{table_name}' does not exist"
    CREATE_TABLE_SUCCESS = "{table_name} table is created"
    DROP_SUCCESS = "{table_name} table is dropped"
    INSERT_RESULT = "The row is inserted"


class MessageHandler:
    @staticmethod
    def print_error(message_type, **kwargs):
        messages = {
            MessageKeys.SYNTAX_ERROR: MessageValues.SYNTAX_ERROR,
            MessageKeys.DUPLICATE_COLUMN_DEF_ERROR: MessageValues.DUPLICATE_COLUMN_DEF_ERROR,
            MessageKeys.DUPLICATE_PRIMARY_KEY_DEF_ERROR: MessageValues.DUPLICATE_PRIMARY_KEY_DEF_ERROR,
            MessageKeys.REFERENCE_TYPE_ERROR: MessageValues.REFERENCE_TYPE_ERROR,
            MessageKeys.REFERENCE_NON_PRIMARY_KEY_ERROR: MessageValues.REFERENCE_NON_PRIMARY_KEY_ERROR,
            MessageKeys.REFERENCE_EXISTENCE_ERROR: MessageValues.REFERENCE_EXISTENCE_ERROR,
            MessageKeys.PRIMARY_KEY_COLUMN_DEF_ERROR: MessageValues.PRIMARY_KEY_COLUMN_DEF_ERROR,
            MessageKeys.FOREIGN_KEY_COLUMN_DEF_ERROR: MessageValues.FOREIGN_KEY_COLUMN_DEF_ERROR,
            MessageKeys.TABLE_EXISTENCE_ERROR: MessageValues.TABLE_EXISTENCE_ERROR,
            MessageKeys.CHAR_LENGTH_ERROR: MessageValues.CHAR_LENGTH_ERROR,
            MessageKeys.NO_SUCH_TABLE: MessageValues.NO_SUCH_TABLE,
            MessageKeys.DROP_REFERENCED_TABLE_ERROR: MessageValues.DROP_REFERENCED_TABLE_ERROR,
            MessageKeys.SELECT_TABLE_EXISTENCE_ERROR: MessageValues.SELECT_TABLE_EXISTENCE_ERROR,
        }
        message_template = messages.get(message_type, "Unknown error")
        try:
            message = message_template.format(**kwargs)
        except KeyError as e:
            missing_key = e.args[0]
            print(
                f"Error: Missing keyword argument '{missing_key}' for message formatting."
            )
            return
        print(message)

    @staticmethod
    def print_success(message_type, **kwargs):
        messages = {
            MessageKeys.CREATE_TABLE_SUCCESS: MessageValues.CREATE_TABLE_SUCCESS,
            MessageKeys.DROP_SUCCESS: MessageValues.DROP_SUCCESS,
            MessageKeys.INSERT_RESULT: MessageValues.INSERT_RESULT,
        }
        message_template = messages.get(message_type, "Unknown message")
        try:
            message = message_template.format(**kwargs)
        except KeyError as e:
            missing_key = e.args[0]
            print(
                f"Error: Missing keyword argument '{missing_key}' for message formatting."
            )
            return
        print(message)
