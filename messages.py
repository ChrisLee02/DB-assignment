# modularize message key and value.


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
    INSERT_TYPE_MISMATCH_ERROR = "InsertTypeMismatchError"
    INSERT_COLUMN_EXISTENCE_ERROR = "InsertColumnExistenceError"
    INSERT_COLUMN_NON_NULLABLE_ERROR = "InsertColumnNonNullableError"
    DELETE_RESULT = "DeleteResult"
    SELECT_COLUMN_RESOLVE_ERROR = "SelectColumnResolveError"
    SELECT_COLUMN_NOT_GROUPED = "SelectColumnNotGrouped"
    TABLE_NOT_SPECIFIED = "TableNotSpecified"
    COLUMN_NOT_EXIST = "ColumnNotExist"
    AMBIGUOUS_REFERENCE = "AmbiguousReference"
    INCOMPARABLE_ERROR = "IncomparableError"


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
    INSERT_RESULT = "1 row inserted"
    INSERT_TYPE_MISMATCH_ERROR = "Insert has failed: types are not matched"
    INSERT_COLUMN_EXISTENCE_ERROR = "Insert has failed: '{col_name}' does not exist"
    INSERT_COLUMN_NON_NULLABLE_ERROR = "Insert has failed: '{col_name}' is not nullable"
    DELETE_RESULT = lambda count: f"{count} row deleted" if count == 1 else f"{count} rows deleted"
    SELECT_COLUMN_RESOLVE_ERROR = "Select has failed: fail to resolve '{col_name}'"
    SELECT_COLUMN_NOT_GROUPED = "Select has failed: column '{col_name}' must either be included in the GROUP BY clause or be used in an aggregate function"
    TABLE_NOT_SPECIFIED = (
        "{clause_name} clause trying to reference tables which are not specified"
    )
    COLUMN_NOT_EXIST = "{clause_name} clause trying to reference non existing column"
    AMBIGUOUS_REFERENCE = "{clause_name} clause contains ambiguous column reference"
    INCOMPARABLE_ERROR = "Trying to compare incomparable columns or values"


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
            MessageKeys.INSERT_TYPE_MISMATCH_ERROR: MessageValues.INSERT_TYPE_MISMATCH_ERROR,
            MessageKeys.INSERT_COLUMN_EXISTENCE_ERROR: MessageValues.INSERT_COLUMN_EXISTENCE_ERROR,
            MessageKeys.INSERT_COLUMN_NON_NULLABLE_ERROR: MessageValues.INSERT_COLUMN_NON_NULLABLE_ERROR,
            MessageKeys.SELECT_COLUMN_RESOLVE_ERROR: MessageValues.SELECT_COLUMN_RESOLVE_ERROR,
            MessageKeys.SELECT_COLUMN_NOT_GROUPED: MessageValues.SELECT_COLUMN_NOT_GROUPED,
            MessageKeys.TABLE_NOT_SPECIFIED: MessageValues.TABLE_NOT_SPECIFIED,
            MessageKeys.COLUMN_NOT_EXIST: MessageValues.COLUMN_NOT_EXIST,
            MessageKeys.AMBIGUOUS_REFERENCE: MessageValues.AMBIGUOUS_REFERENCE,
            MessageKeys.INCOMPARABLE_ERROR: MessageValues.INCOMPARABLE_ERROR,
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
            MessageKeys.DELETE_RESULT: MessageValues.DELETE_RESULT,
        }
        message_template = messages.get(message_type, "Unknown message")

        try:
            if callable(message_template):
                message = message_template(**kwargs)
            else:
                message = message_template.format(**kwargs)
        except KeyError as e:
            missing_key = e.args[0]
            print(
                f"Error: Missing keyword argument '{missing_key}' for message formatting."
            )
            return
        print(message)
