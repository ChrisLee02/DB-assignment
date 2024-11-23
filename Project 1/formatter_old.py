# formatter.py


import datetime
from utils import get_value


class Formatter:
    @staticmethod
    def format_table_select(headers_col_refs, rows, referred_tables):
        # calculate width as max width of whole header and rows.
        # set each col_width as max width of column(header or any rows)
        # for appearance, set minimum total width as 15

        headers = [i.column if i.table is None else str(i) for i in headers_col_refs]
        col_widths = [len(header) for header in headers]
        for row in rows:
            for idx, header in enumerate(headers_col_refs):
                cell = get_value(header, row, referred_tables)

                col_widths[idx] = max(col_widths[idx], len(str(cell)))

        total_col_width = sum(col_widths) + 3 * (len(col_widths) - 1)
        total_width = max(total_col_width, 15)

        if total_col_width < total_width:
            # distribute extra space equally among columns
            extra_space = total_width - total_col_width
            add_per_col = extra_space // len(col_widths)
            col_widths = [w + add_per_col for w in col_widths]
            col_widths[0] += extra_space % len(col_widths)

        # generate format string for row formatting
        format_str = " | ".join(f"{{:<{w}}}" for w in col_widths)

        # create header and separator lines
        header_line = format_str.format(*headers)
        separator_line = "-" * total_width

        if not rows:
            # if no rows exist, only print headers and separator lines
            table = "\n".join([separator_line, header_line, separator_line])
        else:
            row_lines = [
                format_str.format(
                    *[
                        (
                            str(get_value(header, row, referred_tables))
                            if not isinstance(
                                get_value(header, row, referred_tables),
                                datetime.datetime,
                            )
                            else get_value(header, row, referred_tables).strftime(
                                "%Y-%m-%d"
                            )
                        )
                        for header in headers_col_refs
                    ]
                )
                for row in rows
            ]
            table = "\n".join(
                [separator_line, header_line, separator_line]
                + row_lines
                + [separator_line]
            )

        return table

    @staticmethod
    def format_table(headers, rows):

        # calculate width as max width of whole header and rows.
        # set each col_width as max width of column(header or any rows)
        # for appearance, set minimum total width as 15

        col_widths = [len(header) for header in headers]
        for row in rows:
            for idx, cell in enumerate(row):
                col_widths[idx] = max(col_widths[idx], len(str(cell)))

        total_col_width = sum(col_widths) + 3 * (len(col_widths) - 1)

        total_width = max(total_col_width, 15)

        if total_col_width < total_width:
            extra_space = total_width - total_col_width
            add_per_col = extra_space // len(col_widths)
            col_widths = [w + add_per_col for w in col_widths]
            col_widths[0] += extra_space % len(col_widths)

        format_str = " | ".join(f"{{:<{w}}}" for w in col_widths)

        header_line = format_str.format(*headers)
        separator_line = "-" * total_width

        if not rows:
            table = "\n".join([separator_line, separator_line])
        else:
            row_lines = [
                format_str.format(*[str(cell) for cell in row]) for row in rows
            ]
            table = "\n".join(
                [separator_line, header_line] + row_lines + [separator_line]
            )

        return table

    @staticmethod
    def format_footer(row_count):
        # handle row/rows according to even and odd.
        return (
            f"{row_count} row in set" if row_count == 1 else f"{row_count} rows in set"
        )

    @staticmethod
    def format_table_list(table_names):
        if not table_names:
            max_length = 0
        else:
            max_length = max(len(name) for name in table_names)

        total_width = max(max_length, 15)

        format_str = f"{{:<{total_width}}}"

        separator_line = "-" * total_width

        if not table_names:
            table = "\n".join([separator_line, separator_line])
        else:
            row_lines = [format_str.format(name) for name in table_names]
            table = "\n".join([separator_line] + row_lines + [separator_line])

        footer = Formatter.format_footer(len(table_names))
        return "\n".join([table, footer])
