from decimal import Decimal


def print_records(rows: list[dict], headers: list[str]):
    column_widths = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            value = row.get(header, "")
            if value is None:
                value = "None"
            elif isinstance(value, (float, Decimal)):
                value = f"{float(value):.3f}"
            column_widths[header] = max(column_widths[header], len(str(value)))

    header_line = "  ".join(f"{header:<{column_widths[header]}}" for header in headers)
    separator_line = "-" * len(header_line)
    print(separator_line)
    print(header_line)
    print(separator_line)

    if not rows:
        return

    for row in rows:
        row_line = "  ".join(
            (
                f"{'None' if row.get(header) is None else (f'{float(row.get(header)):.3f}' if isinstance(row.get(header), (float, Decimal)) else str(row.get(header))):<{column_widths[header]}}"
            )
            for header in headers
        )
        print(row_line)
    print(separator_line)


def print_records_recommend(rows: list[dict], headers: list[str], message: str):
    column_widths = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            value = row.get(header, "")
            if value is None:
                value = "None"
            elif isinstance(value, (float, Decimal)):
                value = f"{float(value):.3f}"
            column_widths[header] = max(column_widths[header], len(str(value)))

    header_line = "  ".join(f"{header:<{column_widths[header]}}" for header in headers)
    separator_line = "-" * len(header_line)
    print(separator_line)
    print(message)
    print(separator_line)
    print(header_line)
    print(separator_line)

    if not rows:
        return

    for row in rows:
        row_line = "  ".join(
            (
                f"{'None' if row.get(header) is None else (f'{float(row.get(header)):.3f}' if isinstance(row.get(header), (float, Decimal)) else str(row.get(header))):<{column_widths[header]}}"
            )
            for header in headers
        )
        print(row_line)
    if message == "Popularity-based":
        print(separator_line)
