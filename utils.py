def print_records(rows: list[dict], headers: list[str]):
    column_widths = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            value = row.get(header, "")
            if header == "avg.rating" and isinstance(value, (float, int)):
                value = f"{value:.3f}"
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
                f"{str(row.get(header, '')):<{column_widths[header]}}"
                if header != "avg.rating"
                else f"{row.get(header, ''):.3f:<{column_widths[header]}}"
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
            if header == "avg.rating" and isinstance(value, (float, int)):
                value = f"{value:.3f}"
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
                f"{str(row.get(header, '')):<{column_widths[header]}}"
                if header != "avg.rating"
                else f"{row.get(header, ''):.3f:<{column_widths[header]}}"
            )
            for header in headers
        )
        print(row_line)
    if message == "Rating-based":
        print(separator_line)
