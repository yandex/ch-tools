def _format_str_match(value):
    if value is None:
        return None

    if value.find(",") < 0:
        return f"LIKE '{value}'"

    return "IN ({0})".format(
        ",".join("'{0}'".format(item.strip()) for item in value.split(","))
    )


def _format_str_imatch(value):
    if value is None:
        return None

    return _format_str_match(value.lower())
