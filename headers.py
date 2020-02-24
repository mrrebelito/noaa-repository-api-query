
def parse_fields(value):

    if isinstance(value, list) and len(value) > 1:
        return ';'.join(value)
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    elif isinstance(value, str):
        return value.replace('\n','')
    elif value is None:
        return ''



for item in lists:
    print(parse_fields(item))