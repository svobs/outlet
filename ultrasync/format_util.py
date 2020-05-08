import humanfriendly


def humanfriendlier_size(size_bytes):
    size_hf = humanfriendly.format_size(size_bytes)
    size_tokens = size_hf.split(' ')
    if size_tokens[1] == 'bytes':
        return size_hf
    size = "{0:,.2f}".format(float(size_tokens[0]))
    return f'{size} {size_tokens[1]}'


def with_commas(integer):
    return f"{integer:n}"
