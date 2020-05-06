import humanfriendly


def humanfriendlier_size(size_bytes):
    size_hf = humanfriendly.format_size(size_bytes)
    size_tokens = size_hf.split(' ')
    return "{:,.2f}".format(float(size_tokens[0])) + size_tokens[1]


def with_commas(integer):
    return f"{integer:n}"
