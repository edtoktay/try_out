class ResponseMapper:
    """
    A class that maps a list of rows to dictionaries using a given set of keys.

    Args:
        keys (list): A list of keys to be used for mapping.

    Methods:
        map(rows): Maps a list of rows to dictionaries using the provided keys.

    Returns:
        list: A list of dictionaries representing the mapped rows.
    """

    def __init__(self, keys) -> None:
        self.keys = keys

    def map(self, rows: list):
        response = [dict(zip(self.keys, row))
                    for row in rows] if rows is not None and len(rows) > 0 else []
        if response is not None and len(response) == 1:
            return response[0]
        return response
