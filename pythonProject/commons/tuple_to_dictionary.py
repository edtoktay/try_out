class ResponseMapper:
    def __init__(self, keys) -> None:
        self.keys = keys
        pass

    def map(self, rows: list):
        response = [dict(zip(self.keys, row))
                    for row in rows] if rows is not None and len(rows) > 0 else []
        if response is not None and len(response) == 1:
            return response[0]
        return response
