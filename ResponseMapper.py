class ResponseMapper:

    def map(self, rows):
        return [dict(zip(self.keys, rows[i])) for i in range(len(rows))] if rows is not None and len(rows) > 0 else None

    def __init__(self, keys):
        self.keys = keys
