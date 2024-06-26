class ResponseMapper:

    def map(self, rows):
        resp = [dict(zip(self.keys, rows[i])) for i in range(
            len(rows))] if rows is not None and len(rows) > 0 else None
        if resp is not None and len(resp) == 1:
            return resp[0]
        return resp

    def __init__(self, keys):
        self.keys = keys
