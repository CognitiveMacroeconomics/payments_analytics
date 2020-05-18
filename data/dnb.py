import sqlalchemy as sa


class TargetHandler:
    """
    Class to handle all connections and calls to the database.
    This might prove to be overkill, in which case it can be disregarded.
    """
    def __init__(self, db_uri, user, password, table_name, db_type="mssql"):
        self.conn_string = f"{db_type}://{user}/{password}@{db_uri}/{table_name}"
        self.engine = sa.create_engine(self.conn_string)

    
    def count(self):
        return 100_000
    
    def __repr__(self):
        return "<Database handler>"