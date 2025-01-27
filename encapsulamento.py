import cx_Oracle
import pyodbc
import clickhouse_connect
import psycopg2

class DatabaseConnection:
    def __init__(self, db_type, host, port, user, password, database):
        self.db_type = db_type.lower()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            if self.db_type == "oracle":
                dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.database)
                self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=dsn)

            elif self.db_type == "sqlserver":
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.host},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.user};"
                    f"PWD={self.password};"
                )
                self.connection = pyodbc.connect(connection_string)

            elif self.db_type == "clickhouse":
                self.connection = clickhouse_connect.get_client(
                    host=self.host, port=self.port, username=self.user, password=self.password, database=self.database
                )

            elif self.db_type == "postgresql":
                self.connection = psycopg2.connect(
                    host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.database
                )

            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            print(f"Connected to {self.db_type} database successfully!")
        except Exception as e:
            print(f"Failed to connect to {self.db_type} database: {e}")

    def execute_query(self, query, params=None):
        try:
            if not self.connection:
                raise ConnectionError("No active database connection.")

            cursor = self.connection.cursor()
            cursor.execute(query, params or ())

            if query.strip().lower().startswith("select"):
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                self.connection.commit()
                cursor.close()
                print("Query executed successfully.")

        except Exception as e:
            print(f"Failed to execute query: {e}")

    def close(self):
        if self.connection:
            if self.db_type == "clickhouse":
                self.connection.disconnect()
            else:
                self.connection.close()
            print(f"Connection to {self.db_type} database closed.")

# Exemplo de uso:
if __name__ == "__main__":
    db = DatabaseConnection(
        db_type="postgresql",
        host="localhost",
        port=5432,
        user="postgres",
        password="yourpassword",
        database="yourdatabase"
    )

    db.connect()

    # Executar uma consulta (exemplo: selecionar dados)
    query = "SELECT * FROM your_table"
    results = db.execute_query(query)
    print(results)

    db.close()
