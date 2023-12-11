import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Define your database credentials
db_settings = {
    "dbname": "your_database_name",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",  # or "127.0.0.1"
    "port": "5432"        # default PostgreSQL port
}

# Create a connection to the database
conn_string = f"postgresql+psycopg2://{db_settings['user']}:{db_settings['password']}@{db_settings['host']}:{db_settings['port']}/{db_settings['dbname']}"
engine = create_engine(conn_string)

# Define the schema as a dictionary (from previous steps)
schema = {
    "Fecha": "datetime64[ns]",
    # ... (other column definitions) ...
    "Region De Transmision": "object"
}

# Create an empty DataFrame with the specified schema
df = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in schema.items()})

# Function to create the table if it doesn't exist
def create_table(engine, table_name):
    with engine.connect() as conn:
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Fecha TIMESTAMP,
            Hora INT,
            "PML MDA ($/MWh)" FLOAT,
            "Componente Energia MDA ($/MWh)" FLOAT,
            "Componente Perdidas MDA ($/MWh)" FLOAT,
            "Componente Congestion MDA ($/MWh)" INT,
            Clave TEXT,
            Sistema TEXT,
            -- ... (other column definitions) ...
            "Region De Transmision" TEXT
        );
        """
        conn.execute(create_query)

# Create the table
table_name = "your_table_name"
create_table(engine, table_name)

# Insert data into the table (assuming df is your DataFrame with data)
df.to_sql(table_name, engine, if_exists='append', index=False)
