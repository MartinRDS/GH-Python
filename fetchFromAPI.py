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
    "Hora": "int64",
    "PML MDA ($/MWh)": "float64",
    "Componente Energia MDA ($/MWh)": "float64",
    "Componente Perdidas MDA ($/MWh)": "float64",
    "Componente Congestion MDA ($/MWh)": "int64",
    "Clave": "object",
    "Sistema": "object",
    "Centro De Control Regional": "object",
    "Zona De Carga": "object",
    "Nombre": "object",
    "Nivel De Tensión (Kv)": "float64",
    "Directamente Modelada": "object",
    "Indirectamente Modelada": "object",
    "Directamente Modelada 1": "object",
    "Indirectamente Modelada 1": "object",
    "Zona De Operación De Transmisión": "object",
    "Gerencia Regional De Transmisión": "object",
    "Zona De Distribución": "object",
    "Gerencia Divisional De Distribución": "object",
    "Clave De Entidad Federativa (Inegi)": "int64",
    "Entidad Federativa (Inegi)": "object",
    "Clave De Municipio (Inegi)": "int64",
    "Municipio (Inegi)": "object",
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
            "Centro De Control Regional" TEXT,
            "Zona De Carga" TEXT,
            "Nombre" TEXT,
            "Nivel De Tensión (Kv)" FLOAT,
            "Directamente Modelada" TEXT,
            "Indirectamente Modelada" TEXT,
            "Directamente Modelada 1" TEXT,
            "Indirectamente Modelada 1" TEXT,
            "Zona De Operación De Transmisión" TEXT,
            "Gerencia Regional De Transmisión" TEXT,
            "Zona De Distribución" TEXT,
            "Gerencia Divisional De Distribución" TEXT,
            "Clave De Entidad Federativa (Inegi)" INT,
            "Entidad Federativa (Inegi)" TEXT,
            "Clave De Municipio (Inegi)" INT,
            "Municipio (Inegi)" TEXT,
            "Region De Transmision" TEXT
        );
        """
        conn.execute(create_query)

# Create the table
table_name = "your_table_name"
create_table(engine, table_name)

# Insert data into the table (assuming df is your DataFrame with data)
df.to_sql(table_name, engine, if_exists='append', index=False)
