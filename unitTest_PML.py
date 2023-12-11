
import requests

nodo = '07ACU-115'
mercado = "MDA" 
date = pd.to_datetime('2021-01-01')
sistema = "BCA"
nodos = pd.read_excel('CatalogoNodosP.xlsx', skiprows=1)
    
    
url = f"https://ws01.cenace.gob.mx:8082/SWPML/SIM/{sistema}/{mercado}/{nodo}/{date.strftime('%Y/%m/%d')}/{date.strftime('%Y/%m/%d')}/JSON"

response = requests.get(url) 
if response.status_code != 200:
    logging.error(f"API request failed for {nodo}, {mercado}, {date}: Status {response.status_code}")

json_response =  response.json()

# Extract data
data = []
for zone in json_response.get('Resultados', []):
    clv_nodo = zone.get('clv_nodo')
    for value in zone.get('Valores', []):
        value['Clave'] = clv_nodo
        data.append(value)


df = pd.DataFrame(data)
if 'fecha' not in df.columns:
    logging.error(f"'fecha' column not found in API response for nodo: {nodo}, mercado: {mercado}, date: {date}, sistema: {sistema}")

# Show last date in fecha
df['fecha'].max()

# Ensure 'fecha' is in date format
df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d', errors='coerce')

column_rename_map = {
    "fecha": "Fecha", "hora": "Hora", "pml": f"PML {mercado} ($/MWh)", 
    "pml_ene": f"Componente Energia {mercado} ($/MWh)", 
    "pml_per": f"Componente Perdidas {mercado} ($/MWh)", 
    "pml_cng": f"Componente Congestion {mercado} ($/MWh)"
}

# Mapping of columns to the conversion function (float)
column_convert_map = {
    f"PML {mercado} ($/MWh)": float,
    f"Componente Energia {mercado} ($/MWh)": float,
    f"Componente Perdidas {mercado} ($/MWh)": float,
    f"Componente Congestion {mercado} ($/MWh)": float
}

# Rename the columns first
df.rename(columns=column_rename_map, inplace=True)

for column in column_convert_map:
    try:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    except Exception as e:
        logging.error(f"Error converting column {column}: {e}")


nodos.columns = nodos.columns.str.title().str.replace('.', ' ', regex=False)
df = pd.merge(df, nodos, how='left', left_on=['Clave', 'Sistema'], right_on=['Clave', 'Sistema'])

