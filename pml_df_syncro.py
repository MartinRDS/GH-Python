from prisma import Prisma
from datetime import datetime, timedelta
import pandas as pd
import requests
from lxml import etree
import logging

# Initialize Prisma Client
client = Prisma()

logging.basicConfig(level=logging.INFO)

#### Fetch data
def fetch_data_from_db(nodo, mercado, date):
    try:
        logging.info(f"Fetching data from DB for nodo: {nodo} and mercado: {mercado} on {date}")
        
        # Synchronous call to Prisma to fetch data
        records = client.cenace_mecp_mda_pml.find_many(
            where={
                'Clave': nodo,
                'Fecha': date,
                'Mercado': mercado,
            },
            execute=True,
        )
        
        if records:
            logging.info(f"Found {len(records)} records in DB for nodo: {nodo} and mercado: {mercado} on {date}")
        else:
            logging.info(f"No records found in DB for nodo: {nodo} and mercado: {mercado} on {date}")

        return records

    except Exception as e:
        logging.error(f"Error fetching from DB: {e}")
        return []
    
##### Save to db
    
def save_to_db(data, mercado):
    if not data:
        logging.info(f"No data provided to save for mercado: {mercado}")
        return

    try:
        logging.info(f"Preparing to save {len(data)} records to the database for mercado: {mercado}")

        name_mapping = {
            'SISTEMA': 'Sistema',
            'CENTRO DE CONTROL REGIONAL': 'CentroDeControlRegional',
            'ZONA DE CARGA': 'ZonaDeCarga',
            'NOMBRE': 'Nombre',
            'NIVEL DE TENSIÓN (kV)': 'NivelDeTension_kV',
            'DIRECTAMENTE MODELADA': 'DirectamenteModelada',
            'INDIRECTAMENTE MODELADA': 'IndirectamenteModelada',
            'DIRECTAMENTE MODELADA.1': 'DirectamenteModelada1',
            'INDIRECTAMENTE MODELADA.1': 'IndirectamenteModelada1',
            'ZONA DE OPERACIÓN DE TRANSMISIÓN': 'ZonaDeOperacionDeTransmision',
            'GERENCIA REGIONAL DE TRANSMISIÓN': 'GerenciaRegionalDeTransmision',
            'ZONA DE DISTRIBUCIÓN': 'ZonaDeDistribucion',
            'GERENCIA DIVISIONAL DE DISTRIBUCIÓN': 'GerenciaDivisionalDeDistribucion',
            'CLAVE DE ENTIDAD FEDERATIVA (INEGI)': 'ClaveDeEntidadFederativa_INEGI',
            'ENTIDAD FEDERATIVA (INEGI)': 'EntidadFederativa_INEGI',
            'CLAVE DE MUNICIPIO (INEGI)': 'ClaveDeMunicipio_INEGI',
            'MUNICIPIO (INEGI)': 'Municipio_INEGI',
            'REGION DE TRANSMISION': 'RegionDeTransmision'
        }

        # Convert and rename records based on the mapping
        for record in data:
            for original_key, new_key in name_mapping.items():
                if original_key in record:
                    record[new_key] = record.pop(original_key)
            
            # Apply type conversions
            for key, value in record.items():
                if key in ['NivelDeTension_kV', 'PML_MDA', 'ComponenteEnergia_MDA', 'ComponentePerdidas_MDA', 'ComponenteCongestion_MDA']:
                    record[key] = float(value) if value is not None else None
                elif key in ['Hora', 'ClaveDeEntidadFederativa_INEGI', 'ClaveDeMunicipio_INEGI']:
                    record[key] = int(value) if value is not None else None
                elif key == 'Fecha':
                    record[key] = datetime.strptime(value, '%Y-%m-%d') if isinstance(value, str) else value
        
        # Synchronous call to Prisma to save data
        client.cenace_mecp_mda_pml.create_many(
            data=[CenaceMecpMdaPml.parse_obj(record) for record in data],
            skip_duplicates=True,
            execute=True,
        )
        logging.info(f"Successfully saved {len(data)} records to the database for mercado: {mercado}")

    except Exception as e:
        logging.error(f"Failed to save to database for mercado: {mercado}: {e}")
        
        
#### fetch from api
def fetch_data_from_api(nodo, mercado, date, sistema, nodos_dict):
    sistema_value = sistema
    url = f"https://ws01.cenace.gob.mx:8082/SWPML/SIM/{sistema_value}/{mercado}/{nodo}/{date.strftime('%Y/%m/%d')}/{date.strftime('%Y/%m/%d')}/XML"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        doc = etree.fromstring(response.content)
        data = []
        # Process the XML and extract data as before
        return data
    except Exception as e:
        logging.error(f"Error fetching from API: {e}")
        return []

#### main
def main():
    mercado = 'MDA'
    nodos = pd.read_excel('CatalogoNodosP.xlsx', skiprows=1)
    nodos_dict = nodos.set_index('CLAVE').to_dict(orient='index')

    for nodo, attributes in nodos_dict.items():
        sistema = attributes['SISTEMA']  # Assuming 'SISTEMA' is in the attributes list
        missing_dates = []
        start_date = datetime(2018, 1, 1)
        end_date = datetime.today() - timedelta(days=14)
        date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

        for date in date_range:
            if not fetch_data_from_db(nodo, mercado, date.strftime('%Y-%m-%d')):
                missing_dates.append(date.strftime('%Y-%m-%d'))

        if missing_dates:
            for date in missing_dates:
                fetched_data = fetch_data_from_api(nodo, mercado, datetime.strptime(date, '%Y-%m-%d'), sistema, nodos_dict)
                if fetched_data:
                    save_to_db(fetched_data, mercado)

if __name__ == "__main__":
    main()
    
mercado = 'MDA'
nodos = pd.read_excel('CatalogoNodosP.xlsx', skiprows=1)
nodos_dict = nodos.set_index('CLAVE').to_dict(orient='index')

for nodo, attributes in nodos_dict.items():
    sistema = attributes['SISTEMA']  # Assuming 'SISTEMA' is in the attributes list
    missing_dates = []
    start_date = datetime(2018, 1, 1)
    end_date = datetime.today() - timedelta(days=14)
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    for date in date_range:
        if not fetch_data_from_db(nodo, mercado, date):
            missing_dates.append(date.strftime('%Y-%m-%d'))

    if missing_dates:
        for date in missing_dates:
            fetched_data = fetch_data_from_api(nodo, mercado, datetime.strptime(date, '%Y-%m-%d'), sistema, nodos_dict)
            if fetched_data:
                save_to_db(fetched_data, mercado)