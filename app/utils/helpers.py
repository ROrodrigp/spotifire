import logging
import os
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def save_json_data(data, filename, directory):
    """
    Guarda datos en formato JSON en un archivo
    
    Args:
        data: Los datos a guardar
        filename: Nombre del archivo
        directory: Directorio donde guardar el archivo
    
    Returns:
        bool: True si se guardó correctamente, False en caso contrario
    """
    try:
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.debug(f"Datos guardados correctamente en {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error guardando datos en {filename}: {str(e)}")
        return False

def load_json_data(filename, directory):
    """
    Carga datos en formato JSON desde un archivo
    
    Args:
        filename: Nombre del archivo
        directory: Directorio donde se encuentra el archivo
    
    Returns:
        dict: Los datos cargados o None si hay error
    """
    try:
        filepath = os.path.join(directory, filename)
        
        if not os.path.exists(filepath):
            logger.warning(f"El archivo {filepath} no existe")
            return None
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        logger.debug(f"Datos cargados correctamente desde {filepath}")
        return data
    except Exception as e:
        logger.error(f"Error cargando datos desde {filename}: {str(e)}")
        return None

def format_date(date_string, format_in='%Y-%m-%dT%H:%M:%S', format_out='%Y-%m-%d'):
    """
    Formatea una fecha de un formato a otro
    
    Args:
        date_string: La fecha en formato string
        format_in: El formato de entrada
        format_out: El formato de salida
    
    Returns:
        str: La fecha formateada o la original si hay error
    """
    try:
        date_obj = datetime.strptime(date_string.split('.')[0], format_in)
        return date_obj.strftime(format_out)
    except Exception as e:
        logger.error(f"Error formateando fecha {date_string}: {str(e)}")
        return date_string