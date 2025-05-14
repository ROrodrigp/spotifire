import os
import uuid
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class Config:
    # Configuración general
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", str(uuid.uuid4()))
    PREFERRED_URL_SCHEME = 'https'
    
    # Directorios
    APP_DIR = os.path.abspath(os.path.dirname(__file__))
    USERS_DATA_DIR = os.path.join(APP_DIR, "data", "users_data")
    
    # Configuración de Spotify
    REDIRECT_BASE_URL = "https://52-203-107-89.nip.io"  
    REDIRECT_URI = f"{REDIRECT_BASE_URL}/callback"
    
    # Creación de directorios necesarios
    @staticmethod
    def init_app():
        os.makedirs(Config.USERS_DATA_DIR, exist_ok=True)