import os
import uuid
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

class Config:
    # Usar la clave secreta desde la variable de entorno, con un fallback por seguridad
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "fallback-key-if-env-not-found")
    
    # Resto de la configuración...
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