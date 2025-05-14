from flask import Flask
from flask.sessions import FileSystemSessionInterface
import os
import logging
import sys
from config import Config

# Configuración de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CustomFileSystemSessionInterface(FileSystemSessionInterface):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_signer = True

def create_app(config_class=Config):
    app = Flask(__name__, template_folder='../templates', static_folder='../static')
    app.config.from_object(config_class)
    
    # Configurar sesiones basadas en archivos
    session_dir = os.path.join(config_class.APP_DIR, "data", "sessions")
    os.makedirs(session_dir, exist_ok=True)
    app.session_interface = CustomFileSystemSessionInterface(
        session_dir, 
        app.secret_key
    )
    
    # Asegurar que existen los directorios necesarios
    Config.init_app()
    
    # Registrar blueprints
    from app.routes.auth import auth_bp
    from app.routes.dashboard import dashboard_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    
    logger.debug("Aplicación inicializada correctamente")
    
    return app