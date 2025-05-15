from flask import Flask
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

def create_app(config_class=Config):
    app = Flask(__name__, template_folder='../templates', static_folder='../static')
    app.config.from_object(config_class)
    
    # Configuración de sesión basada en sistema de archivos
    app.config['SESSION_FILE_DIR'] = os.path.join(config_class.APP_DIR, "data", "sessions")
    app.config['SESSION_USE_SIGNER'] = True
    app.config['SESSION_PERMANENT'] = False
    app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # 1 día en segundos
    
    # Crear directorio de sesiones
    os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)
    
    # Asegurar que existen los directorios necesarios
    Config.init_app()
    
    # Registrar blueprints
    from app.routes.auth import auth_bp
    from app.routes.dashboard import dashboard_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    
    logger.debug("Aplicación inicializada correctamente")
    
    return app