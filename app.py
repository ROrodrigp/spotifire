from flask import Flask, request, redirect, session, render_template, url_for, jsonify
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import uuid
import json
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()
logger.debug("Environment variables loaded")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")
logger.debug(f"Flask secret key set: {os.getenv('FLASK_SECRET_KEY')[:3]}...")

# Configurar Flask para generar URLs HTTPS
app.config['PREFERRED_URL_SCHEME'] = 'https'

# Configuración para almacenar datos de usuarios con rutas absolutas
APP_DIR = os.path.abspath(os.path.dirname(__file__))
USERS_DATA_DIR = os.path.join(APP_DIR, "users_data")
logger.debug(f"Setting users data directory to: {USERS_DATA_DIR}")

if not os.path.exists(USERS_DATA_DIR):
    logger.debug(f"Creating users data directory: {USERS_DATA_DIR}")
    try:
        os.makedirs(USERS_DATA_DIR)
        logger.debug("Directory created successfully")
    except Exception as e:
        logger.error(f"Error creating directory: {str(e)}")

# Establece la URL base para redirecciones seguras
# Reemplaza esta IP con tu IP elástica real
REDIRECT_BASE_URL = "https://52-203-107-89.nip.io"

# Define la URI de redirección completa
REDIRECT_URI = f"{REDIRECT_BASE_URL}/callback"
logger.debug(f"Redirect URI set to: {REDIRECT_URI}")

@app.route('/')
def index():
    """Página principal"""
    logger.debug("Root endpoint accessed")
    return '<h1>Bienvenido a Spotifire</h1><a href="/login">Conectar con Spotify</a>'

@app.route('/login')
def login():
    """Inicia el proceso de autenticación con Spotify"""
    logger.debug("Login endpoint accessed")
    
    # Generar un estado único para seguridad
    state = str(uuid.uuid4())
    session['state'] = state
    logger.debug(f"Generated state: {state[:8]}...")
    
    # Configurar OAuth con la URI de redirección segura
    sp_oauth = SpotifyOAuth(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played user-top-read",
        state=state
    )
    
    # Redirigir a Spotify para autenticación
    auth_url = sp_oauth.get_authorize_url()
    logger.debug(f"Redirecting to Spotify auth URL: {auth_url[:30]}...")
    return redirect(auth_url)

@app.route('/callback')
def callback():
    """Maneja la redirección desde Spotify después de la autenticación"""
    logger.debug("Callback endpoint called")
    
    # Verificar el estado para prevenir CSRF
    request_state = request.args.get('state')
    session_state = session.get('state')
    logger.debug(f"Request state: {request_state[:8] if request_state else None}")
    logger.debug(f"Session state: {session_state[:8] if session_state else None}")
    
    if request_state != session_state:
        logger.error(f"State mismatch: {request_state} vs {session_state}")
        return redirect('/')
    
    logger.debug("State verification passed")
    
    # Configurar OAuth con la misma URI de redirección
    sp_oauth = SpotifyOAuth(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played user-top-read"
    )
    
    # Obtener token usando el código de autorización
    code = request.args.get('code')
    logger.debug(f"Received authorization code: {code[:5] if code else None}...")
    
    try:
        token_info = sp_oauth.get_access_token(code)
        logger.debug("Successfully retrieved access token")
    except Exception as e:
        logger.error(f"Error getting access token: {str(e)}")
        return f"Error obtaining token: {str(e)}"
    
    # Guardar el token en la sesión
    session['token_info'] = token_info
    logger.debug("Token info saved to session")
    
    # Obtener el perfil del usuario para identificarlo
    try:
        sp = spotipy.Spotify(auth=token_info['access_token'])
        user_profile = sp.current_user()
        user_id = user_profile['id']
        logger.debug(f"Retrieved user profile. User ID: {user_id}")
    except Exception as e:
        logger.error(f"Error getting user profile: {str(e)}")
        return f"Error retrieving user profile: {str(e)}"
    
    # Verificar el directorio de datos
    cwd = os.getcwd()
    logger.debug(f"Current working directory: {cwd}")
    logger.debug(f"Users data directory: {USERS_DATA_DIR}")
    
    if not os.path.exists(USERS_DATA_DIR):
        logger.debug(f"Creating directory: {USERS_DATA_DIR}")
        try:
            os.makedirs(USERS_DATA_DIR)
        except Exception as e:
            logger.error(f"Error creating directory: {str(e)}")
            return f"Error creating directory: {str(e)}"
    
    # Guardar el token para este usuario
    user_data_path = os.path.join(USERS_DATA_DIR, f"{user_id}.json")
    logger.debug(f"Will save token to: {user_data_path}")
    
    token_info['user_id'] = user_id
    token_info['display_name'] = user_profile.get('display_name', user_id)
    token_info['last_updated'] = datetime.now().isoformat()
    
    try:
        with open(user_data_path, 'w') as f:
            json.dump(token_info, f)
        logger.debug(f"Token successfully saved to {user_data_path}")
    except Exception as e:
        logger.error(f"Error saving token: {str(e)}")
        return f"Error saving token: {str(e)}"
    
    # Redirigir a la página de dashboard
    logger.debug("Redirecting to dashboard")
    return redirect('/dashboard')

@app.route('/dashboard')
def dashboard():
    """Muestra el dashboard con los datos de Spotify"""
    logger.debug("Dashboard endpoint accessed")
    
    token_info = session.get('token_info')
    if not token_info:
        logger.warning("No token found in session, redirecting to login")
        return redirect('/login')
    
    # Verificar si el token ha expirado
    sp_oauth = SpotifyOAuth(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played user-top-read"
    )
    
    if sp_oauth.is_token_expired(token_info):
        logger.debug("Token expired, refreshing...")
        try:
            token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
            session['token_info'] = token_info
            logger.debug("Token refreshed successfully")
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return redirect('/login')
    
    # Inicializar cliente Spotify con el token
    sp = spotipy.Spotify(auth=token_info['access_token'])
    
    # Obtener datos
    try:
        logger.debug("Fetching recently played tracks")
        recent_tracks = sp.current_user_recently_played(limit=10)
        logger.debug(f"Retrieved {len(recent_tracks['items'])} tracks")
    except Exception as e:
        logger.error(f"Error fetching recent tracks: {str(e)}")
        return f"Error fetching tracks: {str(e)}"
    
    # Crear una versión simplificada para mostrar
    tracks = []
    for item in recent_tracks['items']:
        track = item['track']
        tracks.append({
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'played_at': item['played_at']
        })
    
    # Mostrar los datos en formato JSON
    return jsonify(tracks)

# Endpoint adicional para verificar el estado de autenticación
@app.route('/auth_status')
def auth_status():
    """Verifica el estado de la autenticación y tokens"""
    logger.debug("Auth status endpoint accessed")
    
    token_info = session.get('token_info')
    if not token_info:
        return jsonify({
            "authenticated": False,
            "message": "No hay sesión activa"
        })
    
    # Información básica sobre la autenticación
    return jsonify({
        "authenticated": True,
        "user_id": token_info.get('user_id', 'Desconocido'),
        "display_name": token_info.get('display_name', 'Desconocido'),
        "token_expires_in": token_info.get('expires_in', 0),
        "scopes": token_info.get('scope', '').split()
    })

# Endpoint adicional para probar la escritura en el sistema de archivos
@app.route('/test_filesystem')
def test_filesystem():
    """Prueba la capacidad de escribir en el sistema de archivos"""
    logger.debug("Filesystem test endpoint accessed")
    
    test_dir = os.path.abspath(USERS_DATA_DIR)
    test_file = os.path.join(test_dir, "test_write.json")
    
    results = {
        "current_directory": os.getcwd(),
        "test_directory": test_dir,
        "test_file": test_file,
        "dir_exists": os.path.exists(test_dir),
        "dir_is_writable": os.access(test_dir, os.W_OK) if os.path.exists(test_dir) else False,
        "write_test": False
    }
    
    # Intentar crear el directorio si no existe
    if not os.path.exists(test_dir):
        try:
            os.makedirs(test_dir)
            results["dir_created"] = True
        except Exception as e:
            results["dir_creation_error"] = str(e)
    
    # Intentar escribir un archivo de prueba
    try:
        with open(test_file, 'w') as f:
            f.write('{"test": "successful"}')
        results["write_test"] = True
    except Exception as e:
        results["write_error"] = str(e)
    
    # Verificar que el archivo se escribió correctamente
    if results["write_test"]:
        try:
            with open(test_file, 'r') as f:
                content = f.read()
            results["read_test"] = content == '{"test": "successful"}'
        except Exception as e:
            results["read_error"] = str(e)
    
    return jsonify(results)

if __name__ == '__main__':
    # Para desarrollo, puedes mantener esto
    app.run(host='0.0.0.0', port=8080, debug=True)
    
    # Para producción, comenta la línea anterior y descomenta esta:
    # app.run(host='0.0.0.0', port=8080, ssl_context='adhoc')