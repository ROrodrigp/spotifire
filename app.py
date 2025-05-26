from flask import Flask, request, redirect, session, render_template, url_for, jsonify, flash
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
app.secret_key = os.getenv("FLASK_SECRET_KEY", str(uuid.uuid4()))
logger.debug(f"Flask secret key set: {app.secret_key[:3]}...")

# Configurar Flask para generar URLs HTTPS
app.config['PREFERRED_URL_SCHEME'] = 'https'

# Configuración para almacenar datos de usuarios con rutas absolutas
APP_DIR = os.path.abspath(os.path.dirname(__file__))
USERS_DATA_DIR = os.path.join(APP_DIR, "users_data")
logger.debug(f"Setting users data directory: {USERS_DATA_DIR}")

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
    """Página principal que solicita las credenciales de Spotify"""
    logger.debug("Root endpoint accessed")
    return render_template('index.html', redirect_uri=REDIRECT_URI)

@app.route('/submit_credentials', methods=['POST'])
def submit_credentials():
    """Recibe y procesa las credenciales de Spotify del usuario"""
    logger.debug("Credentials submission received")
    
    client_id = request.form.get('client_id')
    client_secret = request.form.get('client_secret')
    
    if not client_id or not client_secret:
        logger.error("Missing client ID or client secret")
        flash("Por favor proporciona tanto el Client ID como el Client Secret")
        return redirect('/')
    
    # Guardar credenciales en la sesión
    session['client_id'] = client_id
    session['client_secret'] = client_secret
    logger.debug("Credentials saved to session")
    
    # Verificar si ya tenemos un token para este usuario
    client_file = os.path.join(USERS_DATA_DIR, f"{client_id}.json")
    
    if os.path.exists(client_file):
        # Si existe un archivo de datos para este client_id
        logger.debug(f"Found existing client file: {client_file}")
        try:
            with open(client_file, 'r') as f:
                token_info = json.load(f)
            
            # Verificar si el token ha expirado
            sp_oauth = SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=REDIRECT_URI,
                scope="user-library-read user-read-recently-played user-top-read playlist-read-private playlist-read-collaborative user-follow-read"
            )
            
            if sp_oauth.is_token_expired(token_info):
                logger.debug("Existing token has expired, refreshing...")
                try:
                    # Guardar los campos adicionales antes de refrescar
                    saved_client_id = token_info.get('client_id')
                    saved_client_secret = token_info.get('client_secret')
                    saved_redirect_uri = token_info.get('redirect_uri')
                    saved_user_id = token_info.get('user_id')
                    saved_display_name = token_info.get('display_name')
                    
                    token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
                    
                    # Restaurar los campos adicionales
                    token_info['client_id'] = saved_client_id
                    token_info['client_secret'] = saved_client_secret
                    token_info['redirect_uri'] = saved_redirect_uri
                    token_info['user_id'] = saved_user_id
                    token_info['display_name'] = saved_display_name
                    
                    # Actualizar el archivo con el nuevo token
                    token_info['last_updated'] = datetime.now().isoformat()
                    with open(client_file, 'w') as f:
                        json.dump(token_info, f)
                    logger.debug("Token refreshed and saved")
                except Exception as e:
                    logger.error(f"Error refreshing token: {str(e)}")
                    # Si falla la renovación, necesitamos un nuevo token
                    return redirect('/login')
            
            # Guardar el token en la sesión
            session['token_info'] = token_info
            logger.debug("Valid token loaded from file and saved to session")
            
            # Redirigir al dashboard
            return redirect('/dashboard')
        
        except Exception as e:
            logger.error(f"Error processing existing token file: {str(e)}")
            # Si hay cualquier error con el archivo existente, iniciamos nuevo flujo
            return redirect('/login')
    else:
        # No tenemos un token, iniciar el flujo de autenticación
        logger.debug("No existing token found, redirecting to login")
        return redirect('/login')

@app.route('/login')
def login():
    """Inicia el proceso de autenticación con Spotify"""
    logger.debug("Login endpoint accessed")
    
    # Verificar que tenemos las credenciales en la sesión
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    
    if not client_id or not client_secret:
        logger.error("Missing client ID or client secret in session")
        flash("Se requieren las credenciales de Spotify")
        return redirect('/')
    
    # Generar un estado único para seguridad
    state = str(uuid.uuid4())
    session['state'] = state
    logger.debug(f"Generated state: {state[:8]}...")
    
    # Configurar OAuth con las credenciales del usuario
    sp_oauth = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=REDIRECT_URI,
        scope="user-library-read user-read-recently-played user-top-read playlist-read-private playlist-read-collaborative user-follow-read",
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
    
    # Recuperar credenciales de la sesión
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    
    if not client_id or not client_secret:
        logger.error("Missing client ID or client secret in session during callback")
        flash("Se perdieron las credenciales durante la autenticación")
        return redirect('/')
    
    # Verificar el estado para prevenir CSRF
    request_state = request.args.get('state')
    session_state = session.get('state')
    logger.debug(f"Request state: {request_state[:8] if request_state else None}")
    logger.debug(f"Session state: {session_state[:8] if session_state else None}")
    
    if request_state != session_state:
        logger.error(f"State mismatch: {request_state} vs {session_state}")
        return redirect('/')
    
    logger.debug("State verification passed")
    
    # Configurar OAuth con las credenciales del usuario
    sp_oauth = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=REDIRECT_URI,
        scope="user-library-read user-read-recently-played user-top-read playlist-read-private playlist-read-collaborative user-follow-read"
    )
    
    # Obtener token usando el código de autorización
    code = request.args.get('code')
    logger.debug(f"Received authorization code: {code[:5] if code else None}...")
    
    try:
        token_info = sp_oauth.get_access_token(code)
        logger.debug("Successfully retrieved access token")
    except Exception as e:
        logger.error(f"Error getting access token: {str(e)}")
        flash(f"Error obteniendo el token: {str(e)}")
        return redirect('/')
    
    # Guardar el token en la sesión
    session['token_info'] = token_info
    logger.debug("Token info saved to session")
    
    # Obtener el perfil del usuario para identificarlo
    try:
        sp = spotipy.Spotify(auth=token_info['access_token'])
        user_profile = sp.current_user()
        user_id = user_profile['id']
        logger.debug(f"Retrieved user profile. User ID: {user_id}")
        
        # Guardar la información del usuario en el token
        token_info['user_id'] = user_id
        token_info['display_name'] = user_profile.get('display_name', user_id)
    except Exception as e:
        logger.error(f"Error getting user profile: {str(e)}")
        flash(f"Error recuperando el perfil de usuario: {str(e)}")
        return redirect('/')
    
    # Guardar el token para este client_id
    client_file = os.path.join(USERS_DATA_DIR, f"{client_id}.json")
    logger.debug(f"Will save token to: {client_file}")
    
    token_info['last_updated'] = datetime.now().isoformat()
    
    # Añadir credenciales al token para el servicio periódico
    token_info['client_id'] = client_id
    token_info['client_secret'] = client_secret
    token_info['redirect_uri'] = REDIRECT_URI
    
    try:
        with open(client_file, 'w') as f:
            json.dump(token_info, f)
        logger.debug(f"Token successfully saved to {client_file}")
    except Exception as e:
        logger.error(f"Error saving token: {str(e)}")
        flash(f"Error guardando el token: {str(e)}")
        return redirect('/')
    
    # Redirigir a la página de dashboard
    logger.debug("Redirecting to dashboard")
    return redirect('/dashboard')

@app.route('/dashboard')
def dashboard():
    """Muestra el dashboard con los datos de Spotify"""
    logger.debug("Dashboard endpoint accessed")
    
    # Verificar que tenemos las credenciales y token en la sesión
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    token_info = session.get('token_info')
    
    if not client_id or not client_secret:
        logger.warning("No credentials found in session, redirecting to index")
        flash("Se requieren credenciales de Spotify")
        return redirect('/')
    
    if not token_info:
        logger.warning("No token found in session, checking if we have it saved")
        
        # Verificar si existe un token guardado para este client_id
        client_file = os.path.join(USERS_DATA_DIR, f"{client_id}.json")
        
        if os.path.exists(client_file):
            try:
                with open(client_file, 'r') as f:
                    token_info = json.load(f)
                session['token_info'] = token_info
                logger.debug("Loaded token from file")
            except Exception as e:
                logger.error(f"Error loading token from file: {str(e)}")
                flash("Se necesita iniciar sesión en Spotify")
                return redirect('/login')
        else:
            logger.warning("No saved token found, redirecting to login")
            flash("Se necesita iniciar sesión en Spotify")
            return redirect('/login')
    
    # Verificar si el token ha expirado
    sp_oauth = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=REDIRECT_URI,
        scope="user-library-read user-read-recently-played user-top-read playlist-read-private playlist-read-collaborative user-follow-read"
    )
    
    if sp_oauth.is_token_expired(token_info):
        logger.debug("Token expired, refreshing...")
        try:
            # Guardar los campos adicionales antes de refrescar
            saved_client_id = token_info.get('client_id')
            saved_client_secret = token_info.get('client_secret')
            saved_redirect_uri = token_info.get('redirect_uri')
            saved_user_id = token_info.get('user_id')
            saved_display_name = token_info.get('display_name')
            
            # Refrescar el token
            token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
            
            # Restaurar los campos adicionales
            token_info['client_id'] = saved_client_id
            token_info['client_secret'] = saved_client_secret
            token_info['redirect_uri'] = saved_redirect_uri
            token_info['user_id'] = saved_user_id
            token_info['display_name'] = saved_display_name
            token_info['last_updated'] = datetime.now().isoformat()
            
            session['token_info'] = token_info
            
            # Actualizar el archivo con el token renovado
            client_file = os.path.join(USERS_DATA_DIR, f"{client_id}.json")
            with open(client_file, 'w') as f:
                json.dump(token_info, f)
            
            logger.debug("Token refreshed and saved successfully")
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            flash("Error renovando la sesión de Spotify")
            return redirect('/login')
    
    # Inicializar cliente Spotify con el token
    sp = spotipy.Spotify(auth=token_info['access_token'])
    
    # Obtener datos
    try:
        logger.debug("Fetching recently played tracks")
        recent_tracks = sp.current_user_recently_played(limit=10)
        logger.debug(f"Retrieved {len(recent_tracks['items'])} tracks")
        
        # Obtener los artistas principales
        logger.debug("Fetching top artists")
        top_artists = sp.current_user_top_artists(limit=5, time_range='short_term')
        logger.debug(f"Retrieved {len(top_artists['items'])} artists")
    except Exception as e:
        logger.error(f"Error fetching Spotify data: {str(e)}")
        if "unauthorized" in str(e).lower():
            flash("Sesión expirada. Por favor inicia sesión nuevamente.")
            return redirect('/login')
        return f"Error obteniendo datos de Spotify: {str(e)}"
    
    # Crear versiones simplificadas para mostrar
    tracks = []
    for item in recent_tracks['items']:
        track = item['track']
        played_at = item['played_at']
        # Formato simplificado de fecha (solo fecha)
        played_date = played_at.split('T')[0] if 'T' in played_at else played_at
        
        tracks.append({
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'played_at': played_date
        })
    
    artists = []
    for artist in top_artists['items']:
        # Asegurar que la popularidad se maneje correctamente
        popularity = artist.get('popularity', 50)
        # Redondear a la decena más cercana para compatibilidad con clases CSS
        popularity_rounded = int(popularity / 10) * 10
        
        artists.append({
            'name': artist['name'],
            'popularity': popularity_rounded,
            'genres': artist['genres'] if 'genres' in artist and artist['genres'] else []
        })
    
    # Preparar los datos para la plantilla
    user_display_name = token_info.get('display_name', 'Usuario')
    
    # Renderizar la plantilla con los datos
    return render_template(
        'dashboard.html',
        user_name=user_display_name,
        recent_tracks=tracks,
        top_artists=artists
    )

@app.route('/logout')
def logout():
    """Cierra la sesión del usuario"""
    logger.debug("Logout endpoint accessed")
    
    # Limpiar datos de la sesión
    session.pop('client_id', None)
    session.pop('client_secret', None)
    session.pop('token_info', None)
    session.pop('state', None)
    
    flash("Has cerrado sesión correctamente")
    return redirect('/')

if __name__ == '__main__':
    # Asegurar que el directorio de plantillas existe
    templates_dir = os.path.join(APP_DIR, "templates")
    if not os.path.exists(templates_dir):
        logger.debug(f"Creating templates directory: {templates_dir}")
        try:
            os.makedirs(templates_dir)
        except Exception as e:
            logger.error(f"Error creating templates directory: {str(e)}")
    
    # Para desarrollo, puedes mantener esto
    app.run(host='0.0.0.0', port=8080, debug=True)
    
    # Para producción, comenta la línea anterior y descomenta esta:
    # app.run(host='0.0.0.0', port=8080, ssl_context='adhoc')