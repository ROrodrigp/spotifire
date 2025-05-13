from flask import Flask, request, redirect, session, render_template, url_for, jsonify
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import uuid
import json
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")

# Configurar Flask para generar URLs HTTPS
app.config['PREFERRED_URL_SCHEME'] = 'https'

# Configuración para almacenar datos de usuarios
USERS_DATA_DIR = "users_data"
if not os.path.exists(USERS_DATA_DIR):
    os.makedirs(USERS_DATA_DIR)

# Establece la URL base para redirecciones seguras
# Reemplaza esta IP con tu IP elástica real
REDIRECT_BASE_URL = "https://52-203-107-89.nip.io"

# Define la URI de redirección completa
REDIRECT_URI = f"{REDIRECT_BASE_URL}/callback"

@app.route('/')
def index():
    """Página principal"""
    return '<h1>Bienvenido a Spotifire</h1><a href="/login">Conectar con Spotify</a>'

@app.route('/login')
def login():
    """Inicia el proceso de autenticación con Spotify"""
    # Generar un estado único para seguridad
    state = str(uuid.uuid4())
    session['state'] = state
    
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
    return redirect(auth_url)

@app.route('/callback')
def callback():
    """Maneja la redirección desde Spotify después de la autenticación"""
    # Verificar el estado para prevenir CSRF
    if request.args.get('state') != session.get('state'):
        return redirect('/')
    
    # Configurar OAuth con la misma URI de redirección
    sp_oauth = SpotifyOAuth(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played user-top-read"
    )
    
    # Obtener token usando el código de autorización
    token_info = sp_oauth.get_access_token(request.args.get('code'))
    
    # Guardar el token en la sesión
    session['token_info'] = token_info
    
    # Obtener el perfil del usuario para identificarlo
    sp = spotipy.Spotify(auth=token_info['access_token'])
    user_profile = sp.current_user()
    user_id = user_profile['id']
    
    # Guardar el token para este usuario
    user_data_path = os.path.join(USERS_DATA_DIR, f"{user_id}.json")
    token_info['user_id'] = user_id
    token_info['display_name'] = user_profile.get('display_name', user_id)
    token_info['last_updated'] = datetime.now().isoformat()
    
    with open(user_data_path, 'w') as f:
        json.dump(token_info, f)
    
    # Redirigir a la página de dashboard
    return redirect('/dashboard')

@app.route('/dashboard')
def dashboard():
    """Muestra el dashboard con los datos de Spotify"""
    token_info = session.get('token_info')
    if not token_info:
        return redirect('/login')
    
    # Verificar si el token ha expirado
    sp_oauth = SpotifyOAuth(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played user-top-read"
    )
    
    if sp_oauth.is_token_expired(token_info):
        token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
        session['token_info'] = token_info
    
    # Inicializar cliente Spotify con el token
    sp = spotipy.Spotify(auth=token_info['access_token'])
    
    # Obtener datos
    recent_tracks = sp.current_user_recently_played(limit=10)
    
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

if __name__ == '__main__':
    # Para desarrollo, puedes mantener esto
    app.run(host='0.0.0.0', port=8080, debug=True)
    
    # Para producción, comenta la línea anterior y descomenta esta:
    # app.run(host='0.0.0.0', port=8080, ssl_context='adhoc')