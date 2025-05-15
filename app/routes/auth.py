import os
import json
import uuid
import logging
from datetime import datetime
from flask import Blueprint, request, session, render_template, redirect, flash, url_for
import spotipy
from app.services.spotify import get_spotify_oauth, refresh_token
from config import Config

logger = logging.getLogger(__name__)
auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/')
def index():
    """Página principal que solicita las credenciales de Spotify"""
    logger.debug("Root endpoint accessed")
    return render_template('auth/login.html', redirect_uri=Config.REDIRECT_URI)

@auth_bp.route('/submit_credentials', methods=['POST'])
def submit_credentials():
    """Recibe y procesa las credenciales de Spotify del usuario"""
    logger.debug("Credentials submission received")
    
    # Limpiar la sesión actual para evitar mezclas de credenciales
    session.clear()
    logger.debug("Session cleared before processing new credentials")
    
    client_id = request.form.get('client_id')
    client_secret = request.form.get('client_secret')
    
    if not client_id or not client_secret:
        logger.error("Missing client ID or client secret")
        flash("Por favor proporciona tanto el Client ID como el Client Secret")
        return redirect('/')
    
    # Guardar credenciales en la sesión
    session['client_id'] = client_id
    session['client_secret'] = client_secret
    logger.debug(f"New credentials saved to session: client_id={client_id[:4]}...")
    
    # Verificar si ya tenemos un token para este usuario
    client_file = os.path.join(Config.USERS_DATA_DIR, f"{client_id}.json")
    
    if os.path.exists(client_file):
        # Si existe un archivo de datos para este client_id
        logger.debug(f"Found existing client file: {client_file}")
        try:
            with open(client_file, 'r') as f:
                token_info = json.load(f)
            
            # Verificación adicional para asegurar que el token pertenece al client_id correcto
            if token_info.get('client_id') != client_id:
                logger.warning(f"Token client_id mismatch: {token_info.get('client_id')} != {client_id}")
                flash("Inconsistencia en las credenciales guardadas. Iniciando nuevo flujo de autenticación.")
                return redirect('/login')
            
            # Verificar si el token ha expirado
            sp_oauth = get_spotify_oauth(client_id, client_secret)
            
            if sp_oauth.is_token_expired(token_info):
                try:
                    token_info = refresh_token(token_info, client_id, client_secret)
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

@auth_bp.route('/login')
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
    sp_oauth = get_spotify_oauth(client_id, client_secret, state)
    
    # Redirigir a Spotify para autenticación
    auth_url = sp_oauth.get_authorize_url()
    logger.debug(f"Redirecting to Spotify auth URL: {auth_url[:30]}...")
    return redirect(auth_url)

@auth_bp.route('/callback')
def callback():
    """Maneja la redirección desde Spotify después de la autenticación"""
    logger.debug("Callback endpoint called")
    
    # Recuperar credenciales de la sesión
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    
    if not client_id or not client_secret:
        logger.error("Missing client ID or client secret in session during callback")
        flash("Se perdieron las credenciales durante la autenticación. Por favor intenta conectarte nuevamente.")
        return redirect('/')
    
    # Verificar el estado para prevenir CSRF
    request_state = request.args.get('state')
    session_state = session.get('state')
    logger.debug(f"Request state: {request_state[:8] if request_state else None}")
    logger.debug(f"Session state: {session_state[:8] if session_state else None}")
    
    if request_state != session_state:
        logger.error(f"State mismatch: {request_state} vs {session_state}")
        flash("Error de verificación de estado. Por favor intenta conectarte nuevamente.")
        # Limpiar la sesión para evitar problemas futuros
        session.clear()
        return redirect('/')
    
    # Eliminar el estado de la sesión para evitar reutilización
    session.pop('state', None)
    
    logger.debug("State verification passed")
    
    # Configurar OAuth con las credenciales del usuario actual
    sp_oauth = get_spotify_oauth(client_id, client_secret)
    
    # Obtener token usando el código de autorización
    code = request.args.get('code')
    logger.debug(f"Received authorization code: {code[:5] if code else None}...")
    
    try:
        token_info = sp_oauth.get_access_token(code)
        logger.debug("Successfully retrieved access token")
    except Exception as e:
        logger.error(f"Error getting access token: {str(e)}")
        flash(f"Error obteniendo el token: {str(e)}")
        # Limpiar sesión para evitar problemas
        session.clear()
        return redirect('/')
    
    # Obtener el perfil del usuario para identificarlo
    try:
        sp = spotipy.Spotify(auth=token_info['access_token'])
        user_profile = sp.current_user()
        user_id = user_profile['id']
        display_name = user_profile.get('display_name', user_id)
        logger.debug(f"Retrieved user profile. User ID: {user_id}, Display Name: {display_name}")
        
        # Guardar la información del usuario en el token
        token_info['user_id'] = user_id
        token_info['display_name'] = display_name
    except Exception as e:
        logger.error(f"Error getting user profile: {str(e)}")
        flash(f"Error recuperando el perfil de usuario: {str(e)}")
        # Limpiar sesión
        session.clear()
        return redirect('/')
    
    # Añadir información adicional al token
    token_info['last_updated'] = datetime.now().isoformat()
    token_info['client_id'] = client_id
    token_info['client_secret'] = client_secret
    token_info['redirect_uri'] = Config.REDIRECT_URI
    
    # Guardar el token para este client_id
    client_file = os.path.join(Config.USERS_DATA_DIR, f"{client_id}.json")
    logger.debug(f"Will save token to: {client_file}")
    
    try:
        with open(client_file, 'w') as f:
            json.dump(token_info, f)
        logger.debug(f"Token successfully saved to {client_file}")
    except Exception as e:
        logger.error(f"Error saving token: {str(e)}")
        flash(f"Error guardando el token: {str(e)}")
        # Limpiar sesión
        session.clear()
        return redirect('/')
    
    # Guardar el token en la sesión
    session['token_info'] = token_info
    logger.debug("Token info saved to session")
    
    # Redirigir a la página de dashboard
    logger.debug(f"Redirecting to dashboard with client_id: {client_id}")
    return redirect('/dashboard')

@auth_bp.route('/logout')
def logout():
    """Cierra la sesión del usuario"""
    logger.debug("Logout endpoint accessed")
    
    # Limpiar datos de la sesión
    session.clear()
    
    flash("Has cerrado sesión correctamente")
    return redirect('/')