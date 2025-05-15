import os
import json
import logging
from flask import Blueprint, session, redirect, render_template, flash, jsonify
from app.services.spotify import get_spotify_oauth, refresh_token, get_user_data
from config import Config

logger = logging.getLogger(__name__)
dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/dashboard')
def dashboard():
    """Muestra el dashboard con los datos de Spotify"""
    logger.debug("Dashboard endpoint accessed")
    
    # Verificar que tenemos las credenciales en la sesión
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
        client_file = os.path.join(Config.USERS_DATA_DIR, f"{client_id}.json")
        
        if os.path.exists(client_file):
            try:
                with open(client_file, 'r') as f:
                    token_info = json.load(f)
                
                # Verificar que el token corresponde a este client_id
                if token_info.get('client_id') != client_id:
                    logger.error(f"Token mismatch: {token_info.get('client_id')} != {client_id}")
                    flash("Error de consistencia en token. Por favor inicia sesión nuevamente.")
                    return redirect('/logout')
                
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
    sp_oauth = get_spotify_oauth(client_id, client_secret)
    
    if sp_oauth.is_token_expired(token_info):
        logger.debug("Token expired, refreshing...")
        try:
            token_info = refresh_token(token_info, client_id, client_secret)
            session['token_info'] = token_info
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            flash("Error renovando la sesión de Spotify")
            return redirect('/login')
    
    # Obtener datos de Spotify
    try:
        data = get_user_data(token_info['access_token'])
        user_display_name = token_info.get('display_name', 'Usuario')
        
        # Renderizar la plantilla con los datos
        return render_template(
            'dashboard/index.html',
            user_name=user_display_name,
            recent_tracks=data['recent_tracks'],
            top_artists=data['top_artists']
        )
    except Exception as e:
        logger.error(f"Error obteniendo datos de Spotify: {str(e)}")
        if "unauthorized" in str(e).lower():
            flash("Sesión expirada. Por favor inicia sesión nuevamente.")
            return redirect('/login')
        return f"Error obteniendo datos de Spotify: {str(e)}"

@dashboard_bp.route('/actualizar_datos', methods=['POST'])
def actualizar_datos():
    """Endpoint para actualizar los datos del dashboard vía AJAX"""
    logger.debug("Actualizar datos endpoint called")
    
    # Verificar que tenemos las credenciales y token en la sesión
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    token_info = session.get('token_info')
    
    if not client_id or not client_secret or not token_info:
        return jsonify({
            'error': 'No se encontraron credenciales o token'
        }), 401
    
    # Verificar si el token ha expirado
    sp_oauth = get_spotify_oauth(client_id, client_secret)
    
    if sp_oauth.is_token_expired(token_info):
        try:
            token_info = refresh_token(token_info, client_id, client_secret)
            session['token_info'] = token_info
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return jsonify({
                'error': 'Error renovando el token'
            }), 401
    
    # Obtener datos de Spotify
    try:
        data = get_user_data(token_info['access_token'])
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error obteniendo datos de Spotify: {str(e)}")
        return jsonify({
            'error': f'Error obteniendo datos: {str(e)}'
        }), 500