import os
import json
import logging
from datetime import datetime
from flask import Blueprint, session, redirect, render_template, flash, jsonify
import spotipy
from app.services.spotify import get_spotify_oauth, refresh_token, get_user_data, validate_token, load_user_token
from app.services.insights import InsightsService
from config import Config

logger = logging.getLogger(__name__)
dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/dashboard')
def dashboard():
    """Muestra el dashboard con los datos de Spotify e insights personalizados"""
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
        
        # Intentar cargar el token desde el archivo
        token_info = load_user_token(client_id)
        
        if not token_info:
            logger.warning("No valid token found, redirecting to login")
            flash("Se necesita iniciar sesión en Spotify")
            return redirect('/login')
        
        # Guardar el token en la sesión
        session['token_info'] = token_info
        logger.debug("Loaded token from file and saved to session")
    
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
        user_id = token_info.get('user_id')
        
        # Obtener insights personalizados
        insights_service = InsightsService(user_id)
        dashboard_insights = insights_service.get_dashboard_insights()
        next_milestone = insights_service.get_next_milestone()
        
        # Renderizar la plantilla con los datos
        return render_template(
            'dashboard/index.html',
            user_name=user_display_name,
            recent_tracks=data['recent_tracks'],
            top_artists=data['top_artists'],
            insights=dashboard_insights,
            next_milestone=next_milestone
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
    
    # Verificar que el token pertenece al client_id actual
    if not validate_token(token_info, client_id):
        return jsonify({
            'error': 'El token no es válido para este cliente'
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
        user_id = token_info.get('user_id')
        
        # Obtener insights actualizados
        insights_service = InsightsService(user_id)
        dashboard_insights = insights_service.get_dashboard_insights()
        next_milestone = insights_service.get_next_milestone()
        
        return jsonify({
            'recent_tracks': data['recent_tracks'],
            'top_artists': data['top_artists'],
            'insights': dashboard_insights,
            'next_milestone': next_milestone
        })
    except Exception as e:
        logger.error(f"Error obteniendo datos de Spotify: {str(e)}")
        return jsonify({
            'error': f'Error obteniendo datos: {str(e)}'
        }), 500

@dashboard_bp.route('/insights')
def insights_page():
    """Página dedicada para insights detallados (placeholder para futura implementación)"""
    logger.debug("Insights page accessed")
    
    # Verificar autenticación
    client_id = session.get('client_id')
    if not client_id:
        flash("Se requiere iniciar sesión")
        return redirect('/')
    
    # Por ahora, redirigir al dashboard principal
    # En el futuro, esta será una página separada con análisis detallados
    flash("Los insights detallados estarán disponibles próximamente")
    return redirect('/dashboard')