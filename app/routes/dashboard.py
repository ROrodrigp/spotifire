import os
import json
import logging
from datetime import datetime
from flask import Blueprint, session, redirect, render_template, flash, jsonify, request
import spotipy
from app.services.spotify import get_spotify_oauth, refresh_token, get_user_data, validate_token, load_user_token
from app.services.athena import AthenaInsightsService
from app.services.music_profiles import MusicProfileService
from config import Config

logger = logging.getLogger(__name__)
dashboard_bp = Blueprint('dashboard', __name__)

# Inicializar servicios una vez al cargar el módulo
try:
    athena_service = AthenaInsightsService(
        database_name='spotify_analytics',
        table_name='user_tracks',
        s3_output_bucket='itam-analytics-ragp'
    )
    logger.info("Servicio de Athena inicializado correctamente")
except Exception as e:
    logger.error(f"Error inicializando servicio de Athena: {str(e)}")
    athena_service = None

try:
    music_profile_service = MusicProfileService()
    logger.info("Servicio de perfiles musicales inicializado correctamente")
except Exception as e:
    logger.error(f"Error inicializando servicio de perfiles musicales: {str(e)}")
    music_profile_service = None

def _has_valid_insights(insights):
    """
    Verifica si los insights contienen datos válidos y útiles.
    
    Args:
        insights: Diccionario con los insights del usuario
        
    Returns:
        bool: True si hay datos útiles, False en caso contrario
    """
    if not insights or not isinstance(insights, dict):
        return False
    
    # Verificar si hay datos en al menos una categoría
    has_top_artists = bool(insights.get('top_artists', []))
    
    # Verificar patrones diarios
    daily_pattern = insights.get('daily_pattern', [])
    has_daily_data = any(item.get('play_count', 0) > 0 for item in daily_pattern)
    
    # Verificar patrones semanales
    weekly_pattern = insights.get('weekly_pattern', {})
    weekday_plays = weekly_pattern.get('weekday', {}).get('play_count', 0)
    weekend_plays = weekly_pattern.get('weekend', {}).get('play_count', 0)
    has_weekly_data = weekday_plays > 0 or weekend_plays > 0
    
    # Verificar distribución de popularidad
    pop_dist = insights.get('popularity_distribution', {})
    has_popularity_data = pop_dist.get('summary', {}).get('total_plays', 0) > 0
    
    # Se considera que hay insights válidos si hay datos en cualquier categoría
    return has_top_artists or has_daily_data or has_weekly_data or has_popularity_data

def _sanitize_insights(insights):
    """
    Limpia y valida la estructura de insights para evitar errores en el template.
    
    Args:
        insights: Diccionario con insights (puede estar incompleto)
        
    Returns:
        Dict: Insights con estructura garantizada
    """
    if not insights:
        insights = {}
    
    # Asegurar estructura básica
    sanitized = {
        'user_id': insights.get('user_id', 'unknown'),
        'period_days': insights.get('period_days', 90),
        'generated_at': insights.get('generated_at', datetime.now().isoformat()),
        'top_artists': insights.get('top_artists', []),
        'daily_pattern': insights.get('daily_pattern', []),
        'weekly_pattern': insights.get('weekly_pattern', {}),
        'popularity_distribution': insights.get('popularity_distribution', {})
    }
    
    # Asegurar estructura de weekly_pattern
    if not sanitized['weekly_pattern'] or not isinstance(sanitized['weekly_pattern'], dict):
        sanitized['weekly_pattern'] = {}
    
    # Asegurar que weekday y weekend existen con estructura mínima
    default_pattern = {'play_count': 0, 'active_days': 0, 'avg_popularity': 0, 'percentage': 0.0}
    
    if 'weekday' not in sanitized['weekly_pattern']:
        sanitized['weekly_pattern']['weekday'] = default_pattern.copy()
    else:
        # Asegurar que percentage existe
        if 'percentage' not in sanitized['weekly_pattern']['weekday']:
            sanitized['weekly_pattern']['weekday']['percentage'] = 0.0
    
    if 'weekend' not in sanitized['weekly_pattern']:
        sanitized['weekly_pattern']['weekend'] = default_pattern.copy()
    else:
        # Asegurar que percentage existe
        if 'percentage' not in sanitized['weekly_pattern']['weekend']:
            sanitized['weekly_pattern']['weekend']['percentage'] = 0.0
    
    # Asegurar estructura de popularity_distribution
    if not sanitized['popularity_distribution'] or not isinstance(sanitized['popularity_distribution'], dict):
        sanitized['popularity_distribution'] = {}
    
    default_tier = {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0}
    
    for tier in ['emergent', 'growing', 'established']:
        if tier not in sanitized['popularity_distribution']:
            sanitized['popularity_distribution'][tier] = default_tier.copy()
        else:
            # Asegurar que percentage existe
            if 'percentage' not in sanitized['popularity_distribution'][tier]:
                sanitized['popularity_distribution'][tier]['percentage'] = 0.0
    
    # Asegurar summary existe
    if 'summary' not in sanitized['popularity_distribution']:
        sanitized['popularity_distribution']['summary'] = {'total_plays': 0, 'dominant_tier': 'unknown'}
    
    return sanitized

@dashboard_bp.route('/dashboard')
def dashboard():
    """
    Muestra el dashboard principal con insights avanzados de Athena.
    
    Este endpoint combina datos en tiempo real de la API de Spotify con insights
    históricos procesados desde Athena. La idea es mostrar tanto el contexto
    inmediato (últimas canciones) como los patrones de largo plazo del usuario.
    """
    logger.debug("Dashboard endpoint accessed")
    
    # Verificación de credenciales (código existente mantenido)
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    token_info = session.get('token_info')
    
    if not client_id or not client_secret:
        logger.warning("No credentials found in session, redirecting to index")
        flash("Se requieren credenciales de Spotify")
        return redirect('/')
    
    if not token_info:
        logger.warning("No token found in session, checking if we have it saved")
        token_info = load_user_token(client_id)
        
        if not token_info:
            logger.warning("No valid token found, redirecting to login")
            flash("Se necesita iniciar sesión en Spotify")
            return redirect('/login')
        
        session['token_info'] = token_info
        logger.debug("Loaded token from file and saved to session")
    
    # Validación y renovación del token (código existente)
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
    
    # Obtener datos básicos de Spotify API (para contexto inmediato)
    try:
        spotify_data = get_user_data(token_info['access_token'])
        user_display_name = token_info.get('display_name', 'Usuario')
        user_id = token_info.get('user_id')
        
        logger.info(f"Datos de Spotify obtenidos para usuario: {user_id}")
        
    except Exception as e:
        logger.error(f"Error obteniendo datos de Spotify: {str(e)}")
        if "unauthorized" in str(e).lower():
            flash("Sesión expirada. Por favor inicia sesión nuevamente.")
            return redirect('/login')
        return f"Error obteniendo datos de Spotify: {str(e)}"
    
    # Obtener insights avanzados de Athena (nueva funcionalidad)
    advanced_insights = {}
    insights_available = False
    
    if athena_service and user_id:
        try:
            logger.info(f"Obteniendo insights avanzados para usuario: {user_id}")
            
            # Obtener todos los insights en una sola operación optimizada
            raw_insights = athena_service.get_user_insights_summary(
                user_id=user_id, 
                days_back=90  # Últimos 3 meses para un análisis representativo
            )
            
            # Sanitizar y validar los insights
            advanced_insights = _sanitize_insights(raw_insights)
            
            # Verificar si tenemos datos útiles
            insights_available = _has_valid_insights(advanced_insights)
            
            if insights_available:
                logger.info(f"Insights avanzados obtenidos exitosamente para {user_id}")
            else:
                logger.info(f"Insights obtenidos pero sin datos suficientes para {user_id}")
            
        except Exception as e:
            logger.warning(f"No se pudieron obtener insights avanzados: {str(e)}")
            # Continúamos con el dashboard básico si Athena falla
            advanced_insights = _sanitize_insights({
                'user_id': user_id,
                'error': 'Los insights avanzados no están disponibles en este momento'
            })
            insights_available = False
    else:
        logger.warning("Servicio de Athena no disponible o user_id faltante")
        advanced_insights = _sanitize_insights({
            'user_id': user_id or 'unknown',
            'error': 'Servicio de insights no disponible'
        })
        insights_available = False
    
    # Obtener perfil musical del usuario (nuevo)
    user_music_profile = None
    if music_profile_service and user_id:
        try:
            logger.info(f"Obteniendo perfil musical para usuario: {user_id}")
            user_music_profile = music_profile_service.get_user_profile(user_id)
            logger.info(f"Perfil musical obtenido: {user_music_profile['name']}")
        except Exception as e:
            logger.warning(f"No se pudo obtener perfil musical: {str(e)}")
            user_music_profile = None
    
    # Renderizar template con datos combinados
    try:
        return render_template(
            'dashboard/index.html',
            user_name=user_display_name,
            user_id=user_id,
            # Datos básicos de Spotify API (contexto inmediato)
            recent_tracks=spotify_data['recent_tracks'],
            top_artists=spotify_data['top_artists'],
            # Insights avanzados de Athena (patrones históricos)
            advanced_insights=advanced_insights,
            insights_available=insights_available,
            # Perfil musical del usuario (ML clustering)
            user_music_profile=user_music_profile
        )
    except Exception as e:
        logger.error(f"Error renderizando template: {str(e)}")
        # En caso de error en el template, mostrar una página de error amigable
        return render_template(
            'dashboard/index.html',
            user_name=user_display_name,
            user_id=user_id,
            recent_tracks=spotify_data['recent_tracks'],
            top_artists=spotify_data['top_artists'],
            advanced_insights=_sanitize_insights({}),
            insights_available=False,
            user_music_profile=None,
            template_error="Hubo un problema cargando algunos insights. Inténtalo de nuevo más tarde."
        )

@dashboard_bp.route('/api/insights/<insight_type>')
def get_specific_insight(insight_type):
    """
    API endpoint para obtener insights específicos de manera asíncrona.
    
    Este endpoint permite cargar insights individuales bajo demanda, lo cual
    mejora el tiempo de carga inicial del dashboard y permite refrescar
    datos específicos sin recargar toda la página.
    
    Args:
        insight_type: Tipo de insight solicitado (top_artists, daily_pattern, etc.)
    """
    logger.debug(f"API insight request: {insight_type}")
    
    # Verificar autenticación
    user_id = session.get('token_info', {}).get('user_id')
    if not user_id:
        return jsonify({'error': 'Usuario no autenticado'}), 401
    
    if not athena_service:
        return jsonify({'error': 'Servicio de insights no disponible'}), 503
    
    # Obtener parámetros opcionales
    days_back = request.args.get('days_back', 90, type=int)
    limit = request.args.get('limit', 10, type=int)
    
    try:
        # Mapeo de tipos de insight a funciones del servicio
        insight_functions = {
            'top_artists': lambda: athena_service.get_top_artists(user_id, limit, days_back),
            'daily_pattern': lambda: athena_service.get_daily_listening_pattern(user_id, min(days_back, 30)),
            'weekly_pattern': lambda: athena_service.get_weekday_vs_weekend_pattern(user_id, days_back),
            'popularity_distribution': lambda: athena_service.get_popularity_distribution(user_id, days_back)
        }
        
        if insight_type not in insight_functions:
            return jsonify({'error': f'Tipo de insight no válido: {insight_type}'}), 400
        
        # Ejecutar la función correspondiente
        result = insight_functions[insight_type]()
        
        return jsonify({
            'success': True,
            'insight_type': insight_type,
            'user_id': user_id,
            'period_days': days_back,
            'data': result,
            'generated_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo insight {insight_type}: {str(e)}")
        return jsonify({'error': f'Error obteniendo {insight_type}'}), 500

@dashboard_bp.route('/api/insights/refresh')
def refresh_all_insights():
    """
    Endpoint para refrescar todos los insights principales.
    
    Este endpoint es útil cuando el usuario quiere actualizar sus datos
    después de que se hayan procesado nuevas reproducciones en el pipeline ETL.
    """
    logger.debug("Refresh all insights request")
    
    # Verificar autenticación
    user_id = session.get('token_info', {}).get('user_id')
    if not user_id:
        return jsonify({'error': 'Usuario no autenticado'}), 401
    
    if not athena_service:
        return jsonify({'error': 'Servicio de insights no disponible'}), 503
    
    # Obtener parámetros
    days_back = request.args.get('days_back', 90, type=int)
    
    try:
        # Obtener resumen completo actualizado
        raw_insights = athena_service.get_user_insights_summary(user_id, days_back)
        
        # Sanitizar los insights
        insights = _sanitize_insights(raw_insights)
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'insights': insights,
            'has_valid_data': _has_valid_insights(insights),
            'refreshed_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error refrescando insights: {str(e)}")
        return jsonify({'error': 'Error refrescando insights'}), 500

@dashboard_bp.route('/actualizar_datos', methods=['POST'])
def actualizar_datos():
    """
    Endpoint heredado para actualizar datos básicos de Spotify API.
    
    Mantenemos este endpoint para compatibilidad, pero ahora también
    puede incluir una actualización de insights si se solicita.
    """
    logger.debug("Actualizar datos endpoint called")
    
    # Verificación de credenciales (código existente)
    client_id = session.get('client_id')
    client_secret = session.get('client_secret')
    token_info = session.get('token_info')
    
    if not client_id or not client_secret or not token_info:
        return jsonify({'error': 'No se encontraron credenciales o token'}), 401
    
    if not validate_token(token_info, client_id):
        return jsonify({'error': 'El token no es válido para este cliente'}), 401
    
    # Verificar y renovar token si es necesario
    sp_oauth = get_spotify_oauth(client_id, client_secret)
    
    if sp_oauth.is_token_expired(token_info):
        try:
            token_info = refresh_token(token_info, client_id, client_secret)
            session['token_info'] = token_info
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return jsonify({'error': 'Error renovando el token'}), 401
    
    # Obtener datos actualizados de Spotify API
    try:
        spotify_data = get_user_data(token_info['access_token'])
        
        # Opcionalmente, incluir datos de insights si se solicita
        include_insights = request.json.get('include_insights', False) if request.is_json else False
        response_data = spotify_data.copy()
        
        if include_insights and athena_service:
            user_id = token_info.get('user_id')
            if user_id:
                try:
                    raw_insights = athena_service.get_user_insights_summary(user_id, days_back=30)
                    insights = _sanitize_insights(raw_insights)
                    response_data['advanced_insights'] = insights
                    response_data['insights_available'] = _has_valid_insights(insights)
                except Exception as e:
                    logger.warning(f"Error obteniendo insights en actualización: {str(e)}")
                    response_data['advanced_insights'] = _sanitize_insights({})
                    response_data['insights_available'] = False
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error obteniendo datos de Spotify: {str(e)}")
        return jsonify({'error': f'Error obteniendo datos: {str(e)}'}), 500

@dashboard_bp.route('/api/user/summary')
def get_user_summary():
    """
    Endpoint para obtener un resumen ejecutivo del perfil musical del usuario.
    
    Este endpoint proporciona métricas clave y tendencias principales que
    pueden utilizarse para personalización o para mostrar un "snapshot"
    rápido del perfil musical del usuario.
    """
    logger.debug("User summary request")
    
    # Verificar autenticación
    user_id = session.get('token_info', {}).get('user_id')
    if not user_id:
        return jsonify({'error': 'Usuario no autenticado'}), 401
    
    if not athena_service:
        return jsonify({'error': 'Servicio de insights no disponible'}), 503
    
    try:
        # Obtener insights básicos para generar resumen
        top_artists = athena_service.get_top_artists(user_id, limit=3, days_back=30)
        weekly_pattern = athena_service.get_weekday_vs_weekend_pattern(user_id, days_back=30)
        popularity_dist = athena_service.get_popularity_distribution(user_id, days_back=30)
        
        # Generar resumen interpretativo
        summary = {
            'user_id': user_id,
            'period': 'últimos 30 días',
            'top_artist': top_artists[0]['artist_name'] if top_artists else 'No disponible',
            'total_top_plays': sum([artist['play_count'] for artist in top_artists]),
            'listening_preference': 'fin de semana' if weekly_pattern.get('weekend', {}).get('percentage', 0) > 60 else 'días laborales',
            'music_taste_profile': _determine_taste_profile(popularity_dist),
            'activity_level': _determine_activity_level(weekly_pattern),
            'generated_at': datetime.now().isoformat()
        }
        
        return jsonify(summary)
        
    except Exception as e:
        logger.error(f"Error generando resumen de usuario: {str(e)}")
        return jsonify({'error': 'Error generando resumen'}), 500

@dashboard_bp.route('/api/music-profile/<user_id>')
def get_user_music_profile(user_id):
    """
    API endpoint para obtener el perfil musical de un usuario específico.
    
    Este endpoint proporciona el perfil musical generado por clustering ML,
    incluyendo estadísticas del perfil y características del usuario.
    
    Args:
        user_id: ID del usuario de Spotify
    """
    logger.debug(f"API music profile request: {user_id}")
    
    # Verificar autenticación básica
    session_user_id = session.get('token_info', {}).get('user_id')
    if not session_user_id:
        return jsonify({'error': 'Usuario no autenticado'}), 401
    
    if not music_profile_service:
        return jsonify({'error': 'Servicio de perfiles musicales no disponible'}), 503
    
    try:
        # Obtener perfil del usuario
        profile = music_profile_service.get_user_profile(user_id)
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'profile': profile,
            'generated_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo perfil musical {user_id}: {str(e)}")
        return jsonify({'error': f'Error obteniendo perfil musical'}), 500

@dashboard_bp.route('/api/music-profiles/stats')
def get_music_profiles_stats():
    """
    Endpoint para obtener estadísticas generales de todos los perfiles musicales.
    
    Útil para mostrar información comparativa y distribuciones de perfiles.
    """
    logger.debug("API music profiles stats request")
    
    # Verificar autenticación básica
    session_user_id = session.get('token_info', {}).get('user_id')
    if not session_user_id:
        return jsonify({'error': 'Usuario no autenticado'}), 401
    
    if not music_profile_service:
        return jsonify({'error': 'Servicio de perfiles musicales no disponible'}), 503
    
    try:
        # Obtener estadísticas generales
        stats = music_profile_service.get_all_profile_stats()
        
        return jsonify({
            'success': True,
            'stats': stats,
            'service_status': 'available' if music_profile_service.is_service_available() else 'limited',
            'generated_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas de perfiles: {str(e)}")
        return jsonify({'error': 'Error obteniendo estadísticas'}), 500

@dashboard_bp.route('/api/music-profiles/refresh', methods=['POST'])
def refresh_music_profiles():
    """
    Endpoint para refrescar los perfiles musicales desde S3.
    
    Útil para actualizar los perfiles cuando se ha entrenado un nuevo modelo.
    """
    logger.debug("API refresh music profiles request")
    
    # Verificar autenticación
    session_user_id = session.get('token_info', {}).get('user_id')
    if not session_user_id:
        return jsonify({'error': 'Usuario no autenticado'}), 401
    
    if not music_profile_service:
        return jsonify({'error': 'Servicio de perfiles musicales no disponible'}), 503
    
    try:
        # Refrescar perfiles
        success = music_profile_service.refresh_profiles()
        
        if success:
            stats = music_profile_service.get_all_profile_stats()
            return jsonify({
                'success': True,
                'message': 'Perfiles musicales actualizados exitosamente',
                'stats': stats,
                'refreshed_at': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'success': False,
                'message': 'No se pudieron actualizar los perfiles musicales'
            }), 500
        
    except Exception as e:
        logger.error(f"Error refrescando perfiles musicales: {str(e)}")
        return jsonify({'error': 'Error refrescando perfiles'}), 500

def _determine_taste_profile(popularity_dist):
    """
    Determina el perfil de gusto musical basado en la distribución de popularidad.
    
    Esta función analiza las proporciones de música emergente vs establecida
    para generar una descripción legible del perfil musical del usuario.
    """
    if not popularity_dist or 'summary' not in popularity_dist:
        return 'Explorador'
    
    dominant_tier = popularity_dist['summary'].get('dominant_tier', 'unknown')
    
    profile_map = {
        'emergent': 'Descubridor Underground',
        'growing': 'Explorador Equilibrado', 
        'established': 'Amante del Mainstream',
        'unknown': 'Explorador'
    }
    
    return profile_map.get(dominant_tier, 'Explorador')

def _determine_activity_level(weekly_pattern):
    """
    Determina el nivel de actividad musical basado en patrones semanales.
    
    Analiza la consistencia y volumen de escucha para clasificar el nivel
    de engagement musical del usuario.
    """
    if not weekly_pattern or 'weekday' not in weekly_pattern or 'weekend' not in weekly_pattern:
        return 'Casual'
    
    total_plays = weekly_pattern.get('weekday', {}).get('play_count', 0) + \
                  weekly_pattern.get('weekend', {}).get('play_count', 0)
    
    total_days = weekly_pattern.get('weekday', {}).get('active_days', 0) + \
                 weekly_pattern.get('weekend', {}).get('active_days', 0)
    
    if total_days == 0:
        return 'Casual'
    
    avg_plays_per_day = total_plays / total_days if total_days > 0 else 0
    
    if avg_plays_per_day > 50:
        return 'Muy Activo'
    elif avg_plays_per_day > 20:
        return 'Activo'
    elif avg_plays_per_day > 5:
        return 'Moderado'
    else:
        return 'Casual'