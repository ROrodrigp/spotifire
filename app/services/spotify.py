import os
import json
import logging
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from flask import session, redirect
from config import Config

logger = logging.getLogger(__name__)

def get_spotify_oauth(client_id, client_secret, state=None):
    """Configura y devuelve un objeto SpotifyOAuth"""
    return SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=Config.REDIRECT_URI,
        scope="user-read-recently-played user-top-read",
        state=state
    )

def refresh_token(token_info, client_id, client_secret):
    """Refresca un token expirado"""
    try:
        # Configurar OAuth
        sp_oauth = get_spotify_oauth(client_id, client_secret)
        
        # Guardar campos adicionales antes de refrescar
        saved_client_id = token_info.get('client_id')
        saved_client_secret = token_info.get('client_secret')
        saved_redirect_uri = token_info.get('redirect_uri')
        saved_user_id = token_info.get('user_id')
        saved_display_name = token_info.get('display_name')
        
        # Refrescar el token
        new_token = sp_oauth.refresh_access_token(token_info['refresh_token'])
        
        # Restaurar campos adicionales
        new_token['client_id'] = saved_client_id
        new_token['client_secret'] = saved_client_secret
        new_token['redirect_uri'] = saved_redirect_uri
        new_token['user_id'] = saved_user_id
        new_token['display_name'] = saved_display_name
        new_token['last_updated'] = datetime.now().isoformat()
        
        # Actualizar archivo
        client_file = os.path.join(Config.USERS_DATA_DIR, f"{client_id}.json")
        with open(client_file, 'w') as f:
            json.dump(new_token, f)
            
        logger.debug("Token refrescado y guardado correctamente")
        return new_token
        
    except Exception as e:
        logger.error(f"Error refrescando token: {str(e)}")
        raise

def get_user_data(access_token):
    """Obtiene datos del usuario y canciones recientes"""
    sp = spotipy.Spotify(auth=access_token)
    
    try:
        # Obtener canciones recientes
        recent_tracks = sp.current_user_recently_played(limit=10)
        
        # Obtener artistas principales
        top_artists = sp.current_user_top_artists(limit=5, time_range='short_term')
        
        # Procesar datos de canciones
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
        
        # Procesar datos de artistas
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
            
        return {
            'recent_tracks': tracks,
            'top_artists': artists
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo datos de Spotify: {str(e)}")
        raise