#!/usr/bin/env python3
"""
Servicio para recolectar datos de reproducción histórica de Spotify, 
lista de canciones con "Me Gusta" y lista de artistas seguidos 
para múltiples usuarios y guardarlos en archivos JSON con timestamp.

Uso:
    # Para procesar todos los usuarios en un directorio:
    # Para procesar todos los usuarios en un directorio:
    python history_periodic_collector.py --users_dir DIRECTORIO_DE_USUARIOS --output_base_dir DIRECTORIO_BASE_SALIDA

    # Para procesar un usuario específico:
    python history_periodic_collector.py --single_user_file ARCHIVO_JSON_USUARIO --output_base_dir DIRECTORIO_BASE_SALIDA

Ejemplos:
    # Procesar todos los usuarios
    # Para procesar un usuario específico:
    python history_periodic_collector.py --single_user_file ARCHIVO_JSON_USUARIO --output_base_dir DIRECTORIO_BASE_SALIDA

Ejemplos:
    # Procesar todos los usuarios
    python history_periodic_collector.py --users_dir /path/to/users_data --output_base_dir /home/ec2-user/spotifire_new_directories/data/users_data

    # Procesar solo un usuario específico
    python history_periodic_collector.py --single_user_file /path/to/users_data/user123.json --output_base_dir /home/ec2-user/spotifire_new_directories/data/users_data

    # Procesar solo un usuario específico
    python history_periodic_collector.py --single_user_file /path/to/users_data/user123.json --output_base_dir /home/ec2-user/spotifire_new_directories/data/users_data
"""

import os
import time
import json
import csv
import argparse
import logging
import glob
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("spotify_collector.log")
    ]
)
logger = logging.getLogger("spotify_collector")

class SpotifyUserCollector:
    def __init__(self, credentials_file, output_base_dir):
        """
        Inicializa el recolector de datos para un usuario específico.
        
        Args:
            credentials_file: Ruta al archivo JSON con las credenciales del usuario
            output_base_dir: Directorio base donde se guardarán los JSON de datos
        """
        self.credentials_file = credentials_file
        self.output_base_dir = output_base_dir
        self.user_id = None
        self.timeout = 20  # Timeout más largo (20 segundos en lugar de 5)
        
        # Cargar credenciales desde el archivo JSON
        try:
            with open(credentials_file, 'r') as f:
                self.credentials = json.load(f)
                logger.info(f"Credenciales cargadas desde {credentials_file}")
        except Exception as e:
            logger.error(f"Error al cargar credenciales: {e}")
            raise
        
        # Obtener los datos básicos del usuario
        self.client_id = self.credentials.get('client_id')
        self.client_secret = self.credentials.get('client_secret')
        self.redirect_uri = self.credentials.get('redirect_uri', "http://localhost:8888/callback")
        self.user_id = self.credentials.get('user_id')
        
        if not self.client_id or not self.client_secret:
            logger.error(f"El archivo {credentials_file} no contiene client_id o client_secret")
            raise ValueError("Credenciales incompletas")
        
        # Configurar directorio de salida específico para este usuario
        self.user_dir = os.path.join(self.output_base_dir, self.user_id) if self.user_id else os.path.join(
            self.output_base_dir, os.path.basename(credentials_file).split('.')[0])
        os.makedirs(self.user_dir, exist_ok=True)
        logger.info(f"Directorio para el usuario configurado: {self.user_dir}")
        
        # Configurar la autenticación de Spotify con mejor manejo de errores
        try:
            self.sp = self._setup_spotify_client()
            
            # Si no tenemos user_id en el archivo, obténerlo del perfil
            if not self.user_id:
                try:
                    user_profile = self.sp.current_user()
                    self.user_id = user_profile['id']
                    # Actualizar el archivo JSON con el user_id
                    self.credentials['user_id'] = self.user_id
                    with open(credentials_file, 'w') as f:
                        json.dump(self.credentials, f)
                    # Actualizar el directorio del usuario
                    self.user_dir = os.path.join(self.output_base_dir, self.user_id)
                    os.makedirs(self.user_dir, exist_ok=True)
                    logger.info(f"ID de usuario obtenido y guardado: {self.user_id}")
                except Exception as e:
                    # Si falla al obtener el perfil, usar un ID basado en el nombre del archivo
                    fallback_id = os.path.basename(credentials_file).split('.')[0]
                    logger.warning(f"No se pudo obtener el ID de usuario: {e}. Usando ID basado en archivo: {fallback_id}")
                    self.user_id = fallback_id
            
            logger.info(f"Cliente de Spotify configurado para el usuario: {self.user_id}")
        except Exception as e:
            logger.error(f"Error al configurar cliente de Spotify para {os.path.basename(credentials_file)}: {e}")
            raise

    def _setup_spotify_client(self):
        """Configura y devuelve un cliente autenticado de Spotify"""
        # Scope para acceder al historial de reproducción
        
        # Configurar OAuth con token existente
        auth_manager = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope="user-library-read user-read-recently-played user-top-read playlist-read-private playlist-read-collaborative user-follow-read",
            open_browser=False,
            cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
        )
        
        # Si hay un refresh_token en el archivo, usarlo para inicializar correctamente
        if "refresh_token" in self.credentials:
            token_info = {
                "access_token": self.credentials.get("access_token", ""),
                "refresh_token": self.credentials.get("refresh_token"),
                "expires_at": self.credentials.get("expires_at", 0),
                "scope": self.credentials.get("scope", "user-library-read user-read-recently-played user-top-read playlist-read-private playlist-read-collaborative user-follow-read"),
                "token_type": self.credentials.get("token_type", "Bearer")
            }
            
            # Guardar en el cache de spotipy
            auth_manager.cache_handler.save_token_to_cache(token_info)
            logger.info(f"Token guardado en el cache de spotipy para {self.user_id}")
            
            # Verificar si el token está expirado y actualizarlo
            if auth_manager.is_token_expired(token_info):
                logger.info(f"Token expirado para {self.user_id}, refrescando...")
                token_info = auth_manager.refresh_access_token(token_info["refresh_token"])
                # Actualizar el archivo JSON con el nuevo token
                self.credentials.update({
                    "access_token": token_info["access_token"],
                    "refresh_token": token_info["refresh_token"],
                    "expires_at": token_info["expires_at"],
                    "last_updated": datetime.now().isoformat()
                })
                with open(self.credentials_file, 'w') as f:
                    json.dump(self.credentials, f)
                logger.info(f"Token actualizado para {self.user_id}")
        
        # Instanciar cliente con el timeout ajustado
        sp = spotipy.Spotify(auth_manager=auth_manager)
        
        # Intentar configurar timeouts directamente en la sesión requests subyacente
        try:
            # Detectar y configurar el timeout en la estructura correcta
            if hasattr(sp, '_session'):
                sp._session.timeout = self.timeout
                logger.info(f"Timeout configurado a {self.timeout} segundos para {self.user_id} (via _session)")
            elif hasattr(sp, '_auth') and hasattr(sp._auth, 'session'):
                sp._auth.session.timeout = self.timeout
                logger.info(f"Timeout configurado a {self.timeout} segundos para {self.user_id} (via _auth.session)")
        except Exception as e:
            logger.warning(f"No se pudo configurar el timeout para {self.user_id}: {e}")
        
        return sp
    
    def get_likes_playlist(self):
        """Obtiene las canciones con 'Me Gustas' de un usuario específico"""
        try:
            # Configurar timeout en el objeto de sesión subyacente
            self._configure_session_timeout()
            
            # Implementar reintentos simples
            max_retries = 3
            retry_delay = 5  # segundos
            
            for attempt in range(max_retries):
                try:
                    results = self.sp.current_user_saved_tracks(limit=50)
                    likes_list = results['items']
                    while results['next']:
                        results = self.sp.next(results)
                        likes_list.extend(results['items'])
                    logger.info(f"Obtenidas {len(likes_list)} canciones likeadas para {self.user_id}")
                    return likes_list
                except Exception as e:
                    if attempt < max_retries - 1 and self._should_retry(e):
                        logger.warning(f"Error al obtener likes para {self.user_id}, reintento {attempt+1}/{max_retries} en {retry_delay} segundos: {str(e)}")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponencial
                    else:
                        # Si es el último intento, propagar el error
                        raise
                        
        except Exception as e:
            logger.error(f"Error al obtener canciones likeadas para {self.user_id}: {e}")
            return []
    
    def get_followed_artists(self):
        """Obtiene los artistas seguidos de un usuario específico"""
        try:
            # Configurar timeout en el objeto de sesión subyacente
            self._configure_session_timeout()
            
            # Implementar reintentos simples
            max_retries = 3
            retry_delay = 5  # segundos
            
            for attempt in range(max_retries):
                try:
                    results = self.sp.current_user_followed_artists(limit=50)
                    follows_list = results['artists']['items']
                    while results['artists']['next']:
                        results = self.sp.next(results['artists'])
                        follows_list.extend(results['artists']['items'])
                    logger.info(f"Obtenidos {len(follows_list)} artistas seguidos para {self.user_id}")
                    return follows_list

                except Exception as e:
                    if attempt < max_retries - 1 and self._should_retry(e):
                        logger.warning(f"Error al obtener artistas seguidos para {self.user_id}, reintento {attempt+1}/{max_retries} en {retry_delay} segundos: {str(e)}")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponencial
                    else:
                        # Si es el último intento, propagar el error
                        raise
                        
        except Exception as e:
            logger.error(f"Error al obtener artistas seguidos para {self.user_id}: {e}")
            return []

    def get_top_tracks(self, period):
        """Obtiene las canciones principales de un usuario específico"""
        try:
            # Configurar timeout en el objeto de sesión subyacente
            self._configure_session_timeout()
            
            # Implementar reintentos simples
            max_retries = 3
            retry_delay = 5  # segundos
            
            for attempt in range(max_retries):
                try:
                    results = self.sp.current_user_top_tracks(limit=50, time_range=period)
                    top_tracks = results['items']
                    while results['next']:
                        results = self.sp.next(results)
                        top_tracks.extend(results['items'])

                    logger.info(f"Obtenidas {len(top_tracks)} top tracks para {self.user_id}")
                    return top_tracks
                except Exception as e:
                    if attempt < max_retries - 1 and self._should_retry(e):
                        logger.warning(f"Error al obtener top tracks para {self.user_id}, reintento {attempt+1}/{max_retries} en {retry_delay} segundos: {str(e)}")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponencial
                    else:
                        # Si es el último intento, propagar el error
                        raise
                        
        except Exception as e:
            logger.error(f"Error al obtener top tracks para {self.user_id}: {e}")
            return []

    def _configure_session_timeout(self):
        """Configura el timeout de la sesión"""
        try:
            if hasattr(self.sp, '_session'):
                self.sp._session.timeout = 20
            elif hasattr(self.sp, '_auth') and hasattr(self.sp._auth, 'session'):
                self.sp._auth.session.timeout = 20
        except Exception as e:
            logger.warning(f"Error al configurar timeout para {self.user_id}: {e}")

    def _should_retry(self, error):
        """Determina si se debe reintentar basado en el tipo de error"""
        error_str = str(error).lower()
        return ("timeout" in error_str or 
                "rate limiting" in error_str or 
                "connection" in error_str or
                "read timed out" in error_str)

    def save_to_json(self, data, data_type):
        """Guarda los datos en un archivo JSON"""
        if not data:
            logger.warning(f"No hay datos para guardar para {self.user_id} - tipo: {data_type}")
            return
        
        try:
            if data_type == "likes":
                filename = os.path.join(self.user_dir, f"likes_list.json")
                list_jsons = [
                    {
                        'track_id': item['track']['id'],
                        'album_id': item['track']['album']['id'],
                        'artists_id': [artist['id'] for artist in item['track']['artists']],
                        'explicit': item['track']['explicit'],
                        'duration_ms': item['track']['duration_ms'],
                        'track_name': item['track']['name'],
                        'track_popularity': item['track']['popularity'],
                        'added_at': item['added_at']
                    }
                    for item in data
                ]
            elif data_type == "followed":
                filename = os.path.join(self.user_dir, f"followed_artists.json")
                list_jsons = {'artists_ids': [item['id'] for item in data]}
            elif data_type == "top_tracks":
                filename = os.path.join(self.user_dir, f"top_tracks.json")
                list_jsons = [
                    {
                        'ith_preference': i + 1,
                        'track_id': item['id'],
                        'album_id': item['album']['id'],
                        'artists_id': [artist['id'] for artist in item['artists']],
                        'explicit': item['explicit'],
                        'duration': item['duration_ms'],
                        'track_name': item['name'],
                        'track_popularity': item['popularity']
                    }
                    for i, item in enumerate(data)
                ]
            else:
                logger.error(f"Tipo de datos no reconocido: {data_type}")
                return None

            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(list_jsons, jsonfile, ensure_ascii=False, indent=4)            
                logger.info(f"Datos guardados en {filename} para {self.user_id}")
                return filename
        
        except Exception as e:
                logger.error(f"Error al guardar datos en JSON para {self.user_id}: {e}")
                return None
    
    def run_once(self):
        """Ejecuta una única recolección de datos"""
        try:
            logger.info(f"Iniciando recolección de datos para usuario: {self.user_id}")
            
            # Obtener datos
            data_likes = self.get_likes_playlist()
            data_followed = self.get_followed_artists()
            data_top_tracks = self.get_top_tracks("long_term")

            # Guardar datos (corregido el error de save_to_csv -> save_to_json)
            likes_result = self.save_to_json(data_likes, "likes")
            followed_result = self.save_to_json(data_followed, "followed")
            top_tracks_result = self.save_to_json(data_top_tracks, "top_tracks")
            self.save_to_csv(data_l,"likes")
            self.save_to_csv(data_f,"followed")
            self.save_to_csv(data_t,"top_tracks")

            success_count = sum(1 for result in [likes_result, followed_result, top_tracks_result] if result is not None)
            
            logger.info(f"Recolección completada para {self.user_id}. Archivos guardados: {success_count}/3")
            return f"Usuario {self.user_id} - {success_count}/3 archivos guardados exitosamente"
        
        except Exception as e:
            logger.error(f"Error al ejecutar recolección para {self.user_id}: {e}")
            return None

class SpotifyMultiUserCollector:
    def __init__(self, users_dir, output_base_dir, interval_seconds=3600):
        """
        Inicializa el recolector de datos de múltiples usuarios de Spotify.
        
        Args:
            users_dir: Directorio donde se encuentran los archivos JSON de credenciales de usuarios
            output_base_dir: Directorio base donde se guardarán los JSON de datos
            interval_seconds: Intervalo en segundos entre recolecciones (por defecto 1 hora)
        """
        self.users_dir = users_dir
        self.output_base_dir = output_base_dir
        self.interval_seconds = interval_seconds
        
        # Asegurar que el directorio de salida existe
        os.makedirs(output_base_dir, exist_ok=True)
        logger.info(f"Directorio base de salida: {output_base_dir}")
        
    def get_user_credentials_files(self):
        """Obtiene la lista de archivos JSON de credenciales de usuarios"""
        pattern = os.path.join(self.users_dir, "*.json")
        files = glob.glob(pattern)
        logger.info(f"Encontrados {len(files)} archivos de credenciales de usuarios")
        return files
    
    def run_once(self):
        """Ejecuta una única recolección de datos para todos los usuarios"""
        files = self.get_user_credentials_files()
        results = []
        
        # Añadir un retraso entre usuarios para evitar sobrecarga de la API
        delay_between_users = 2  # segundos
        
        for i, file in enumerate(files):
            try:
                logger.info(f"Procesando usuario con archivo: {os.path.basename(file)} ({i+1}/{len(files)})")
                collector = SpotifyUserCollector(file, self.output_base_dir)
                result = collector.run_once()
                if result:
                    results.append(result)
                
                # Añadir un retraso entre usuarios para evitar sobrecarga (excepto para el último)
                if i < len(files) - 1:
                    logger.info(f"Esperando {delay_between_users} segundos antes del siguiente usuario...")
                    time.sleep(delay_between_users)
                    
            except Exception as e:
                logger.error(f"Error procesando usuario {os.path.basename(file)}: {e}")
                results.append(f"Error procesando {os.path.basename(file)}: {str(e)}")
        
        return results

class SpotifySingleUserCollector:
    """
    Clase para manejar la recolección de datos de un solo usuario específico.
    """
    def __init__(self, user_file, output_base_dir):
        """
        Inicializa el recolector para un solo usuario.
        
        Args:
            user_file: Ruta al archivo JSON con las credenciales del usuario
            output_base_dir: Directorio base donde se guardarán los JSON de datos
        """
        self.user_file = user_file
        self.output_base_dir = output_base_dir
        
        # Verificar que el archivo existe
        if not os.path.exists(user_file):
            raise FileNotFoundError(f"El archivo de usuario no existe: {user_file}")
        
        logger.info(f"Inicializando recolector para usuario específico: {os.path.basename(user_file)}")
    
    def run_once(self):
        """Ejecuta recolección para el usuario específico"""
        try:
            logger.info(f"Procesando usuario específico: {os.path.basename(self.user_file)}")
            collector = SpotifyUserCollector(self.user_file, self.output_base_dir)
            result = collector.run_once()
            return result
        except Exception as e:
            logger.error(f"Error procesando usuario específico {os.path.basename(self.user_file)}: {e}")
            return None

def main():
    parser = argparse.ArgumentParser(
        description='Recolecta datos históricos de Spotify de múltiples usuarios o un usuario específico',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Procesar todos los usuarios en un directorio:
  python %(prog)s --users_dir /path/to/users_data --output_base_dir /path/to/output

  # Procesar solo un usuario específico:
  python %(prog)s --single_user_file /path/to/user123.json --output_base_dir /path/to/output

  # Procesar un usuario específico con ejecución única:
  python %(prog)s --single_user_file /path/to/user123.json --output_base_dir /path/to/output --once
        """
    )
    
    # Grupo mutuamente exclusivo para la fuente de usuarios
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        '--users_dir', 
        help='Directorio donde están los archivos JSON de credenciales de usuarios'
    )
    source_group.add_argument(
        '--single_user_file', 
        help='Archivo JSON específico de un usuario para procesar'
    )
    
    parser.add_argument(
        '--output_base_dir', 
        required=True, 
        help='Directorio base donde se guardarán los JSON de datos'
    )
    parser.add_argument(
        '--interval', 
        type=int, 
        default=3600, 
        help='Intervalo en segundos entre recolecciones (por defecto: 3600)'
    )
    parser.add_argument(
        '--once', 
        action='store_true', 
        help='Ejecutar solo una vez y salir'
    )
    
    args = parser.parse_args()
    
    # Verificar que el directorio de salida es válido
    try:
        os.makedirs(args.output_base_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"No se puede crear el directorio de salida {args.output_base_dir}: {e}")
        return 1
    
    try:
        if args.single_user_file:
            # Modo de usuario específico
            logger.info("=== MODO USUARIO ESPECÍFICO ===")
            logger.info(f"Archivo de usuario: {args.single_user_file}")
            logger.info(f"Directorio de salida: {args.output_base_dir}")
            
            collector = SpotifySingleUserCollector(
                user_file=args.single_user_file,
                output_base_dir=args.output_base_dir
            )
            
            result = collector.run_once()
            if result:
                logger.info(f"✅ Resultado: {result}")
            else:
                logger.error("❌ No se pudieron recolectar los datos del usuario")
                return 1
                
        else:
            # Modo de múltiples usuarios (comportamiento original)
            logger.info("=== MODO MÚLTIPLES USUARIOS ===")
            
            # Verificar si el directorio de usuarios existe
            if not os.path.isdir(args.users_dir):
                logger.error(f"El directorio de usuarios no existe: {args.users_dir}")
                return 1
            
            logger.info(f"Directorio de usuarios: {args.users_dir}")
            logger.info(f"Directorio de salida: {args.output_base_dir}")
            
            # Inicializar el colector de múltiples usuarios
            collector = SpotifyMultiUserCollector(
                users_dir=args.users_dir,
                output_base_dir=args.output_base_dir,
                interval_seconds=args.interval
            )
            
            # Ejecutar una vez o indefinidamente según las opciones
            if args.once:
                logger.info("Ejecutando recolección única para todos los usuarios")
                results = collector.run_once()
                logger.info(f"✅ Procesados {len(results)} usuarios")
                for result in results:
                    logger.info(f"  - {result}")
            else:
                logger.info("Modo periódico no implementado para múltiples usuarios en esta versión")
                logger.info("Use --once para ejecutar una sola vez")
                return 1
    
    except KeyboardInterrupt:
        logger.info("🛑 Proceso interrumpido por el usuario")
        return 0
    except Exception as e:
        logger.error(f"❌ Error durante la ejecución: {str(e)}")
        return 1
    
    logger.info("🎉 Proceso completado exitosamente")
    return 0

if __name__ == "__main__":
    exit(main())