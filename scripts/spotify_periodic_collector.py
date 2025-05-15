#!/usr/bin/env python3
"""
Servicio para recolectar periódicamente datos de reproducción reciente de Spotify
para múltiples usuarios y guardarlos en archivos CSV con timestamp.

Uso:
    python spotify_periodic_collector.py --users_dir DIRECTORIO_DE_USUARIOS --output_base_dir DIRECTORIO_BASE_SALIDA

Ejemplo:
    python spotify_periodic_collector.py --users_dir /path/to/users_data --output_base_dir /home/ec2-user/spotifire_new_directories/data/users_data
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
            output_base_dir: Directorio base donde se guardarán los CSV de datos
        """
        self.credentials_file = credentials_file
        self.output_base_dir = output_base_dir
        self.user_id = None
        
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
        
        # Configurar la autenticación de Spotify
        try:
            self.sp = self._setup_spotify_client()
            # Si no tenemos user_id en el archivo, obténerlo del perfil
            if not self.user_id:
                user_profile = self.sp.current_user()
                self.user_id = user_profile['id']
                # Actualizar el archivo JSON con el user_id
                self.credentials['user_id'] = self.user_id
                with open(credentials_file, 'w') as f:
                    json.dump(self.credentials, f)
                # Actualizar el directorio del usuario
                self.user_dir = os.path.join(self.output_base_dir, self.user_id)
                os.makedirs(self.user_dir, exist_ok=True)
            
            logger.info(f"Cliente de Spotify configurado para el usuario: {self.user_id}")
        except Exception as e:
            logger.error(f"Error al configurar cliente de Spotify para {os.path.basename(credentials_file)}: {e}")
            raise

    def _setup_spotify_client(self):
        """Configura y devuelve un cliente autenticado de Spotify"""
        # Scope para acceder al historial de reproducción
        scope = "user-read-recently-played"
        
        # Configurar OAuth con token existente
        auth_manager = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope=scope,
            open_browser=False,
            cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
        )
        
        # Si hay un refresh_token en el archivo, usarlo para inicializar correctamente
        if "refresh_token" in self.credentials:
            token_info = {
                "access_token": self.credentials.get("access_token", ""),
                "refresh_token": self.credentials.get("refresh_token"),
                "expires_at": self.credentials.get("expires_at", 0),
                "scope": self.credentials.get("scope", scope),
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
        
        return spotipy.Spotify(auth_manager=auth_manager)
    
    def get_recently_played(self):
        """Obtiene las canciones reproducidas recientemente por el usuario"""
        try:
            results = self.sp.current_user_recently_played(limit=50)
            logger.info(f"Obtenidas {len(results['items'])} canciones recientes para {self.user_id}")
            return results['items']
        except Exception as e:
            logger.error(f"Error al obtener canciones recientes para {self.user_id}: {e}")
            return []
    
    def save_to_csv(self, data):
        """Guarda los datos en un archivo CSV con timestamp"""
        if not data:
            logger.warning(f"No hay datos para guardar para {self.user_id}")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.user_dir, f"recently_played_{timestamp}.csv")
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'played_at', 'track_name', 'artist_name', 'album_name', 
                    'track_id', 'artist_id', 'album_id', 'duration_ms', 
                    'popularity', 'explicit'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for item in data:
                    track = item['track']
                    writer.writerow({
                        'played_at': item['played_at'],
                        'track_name': track['name'],
                        'artist_name': track['artists'][0]['name'],
                        'album_name': track['album']['name'],
                        'track_id': track['id'],
                        'artist_id': track['artists'][0]['id'],
                        'album_id': track['album']['id'],
                        'duration_ms': track['duration_ms'],
                        'popularity': track['popularity'],
                        'explicit': track['explicit']
                    })
                
                logger.info(f"Datos guardados en {filename} para {self.user_id}")
                return filename
        except Exception as e:
            logger.error(f"Error al guardar datos en CSV para {self.user_id}: {e}")
            return None
    
    def run_once(self):
        """Ejecuta una única recolección de datos"""
        try:
            data = self.get_recently_played()
            return self.save_to_csv(data)
        except Exception as e:
            logger.error(f"Error al ejecutar recolección para {self.user_id}: {e}")
            return None


class SpotifyMultiUserCollector:
    def __init__(self, users_dir, output_base_dir, interval_seconds=3600):
        """
        Inicializa el recolector periódico de datos de múltiples usuarios de Spotify.
        
        Args:
            users_dir: Directorio donde se encuentran los archivos JSON de credenciales de usuarios
            output_base_dir: Directorio base donde se guardarán los CSV de datos
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
        
        for file in files:
            try:
                logger.info(f"Procesando usuario con archivo: {os.path.basename(file)}")
                collector = SpotifyUserCollector(file, self.output_base_dir)
                result = collector.run_once()
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error procesando usuario {os.path.basename(file)}: {e}")
        
        return results
    
    def run_forever(self):
        """Ejecuta el servicio de recolección periódica indefinidamente para todos los usuarios"""
        logger.info(f"Iniciando servicio de recolección periódica cada {self.interval_seconds} segundos")
        
        try:
            while True:
                start_time = time.time()
                
                # Ejecutar la recolección para todos los usuarios
                results = self.run_once()
                logger.info(f"Recolección completada para {len(results)} usuarios")
                
                # Calcular tiempo real transcurrido y esperar hasta el próximo intervalo
                elapsed = time.time() - start_time
                wait_time = max(1, self.interval_seconds - elapsed)
                
                logger.info(f"Próxima recolección en {wait_time:.2f} segundos")
                time.sleep(wait_time)
        except KeyboardInterrupt:
            logger.info("Servicio detenido por el usuario")
        except Exception as e:
            logger.error(f"Error en el servicio: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(description='Recolecta periódicamente datos de reproducción reciente de múltiples usuarios de Spotify')
    parser.add_argument('--users_dir', required=True, help='Directorio donde están los archivos JSON de credenciales de usuarios')
    parser.add_argument('--output_base_dir', required=True, help='Directorio base donde se guardarán los CSV de datos')
    parser.add_argument('--interval', type=int, default=3600, help='Intervalo en segundos entre recolecciones (por defecto: 3600)')
    parser.add_argument('--once', action='store_true', help='Ejecutar solo una vez y salir')
    args = parser.parse_args()
    
    # Verificar si el directorio de usuarios existe
    if not os.path.isdir(args.users_dir):
        logger.error(f"El directorio de usuarios no existe: {args.users_dir}")
        return 1
    
    # Inicializar el colector de múltiples usuarios
    collector = SpotifyMultiUserCollector(
        users_dir=args.users_dir,
        output_base_dir=args.output_base_dir,
        interval_seconds=args.interval
    )
    
    # Ejecutar una vez o indefinidamente según las opciones
    if args.once:
        logger.info("Ejecutando recolección única para todos los usuarios")
        collector.run_once()
    else:
        logger.info("Iniciando servicio de recolección periódica")
        collector.run_forever()
    
    return 0

if __name__ == "__main__":
    exit(main())