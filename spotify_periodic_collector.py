#!/usr/bin/env python3
"""
Servicio para recolectar periódicamente datos de reproducción reciente de Spotify
y guardarlos en archivos CSV con timestamp.

Uso:
    python spotify_periodic_collector.py --credentials_file RUTA_AL_ARCHIVO.json --output_dir DIRECTORIO_SALIDA

Ejemplo:
    python spotify_periodic_collector.py --credentials_file 8fc0525b8843481fb3716bfb9cf15ba3.json --output_dir ./datos_spotify
"""

import os
import time
import json
import csv
import argparse
import logging
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

class SpotifyPeriodicCollector:
    def __init__(self, credentials_file, output_dir, interval_seconds=3600):
        """
        Inicializa el recolector periódico de datos de Spotify.
        
        Args:
            credentials_file: Ruta al archivo JSON con las credenciales
            output_dir: Directorio donde se guardarán los CSV de datos
            interval_seconds: Intervalo en segundos entre recolecciones (por defecto 1 hora)
        """
        self.credentials_file = credentials_file
        self.output_dir = output_dir
        self.interval_seconds = interval_seconds
        self.user_id = None
        
        # Asegurar que el directorio de salida existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Cargar credenciales desde el archivo JSON
        try:
            with open(credentials_file, 'r') as f:
                self.credentials = json.load(f)
                logger.info(f"Credenciales cargadas desde {credentials_file}")
        except Exception as e:
            logger.error(f"Error al cargar credenciales: {e}")
            raise
        
        # Configurar la autenticación de Spotify
        try:
            self.sp = self._setup_spotify_client()
            self.user_id = self.sp.current_user()['id']
            logger.info(f"Cliente de Spotify configurado para el usuario: {self.user_id}")
        except Exception as e:
            logger.error(f"Error al configurar cliente de Spotify: {e}")
            raise

    def _setup_spotify_client(self):
        """Configura y devuelve un cliente autenticado de Spotify"""
        # Spotipy requiere las credenciales en el formato adecuado
        scope = "user-read-recently-played"
        
        auth_manager = SpotifyOAuth(
            client_id=self.credentials.get("client_id"),
            client_secret=self.credentials.get("client_secret"),
            redirect_uri=self.credentials.get("redirect_uri"),
            scope=scope,
            open_browser=False,
            cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
        )
        
        return spotipy.Spotify(auth_manager=auth_manager)
    
    def get_recently_played(self):
        """Obtiene las canciones reproducidas recientemente por el usuario"""
        try:
            results = self.sp.current_user_recently_played(limit=50)
            logger.info(f"Obtenidas {len(results['items'])} canciones recientes")
            return results['items']
        except Exception as e:
            logger.error(f"Error al obtener canciones recientes: {e}")
            return []
    
    def save_to_csv(self, data):
        """Guarda los datos en un archivo CSV con timestamp"""
        if not data:
            logger.warning("No hay datos para guardar")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        user_dir = os.path.join(self.output_dir, self.user_id)
        os.makedirs(user_dir, exist_ok=True)
        
        filename = os.path.join(user_dir, f"recently_played_{timestamp}.csv")
        
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
                
                logger.info(f"Datos guardados en {filename}")
                return filename
        except Exception as e:
            logger.error(f"Error al guardar datos en CSV: {e}")
            return None
    
    def run_once(self):
        """Ejecuta una única recolección de datos"""
        data = self.get_recently_played()
        return self.save_to_csv(data)
    
    def run_forever(self):
        """Ejecuta el servicio de recolección periódica indefinidamente"""
        logger.info(f"Iniciando servicio de recolección periódica cada {self.interval_seconds} segundos")
        
        try:
            while True:
                filename = self.run_once()
                if filename:
                    logger.info(f"Próxima recolección en {self.interval_seconds} segundos")
                
                # Esperar hasta la próxima recolección
                time.sleep(self.interval_seconds)
        except KeyboardInterrupt:
            logger.info("Servicio detenido por el usuario")
        except Exception as e:
            logger.error(f"Error en el servicio: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Recolecta periódicamente datos de reproducción reciente de Spotify')
    parser.add_argument('--credentials_file', required=True, help='Ruta al archivo JSON con las credenciales')
    parser.add_argument('--output_dir', required=True, help='Directorio donde se guardarán los CSV de datos')
    parser.add_argument('--interval', type=int, default=3600, help='Intervalo en segundos entre recolecciones (por defecto: 3600)')
    args = parser.parse_args()
    
    try:
        collector = SpotifyPeriodicCollector(
            credentials_file=args.credentials_file,
            output_dir=args.output_dir,
            interval_seconds=args.interval
        )
        collector.run_forever()
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())