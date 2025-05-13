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
        
        # Intentar diferentes formatos de archivo JSON
        if "access_token" in self.credentials:
            # Este es un formato de token completo de OAuth (como los guardados por app.py)
            logger.info("Detectado token completo de OAuth en el archivo")
            
            # Si el archivo tiene client_id y client_secret directamente
            if "client_id" in self.credentials and "client_secret" in self.credentials:
                logger.info("Usando client_id y client_secret del archivo")
                client_id = self.credentials.get("client_id")
                client_secret = self.credentials.get("client_secret")
                redirect_uri = self.credentials.get("redirect_uri", "http://localhost:8888/callback")
            else:
                # Si no tiene credenciales, intentar variables de entorno
                logger.info("Token sin credenciales, usando variables de entorno")
                client_id = os.environ.get("SPOTIPY_CLIENT_ID")
                client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
                redirect_uri = os.environ.get("SPOTIPY_REDIRECT_URI", "http://localhost:8888/callback")
                
                if not client_id or not client_secret:
                    logger.error("No se encontraron credenciales ni en el archivo ni en variables de entorno")
                    raise ValueError("No se encontraron credenciales para autenticación")
            
            # Configurar OAuth con token existente
            auth_manager = SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope=scope,
                open_browser=False,
                cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
            )
            
            # Si hay un refresh_token, usarlo para inicializar correctamente
            if "refresh_token" in self.credentials:
                # El archivo ya incluye un token, podemos configurar la cache para spotipy
                logger.info("Usando refresh_token del archivo")
                token_info = {
                    "access_token": self.credentials.get("access_token"),
                    "refresh_token": self.credentials.get("refresh_token"),
                    "expires_at": self.credentials.get("expires_at", 0),
                    "scope": self.credentials.get("scope", scope),
                    "token_type": self.credentials.get("token_type", "Bearer")
                }
                
                # Guardar en el cache de spotipy
                auth_manager.cache_handler.save_token_to_cache(token_info)
                logger.info("Token guardado en el cache de spotipy")
        
        elif "client_id" in self.credentials and "client_secret" in self.credentials:
            # Formato estándar con client_id y client_secret
            logger.info("Usando formato estándar con client_id y client_secret")
            auth_manager = SpotifyOAuth(
                client_id=self.credentials.get("client_id"),
                client_secret=self.credentials.get("client_secret"),
                redirect_uri=self.credentials.get("redirect_uri", "http://localhost:8888/callback"),
                scope=scope,
                open_browser=False,
                cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
            )
        elif "id" in self.credentials:
            # Formato alternativo
            logger.info("Usando formato alternativo con 'id' como client_id")
            auth_manager = SpotifyOAuth(
                client_id=self.credentials.get("id"),
                client_secret=self.credentials.get("secret", self.credentials.get("client_secret")),
                redirect_uri=self.credentials.get("uri", self.credentials.get("redirect_uri", "http://localhost:8888/callback")),
                scope=scope,
                open_browser=False,
                cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
            )
        elif "SPOTIPY_CLIENT_ID" in os.environ and "SPOTIPY_CLIENT_SECRET" in os.environ:
            # Usar variables de entorno si están disponibles
            logger.info("Usando credenciales de variables de entorno")
            auth_manager = SpotifyOAuth(
                scope=scope,
                open_browser=False,
                cache_path=f".spotify_cache_{os.path.basename(self.credentials_file)}"
            )
        else:
            # Imprimir el contenido del archivo para depuración
            logger.error(f"Formato de credenciales no reconocido. Contenido: {json.dumps(self.credentials, indent=2)}")
            raise ValueError("Formato de credenciales no válido. Necesita contener 'client_id' y 'client_secret' o configurar variables de entorno SPOTIPY_CLIENT_ID y SPOTIPY_CLIENT_SECRET")
        
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
    parser.add_argument('--check-credentials', action='store_true', help='Solo verifica el formato de las credenciales sin ejecutar el servicio')
    args = parser.parse_args()
    
    # Verificar si el archivo de credenciales existe
    if not os.path.isfile(args.credentials_file):
        logger.error(f"El archivo de credenciales no existe: {args.credentials_file}")
        return 1
    
    # Solo verificar las credenciales si se solicita
    if args.check_credentials:
        try:
            with open(args.credentials_file, 'r') as f:
                credentials = json.load(f)
                print(f"Contenido del archivo de credenciales: {json.dumps(credentials, indent=2)}")
                print("Verificando si tiene los campos necesarios...")
                
                if "client_id" in credentials and "client_secret" in credentials:
                    print("✅ El archivo contiene los campos necesarios (client_id, client_secret)")
                elif "id" in credentials:
                    print("✅ El archivo contiene 'id', que se puede usar como client_id")
                else:
                    print("❌ El archivo no contiene los campos necesarios")
                    print("Campos requeridos:")
                    print("  - client_id y client_secret")
                    print("  O")
                    print("  - id y secret (o client_secret)")
                    print("\nAlternativamente, puedes configurar las variables de entorno:")
                    print("  export SPOTIPY_CLIENT_ID='tu-client-id'")
                    print("  export SPOTIPY_CLIENT_SECRET='tu-client-secret'")
                    print("  export SPOTIPY_REDIRECT_URI='tu-redirect-uri'")
            return 0
        except Exception as e:
            logger.error(f"Error al verificar credenciales: {e}")
            return 1
    
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