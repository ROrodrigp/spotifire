import pandas as pd
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class MusicProfileService:
    """
    Servicio simple para obtener perfiles musicales de usuarios.
    Carga datos desde CSV (local o S3) y proporciona lookup rápido.
    """
    
    def __init__(self, 
                 local_path='ml/data/user_music_profiles.csv',
                 s3_bucket='itam-analytics-ragp', 
                 s3_key='spotifire/ml/user_music_profiles.csv',
                 region_name='us-east-1'):
        """
        Inicializa el servicio de perfiles musicales.
        
        Args:
            local_path: Ruta local del archivo CSV
            s3_bucket: Bucket de S3 donde está el archivo
            s3_key: Key del archivo en S3
            region_name: Región de AWS
        """
        self.local_path = local_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region_name = region_name
        
        # Diccionario para lookup rápido: user_id -> profile_data
        self.user_profiles = {}
        self.profile_stats = {}
        self.last_updated = None
        
        # Intentar cargar datos
        self.load_profiles()
    
    def load_profiles(self):
        """Carga los perfiles desde archivo (local primero, S3 como fallback)"""
        try:
            # Intentar cargar desde archivo local primero
            if os.path.exists(self.local_path):
                logger.info(f"Cargando perfiles desde archivo local: {self.local_path}")
                df = pd.read_csv(self.local_path)
                self._process_profiles_dataframe(df)
                return True
            
            # Si no existe local, intentar descargar desde S3
            logger.info(f"Archivo local no encontrado, intentando descargar desde S3...")
            if self._download_from_s3():
                df = pd.read_csv(self.local_path)
                self._process_profiles_dataframe(df)
                return True
            
            # Si no hay datos disponibles, usar perfiles por defecto
            logger.warning("No se pudieron cargar perfiles, usando datos por defecto")
            self._load_default_profiles()
            return False
            
        except Exception as e:
            logger.error(f"Error cargando perfiles: {str(e)}")
            self._load_default_profiles()
            return False
    
    def _download_from_s3(self):
        """Descarga el archivo de perfiles desde S3"""
        try:
            s3_client = boto3.client('s3', region_name=self.region_name)
            
            # Crear directorio local si no existe
            os.makedirs(os.path.dirname(self.local_path), exist_ok=True)
            
            # Descargar archivo
            s3_client.download_file(self.s3_bucket, self.s3_key, self.local_path)
            logger.info(f"Archivo descargado desde s3://{self.s3_bucket}/{self.s3_key}")
            return True
            
        except Exception as e:
            logger.warning(f"No se pudo descargar desde S3: {str(e)}")
            return False
    
    def _process_profiles_dataframe(self, df):
        """Procesa el DataFrame de perfiles y crea estructuras de lookup"""
        logger.info(f"Procesando {len(df)} perfiles de usuario")
        
        # Crear diccionario de lookup
        self.user_profiles = {}
        profile_counts = {}
        
        for _, row in df.iterrows():
            user_id = str(row['user_id'])
            profile_data = {
                'cluster': int(row['cluster']) if pd.notna(row['cluster']) else 0,
                'name': row.get('profile_name', 'Casual Listener'),
                'emoji': row.get('profile_emoji', '🎵'),
                'description': row.get('profile_description', 'Perfil musical estándar'),
                'characteristics': row.get('profile_characteristics', ''),
                'generated_at': row.get('generated_at', datetime.now().isoformat())
            }
            
            self.user_profiles[user_id] = profile_data
            
            # Contar perfiles para estadísticas
            profile_name = profile_data['name']
            profile_counts[profile_name] = profile_counts.get(profile_name, 0) + 1
        
        # Calcular estadísticas de perfiles
        total_users = len(df)
        self.profile_stats = {}
        for profile_name, count in profile_counts.items():
            self.profile_stats[profile_name] = {
                'user_count': count,
                'percentage': round((count / total_users) * 100, 1) if total_users > 0 else 0
            }
        
        self.last_updated = datetime.now().isoformat()
        logger.info(f"Perfiles cargados exitosamente. Última actualización: {self.last_updated}")
    
    def _load_default_profiles(self):
        """Carga perfiles por defecto en caso de error"""
        logger.info("Cargando perfiles por defecto")
        
        # Perfil por defecto para cualquier usuario
        default_profile = {
            'cluster': 4,
            'name': 'Casual Listener',
            'emoji': '🎵',
            'description': 'Escuchas música de fondo - prefieres lo conocido y familiar',
            'characteristics': 'Perfil musical estándar',
            'generated_at': datetime.now().isoformat()
        }
        
        self.user_profiles = {}  # Vacío - se usará el perfil por defecto
        self.profile_stats = {
            'Casual Listener': {'user_count': 1, 'percentage': 100.0}
        }
        self.last_updated = datetime.now().isoformat()
    
    def get_user_profile(self, user_id: str) -> Dict:
        """
        Obtiene el perfil musical de un usuario específico.
        
        Args:
            user_id: ID del usuario
            
        Returns:
            Diccionario con información del perfil musical
        """
        user_id = str(user_id)
        
        if user_id in self.user_profiles:
            profile = self.user_profiles[user_id].copy()
            
            # Agregar estadísticas del perfil
            profile_name = profile['name']
            if profile_name in self.profile_stats:
                profile['stats'] = self.profile_stats[profile_name]
            
            logger.debug(f"Perfil encontrado para usuario {user_id}: {profile['name']}")
            return profile
        
        # Usuario no encontrado - retornar perfil por defecto
        logger.debug(f"Usuario {user_id} no encontrado, usando perfil por defecto")
        return {
            'cluster': 4,
            'name': 'Casual Listener',
            'emoji': '🎵',
            'description': 'Escuchas música de fondo - prefieres lo conocido y familiar',
            'characteristics': 'Perfil musical estándar',
            'generated_at': datetime.now().isoformat(),
            'stats': {'user_count': 1, 'percentage': 0}
        }
    
    def get_all_profile_stats(self) -> Dict:
        """
        Obtiene estadísticas de todos los perfiles musicales.
        
        Returns:
            Diccionario con estadísticas por perfil
        """
        return {
            'profiles': self.profile_stats,
            'total_users': len(self.user_profiles),
            'last_updated': self.last_updated,
            'available_profiles': list(self.profile_stats.keys())
        }
    
    def refresh_profiles(self) -> bool:
        """
        Refresca los perfiles desde S3 (útil para actualizaciones en vivo).
        
        Returns:
            True si se actualizó exitosamente, False en caso contrario
        """
        logger.info("Refrescando perfiles musicales...")
        
        # Forzar descarga desde S3
        if self._download_from_s3():
            return self.load_profiles()
        
        logger.warning("No se pudieron refrescar los perfiles")
        return False
    
    def is_service_available(self) -> bool:
        """
        Verifica si el servicio está disponible y tiene datos.
        
        Returns:
            True si el servicio tiene datos cargados
        """
        return len(self.user_profiles) > 0 or self.profile_stats is not None