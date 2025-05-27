#!/usr/bin/env python3
"""
Script simple para generar perfiles musicales usando clustering K-Means.
Usa datos de Athena para crear 5 perfiles de usuario y guarda resultados en CSV.

Uso:
    python ml/scripts/generate_music_profiles.py
"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import boto3
import logging
import os
from datetime import datetime
import argparse

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mapeo de clusters a perfiles musicales
PROFILE_MAPPING = {
    0: {
        'name': 'Mainstream Explorer',
        'emoji': '🎯',
        'description': 'Te gustan los éxitos del momento y sigues las tendencias musicales',
        'characteristics': 'Popularidad alta, actividad media-alta, horario de tarde'
    },
    1: {
        'name': 'Underground Hunter', 
        'emoji': '🔍',
        'description': 'Descubres artistas antes que nadie y prefieres música alternativa',
        'characteristics': 'Popularidad baja, alta diversidad, escucha nocturna'
    },
    2: {
        'name': 'Music Addict',
        'emoji': '⚡',
        'description': 'La música es tu vida - escuchas constantemente y de todo',
        'characteristics': 'Actividad muy alta, gran diversidad, todo el día'
    },
    3: {
        'name': 'Night Owl',
        'emoji': '🌙', 
        'description': 'Tu momento musical es la noche - música para acompañar las horas tardías',
        'characteristics': 'Actividad nocturna, popularidad media, artistas selectos'
    },
    4: {
        'name': 'Casual Listener',
        'emoji': '🎵',
        'description': 'Escuchas música de fondo - prefieres lo conocido y familiar', 
        'characteristics': 'Baja actividad, popularidad media-alta, horario diurno'
    }
}

class MusicProfileGenerator:
    def __init__(self, region_name='us-east-1', database_name='spotify_analytics', table_name='user_tracks'):
        """Inicializa el generador de perfiles musicales"""
        self.athena_client = boto3.client('athena', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.database_name = database_name
        self.table_name = table_name
        self.s3_output_location = 's3://itam-analytics-ragp/athena-results/'
        
    def extract_user_features(self):
        """Extrae features completas desde múltiples tablas de Athena"""
        
        query = f"""
        WITH user_stats AS (
            SELECT 
                user_id,
                COUNT(*) as total_plays,
                COUNT(DISTINCT artist_id) as artist_diversity,
                COUNT(DISTINCT track_id) as track_diversity,
                AVG(CAST(popularity as DOUBLE)) as avg_popularity,
                AVG(CAST(play_hour as DOUBLE)) as peak_hour,
                AVG(CASE WHEN explicit = true THEN 1.0 ELSE 0.0 END) * 100 as explicit_percentage,
                AVG(CAST(duration_minutes as DOUBLE)) as avg_track_duration
            FROM {self.database_name}.user_tracks
            WHERE user_id IS NOT NULL 
                AND popularity IS NOT NULL
                AND play_hour IS NOT NULL
            GROUP BY user_id
            HAVING COUNT(*) >= 10
        ),
        user_likes AS (
            SELECT 
                user_id,
                COUNT(*) as total_likes,
                AVG(CAST(track_popularity as DOUBLE)) as avg_like_popularity,
                COUNT(DISTINCT artists_id[1]) as liked_artists_count  -- Primer artista de cada track
            FROM {self.database_name}.likes
            WHERE user_id IS NOT NULL
            GROUP BY user_id
        ),
        user_follows AS (
            SELECT 
                user_id,
                COUNT(*) as total_follows
            FROM {self.database_name}.followed_artists
            WHERE user_id IS NOT NULL
            GROUP BY user_id
        ),
        user_tops AS (
            SELECT 
                user_id,
                COUNT(*) as total_top_tracks,
                AVG(CAST(track_popularity as DOUBLE)) as avg_top_popularity
            FROM {self.database_name}.top_tracks
            WHERE user_id IS NOT NULL
            GROUP BY user_id
        )
        SELECT 
            us.user_id,
            us.total_plays as daily_activity,
            us.artist_diversity,
            us.track_diversity,
            us.avg_popularity,
            us.peak_hour,
            us.explicit_percentage,
            us.avg_track_duration,
            
            -- Features de engagement social
            COALESCE(ul.total_likes, 0) as total_likes,
            COALESCE(ul.avg_like_popularity, us.avg_popularity) as avg_like_popularity,
            COALESCE(ul.liked_artists_count, 0) as liked_artists_count,
            COALESCE(uf.total_follows, 0) as total_follows,
            COALESCE(ut.total_top_tracks, 0) as total_top_tracks,
            COALESCE(ut.avg_top_popularity, us.avg_popularity) as avg_top_popularity,
            
            -- Features calculadas (ratios)
            CASE 
                WHEN us.total_plays > 0 THEN CAST(COALESCE(ul.total_likes, 0) as DOUBLE) / us.total_plays * 100
                ELSE 0.0 
            END as like_ratio,
            
            CASE 
                WHEN us.artist_diversity > 0 THEN CAST(COALESCE(uf.total_follows, 0) as DOUBLE) / us.artist_diversity * 100
                ELSE 0.0 
            END as follow_ratio,
            
            -- Selectividad (cuánto le gusta vs cuánto escucha)
            CASE 
                WHEN us.artist_diversity > 0 THEN CAST(COALESCE(ul.liked_artists_count, 0) as DOUBLE) / us.artist_diversity * 100
                ELSE 0.0 
            END as like_selectivity
            
        FROM user_stats us
        LEFT JOIN user_likes ul ON us.user_id = ul.user_id
        LEFT JOIN user_follows uf ON us.user_id = uf.user_id  
        LEFT JOIN user_tops ut ON us.user_id = ut.user_id
        ORDER BY us.total_plays DESC
        """
        
        logger.info("Ejecutando query para extraer features de usuarios...")
        logger.info(f"Query: {query}")
        
        # Ejecutar query
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database_name},
            ResultConfiguration={'OutputLocation': self.s3_output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Esperar a que complete
        self._wait_for_query_completion(query_execution_id)
        
        # Obtener resultados
        results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Convertir a DataFrame
        df = self._results_to_dataframe(results)
        logger.info(f"Extraídas features para {len(df)} usuarios")
        
        return df
    
    def _wait_for_query_completion(self, query_execution_id, timeout_seconds=60):
        """Espera a que la query de Athena complete"""
        import time
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout_seconds:
                raise Exception("Query timeout")
            
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'FAILED':
                    error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    raise Exception(f"Query failed: {error_message}")
                return status
            
            time.sleep(2)
    
    def _results_to_dataframe(self, results):
        """Convierte resultados de Athena a DataFrame"""
        rows = results['ResultSet']['Rows']
        
        if len(rows) == 0:
            return pd.DataFrame()
        
        # Headers
        headers = [col['VarCharValue'] for col in rows[0]['Data']]
        
        # Data rows
        data_rows = []
        for row in rows[1:]:
            row_data = []
            for col in row['Data']:
                value = col.get('VarCharValue', None)
                row_data.append(value)
            data_rows.append(row_data)
        
        df = pd.DataFrame(data_rows, columns=headers)
        
        # Convertir tipos numéricos
        numeric_columns = ['avg_popularity', 'daily_activity', 'artist_diversity', 'peak_hour', 'explicit_percentage']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def generate_clusters(self, df, n_clusters=5):
        """Genera clusters usando K-Means"""
        logger.info(f"Generando {n_clusters} clusters para {len(df)} usuarios")
        
        # Preparar features para clustering
        feature_columns = ['avg_popularity', 'daily_activity', 'artist_diversity', 'peak_hour', 'explicit_percentage']
        X = df[feature_columns].fillna(0)  # Manejar NaN
        
        # Normalizar features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # K-Means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X_scaled)
        
        # Agregar clusters al DataFrame
        df['cluster'] = clusters
        
        # Mapear clusters a perfiles musicales usando heurísticas simples
        df['profile'] = df['cluster'].apply(self._map_cluster_to_profile)
        
        logger.info("Clustering completado")
        return df, kmeans, scaler
    
    def _map_cluster_to_profile(self, cluster_id):
        """Mapea un cluster ID a un perfil musical"""
        # Por simplicidad, usamos mapeo directo 
        # En un caso real, analizaríamos las características de cada cluster
        if cluster_id in PROFILE_MAPPING:
            return PROFILE_MAPPING[cluster_id]
        else:
            # Perfil por defecto
            return PROFILE_MAPPING[4]  # Casual Listener
    
    def analyze_clusters(self, df):
        """Analiza las características de cada cluster"""
        logger.info("Analizando características de clusters...")
        
        feature_columns = ['avg_popularity', 'daily_activity', 'artist_diversity', 'peak_hour', 'explicit_percentage']
        
        cluster_stats = []
        for cluster_id in sorted(df['cluster'].unique()):
            cluster_data = df[df['cluster'] == cluster_id]
            profile_name = cluster_data.iloc[0]['profile']['name']
            
            stats = {
                'cluster_id': cluster_id,
                'profile_name': profile_name,
                'user_count': len(cluster_data),
                'percentage': len(cluster_data) / len(df) * 100
            }
            
            # Estadísticas por feature
            for feature in feature_columns:
                stats[f'{feature}_mean'] = cluster_data[feature].mean()
                stats[f'{feature}_std'] = cluster_data[feature].std()
            
            cluster_stats.append(stats)
            
            logger.info(f"Cluster {cluster_id} ({profile_name}): {len(cluster_data)} usuarios ({stats['percentage']:.1f}%)")
        
        return pd.DataFrame(cluster_stats)
    
    def save_results(self, df, output_path='ml/data/user_music_profiles.csv'):
        """Guarda los resultados en CSV"""
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Preparar DataFrame para guardar
        output_df = df[['user_id', 'cluster']].copy()
        
        # Agregar información del perfil
        output_df['profile_name'] = df['profile'].apply(lambda x: x['name'])
        output_df['profile_emoji'] = df['profile'].apply(lambda x: x['emoji'])
        output_df['profile_description'] = df['profile'].apply(lambda x: x['description'])
        output_df['profile_characteristics'] = df['profile'].apply(lambda x: x['characteristics'])
        
        # Agregar timestamp
        output_df['generated_at'] = datetime.now().isoformat()
        
        # Guardar
        output_df.to_csv(output_path, index=False)
        logger.info(f"Resultados guardados en: {output_path}")
        
        return output_path
    
    def upload_to_s3(self, local_path, bucket='itam-analytics-ragp', s3_key='spotifire/ml/user_music_profiles.csv'):
        """Sube el archivo de perfiles a S3"""
        try:
            self.s3_client.upload_file(local_path, bucket, s3_key)
            logger.info(f"Archivo subido a S3: s3://{bucket}/{s3_key}")
            return f"s3://{bucket}/{s3_key}"
        except Exception as e:
            logger.error(f"Error subiendo a S3: {str(e)}")
            return None

def main():
    parser = argparse.ArgumentParser(description='Genera perfiles musicales usando clustering')
    parser.add_argument('--output', default='ml/data/user_music_profiles.csv', help='Archivo de salida')
    parser.add_argument('--upload-s3', action='store_true', help='Subir resultados a S3')
    args = parser.parse_args()
    
    logger.info("=== Iniciando generación de perfiles musicales ===")
    
    try:
        # Inicializar generador
        generator = MusicProfileGenerator()
        
        # Extraer features
        df = generator.extract_user_features()
        
        if len(df) == 0:
            logger.error("No se encontraron datos de usuarios")
            return 1
        
        # Generar clusters
        df_clustered, kmeans, scaler = generator.generate_clusters(df)
        
        # Analizar clusters
        cluster_stats = generator.analyze_clusters(df_clustered)
        logger.info("\n" + cluster_stats.to_string())
        
        # Guardar resultados
        output_path = generator.save_results(df_clustered, args.output)
        
        # Subir a S3 si se solicita
        if args.upload_s3:
            s3_path = generator.upload_to_s3(output_path)
            if s3_path:
                logger.info(f"✅ Perfiles disponibles en: {s3_path}")
        
        logger.info("=== Generación de perfiles completada exitosamente ===")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())