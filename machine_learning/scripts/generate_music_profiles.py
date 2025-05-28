#!/usr/bin/env python3
"""
Script para generar perfiles musicales usando clustering K-Means.
Usa datos de Athena para crear 5 perfiles de usuario con lógica de mapeo balanceada.

Uso:
    python ml/scripts/generate_music_profiles.py
"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, RobustScaler
import boto3
import logging
import os
from datetime import datetime
import argparse

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mapeo de perfiles musicales con criterios más específicos
PROFILE_MAPPING = {
    'mainstream_explorer': {
        'name': 'Mainstream Explorer',
        'emoji': '🎯',
        'description': 'Te gustan los éxitos del momento y sigues las tendencias musicales',
        'characteristics': 'Popularidad alta (>65), actividad media, horario diurno',
        'criteria': 'high_popularity + moderate_activity + day_listening'
    },
    'underground_hunter': {
        'name': 'Underground Hunter', 
        'emoji': '🔍',
        'description': 'Descubres artistas antes que nadie y prefieres música alternativa',
        'characteristics': 'Popularidad baja (<45), alta diversidad, exploración alta',
        'criteria': 'low_popularity + high_diversity + high_exploration'
    },
    'music_addict': {
        'name': 'Music Addict',
        'emoji': '⚡',
        'description': 'La música es tu vida - escuchas constantemente y de todo',
        'characteristics': 'Actividad muy alta (>200), diversidad alta, todo el día',
        'criteria': 'very_high_activity + high_diversity + consistent_listening'
    },
    'night_owl': {
        'name': 'Night Owl',
        'emoji': '🌙', 
        'description': 'Tu momento musical es la noche - música para acompañar las horas tardías',
        'characteristics': 'Escucha nocturna (>22h o <6h), actividad selectiva',
        'criteria': 'night_listening + selective_activity'
    },
    'casual_listener': {
        'name': 'Casual Listener',
        'emoji': '🎵',
        'description': 'Escuchas música de fondo - prefieres lo conocido y familiar', 
        'characteristics': 'Baja actividad (<80), popularidad media, horario regular',
        'criteria': 'low_activity + moderate_popularity + regular_schedule'
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
        """Extrae features completas desde múltiples tablas de Athena con limpieza de outliers"""
        
        query = f"""
        WITH user_stats AS (
            SELECT 
                user_id,
                COUNT(*) as total_plays,
                COUNT(DISTINCT artist_id) as artist_diversity,
                COUNT(DISTINCT track_id) as track_diversity,
                -- Limpiar outliers de popularidad (0-100 es el rango válido)
                AVG(CASE 
                    WHEN CAST(popularity as DOUBLE) BETWEEN 0 AND 100 
                    THEN CAST(popularity as DOUBLE) 
                    ELSE NULL 
                END) as avg_popularity,
                AVG(CAST(play_hour as DOUBLE)) as peak_hour,
                AVG(CASE WHEN explicit = true THEN 1.0 ELSE 0.0 END) * 100 as explicit_percentage,
                AVG(CAST(duration_minutes as DOUBLE)) as avg_track_duration,
                STDDEV(CASE 
                    WHEN CAST(popularity as DOUBLE) BETWEEN 0 AND 100 
                    THEN CAST(popularity as DOUBLE) 
                    ELSE NULL 
                END) as popularity_variance,
                -- Análisis temporal mejorado
                COUNT(CASE WHEN play_hour BETWEEN 22 AND 23 OR play_hour BETWEEN 0 AND 5 THEN 1 END) as night_plays,
                COUNT(CASE WHEN play_hour BETWEEN 6 AND 21 THEN 1 END) as day_plays,
                COUNT(CASE WHEN play_hour BETWEEN 9 AND 17 THEN 1 END) as work_hours_plays,
                -- Patrones de fin de semana
                COUNT(CASE WHEN play_day_of_week IN (1, 7) THEN 1 END) as weekend_plays,
                COUNT(CASE WHEN play_day_of_week BETWEEN 2 AND 6 THEN 1 END) as weekday_plays
            FROM {self.database_name}.user_tracks
            WHERE user_id IS NOT NULL 
                AND play_hour IS NOT NULL
                AND play_day_of_week IS NOT NULL
            GROUP BY user_id
            HAVING COUNT(*) >= 20
        ),
        user_likes AS (
            SELECT 
                user_id,
                COUNT(*) as total_likes,
                AVG(CASE 
                    WHEN CAST(track_popularity as DOUBLE) BETWEEN 0 AND 100 
                    THEN CAST(track_popularity as DOUBLE) 
                    ELSE NULL 
                END) as avg_like_popularity,
                COUNT(DISTINCT artists_id) as liked_artists_count
            FROM {self.database_name}.likes
            WHERE user_id IS NOT NULL
                AND artists_id IS NOT NULL
                AND artists_id != ''
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
                AVG(CASE 
                    WHEN CAST(track_popularity as DOUBLE) BETWEEN 0 AND 100 
                    THEN CAST(track_popularity as DOUBLE) 
                    ELSE NULL 
                END) as avg_top_popularity,
                COUNT(DISTINCT artists_id) as top_artists_diversity
            FROM {self.database_name}.top_tracks
            WHERE user_id IS NOT NULL
                AND artists_id IS NOT NULL
                AND artists_id != ''
            GROUP BY user_id
        )
        SELECT 
            us.user_id,
            us.total_plays as daily_activity,
            us.artist_diversity,
            us.track_diversity,
            COALESCE(us.avg_popularity, 50.0) as avg_popularity,
            us.peak_hour,
            us.explicit_percentage,
            us.avg_track_duration,
            COALESCE(us.popularity_variance, 0.0) as popularity_variance,
            
            -- Patrones temporales normalizados
            CASE 
                WHEN (us.night_plays + us.day_plays) > 0 
                THEN CAST(us.night_plays as DOUBLE) / (us.night_plays + us.day_plays) * 100
                ELSE 0.0 
            END as night_preference_ratio,
            
            CASE 
                WHEN (us.weekend_plays + us.weekday_plays) > 0 
                THEN CAST(us.weekend_plays as DOUBLE) / (us.weekend_plays + us.weekday_plays) * 100
                ELSE 0.0 
            END as weekend_preference_ratio,
            
            CASE 
                WHEN us.total_plays > 0 
                THEN CAST(us.work_hours_plays as DOUBLE) / us.total_plays * 100
                ELSE 0.0 
            END as work_hours_ratio,
            
            -- Features de engagement social
            COALESCE(ul.total_likes, 0) as total_likes,
            COALESCE(ul.avg_like_popularity, us.avg_popularity) as avg_like_popularity,
            COALESCE(ul.liked_artists_count, 0) as liked_artists_count,
            COALESCE(uf.total_follows, 0) as total_follows,
            COALESCE(ut.total_top_tracks, 0) as total_top_tracks,
            COALESCE(ut.avg_top_popularity, us.avg_popularity) as avg_top_popularity,
            COALESCE(ut.top_artists_diversity, 0) as top_artists_diversity,
            
            -- Features calculadas (ratios)
            CASE 
                WHEN us.total_plays > 0 THEN CAST(COALESCE(ul.total_likes, 0) as DOUBLE) / us.total_plays * 100
                ELSE 0.0 
            END as like_ratio,
            
            CASE 
                WHEN us.artist_diversity > 0 THEN CAST(COALESCE(uf.total_follows, 0) as DOUBLE) / us.artist_diversity * 100
                ELSE 0.0 
            END as follow_ratio,
            
            -- Selectividad y exploración
            CASE 
                WHEN us.artist_diversity > 0 THEN CAST(COALESCE(ul.liked_artists_count, 0) as DOUBLE) / us.artist_diversity * 100
                ELSE 0.0 
            END as like_selectivity,
            
            CASE 
                WHEN us.total_plays > 0 THEN CAST(us.track_diversity as DOUBLE) / us.total_plays * 100
                ELSE 0.0 
            END as exploration_ratio,
            
            -- Intensidad de escucha por día
            CASE 
                WHEN us.total_plays > 0 
                THEN us.total_plays / 30.0  -- Asumiendo datos de ~30 días
                ELSE 0.0 
            END as daily_listening_intensity
            
        FROM user_stats us
        LEFT JOIN user_likes ul ON us.user_id = ul.user_id
        LEFT JOIN user_follows uf ON us.user_id = uf.user_id  
        LEFT JOIN user_tops ut ON us.user_id = ut.user_id
        ORDER BY us.total_plays DESC
        """
        
        logger.info("Ejecutando query para extraer features de usuarios...")
        
        # Ejecutar query
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database_name},
            ResultConfiguration={'OutputLocation': self.s3_output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Query execution ID: {query_execution_id}")
        
        # Esperar a que complete
        self._wait_for_query_completion(query_execution_id)
        
        # Obtener resultados
        results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Convertir a DataFrame
        df = self._results_to_dataframe(results)
        logger.info(f"Extraídas features para {len(df)} usuarios")
        
        return df
    
    def _wait_for_query_completion(self, query_execution_id, timeout_seconds=120):
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
                    logger.error(f"Query execution details: {response['QueryExecution']}")
                    raise Exception(f"Query failed: {error_message}")
                return status
            
            time.sleep(3)
    
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
        numeric_columns = [col for col in df.columns if col != 'user_id']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Rellenar NaN con valores apropiados
        df = df.fillna(0)
        
        return df
    
    def generate_clusters(self, df, n_clusters=5):
        """Genera clusters usando K-Means con preprocesamiento robusto"""
        logger.info(f"Generando {n_clusters} clusters para {len(df)} usuarios")
        
        # Preparar features para clustering - más balanceadas
        feature_columns = [
            'avg_popularity',               # Qué tan mainstream es
            'daily_listening_intensity',    # Intensidad de escucha diaria
            'artist_diversity',            # Diversidad de artistas
            'night_preference_ratio',      # Preferencia nocturna
            'weekend_preference_ratio',    # Preferencia de fin de semana
            'exploration_ratio',           # Qué tan explorador es
            'like_ratio',                  # Engagement con likes
            'popularity_variance',         # Consistencia en popularidad
            'peak_hour'                    # Hora pico de escucha
        ]
        
        # Filtrar columnas que existen
        available_features = [col for col in feature_columns if col in df.columns]
        logger.info(f"Features disponibles para clustering: {available_features}")
        
        X = df[available_features].fillna(0)
        
        # Usar RobustScaler para manejar mejor los outliers
        scaler = RobustScaler()
        X_scaled = scaler.fit_transform(X)
        
        # K-Means clustering con más iteraciones
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=20, max_iter=500)
        clusters = kmeans.fit_predict(X_scaled)
        
        # Agregar clusters al DataFrame
        df['cluster'] = clusters
        
        # Analizar las características de cada cluster
        cluster_characteristics = self._analyze_cluster_characteristics(df, available_features)
        
        # Mapear clusters a perfiles usando lógica mejorada
        df['profile'] = df.apply(lambda row: self._map_user_to_profile_improved(row, cluster_characteristics), axis=1)
        
        logger.info("Clustering completado")
        return df, kmeans, scaler
    
    def _analyze_cluster_characteristics(self, df, feature_columns):
        """Analiza las características de cada cluster"""
        cluster_chars = {}
        
        for cluster_id in sorted(df['cluster'].unique()):
            cluster_data = df[df['cluster'] == cluster_id]
            
            chars = {}
            for feature in feature_columns:
                if feature in cluster_data.columns:
                    chars[feature] = cluster_data[feature].mean()
            
            cluster_chars[cluster_id] = chars
            
            logger.info(f"Cluster {cluster_id} características ({len(cluster_data)} usuarios):")
            logger.info(f"  - Popularidad promedio: {chars.get('avg_popularity', 0):.1f}")
            logger.info(f"  - Intensidad diaria: {chars.get('daily_listening_intensity', 0):.1f}")
            logger.info(f"  - Diversidad de artistas: {chars.get('artist_diversity', 0):.1f}")
            logger.info(f"  - Preferencia nocturna: {chars.get('night_preference_ratio', 0):.1f}%")
            logger.info(f"  - Exploración: {chars.get('exploration_ratio', 0):.1f}%")
        
        return cluster_chars
    
    def _map_user_to_profile_improved(self, user_row, cluster_characteristics):
        """Mapea un usuario individual a un perfil usando lógica mejorada y balanceada"""
        
        # Obtener características del usuario
        avg_pop = user_row.get('avg_popularity', 50)
        intensity = user_row.get('daily_listening_intensity', 5)
        diversity = user_row.get('artist_diversity', 10)
        night_pref = user_row.get('night_preference_ratio', 30)
        exploration = user_row.get('exploration_ratio', 20)
        peak_hour = user_row.get('peak_hour', 12)
        weekend_pref = user_row.get('weekend_preference_ratio', 40)
        like_ratio = user_row.get('like_ratio', 5)
        
        # Calculamos percentiles dentro del dataset para umbrales adaptativos
        cluster_id = user_row.get('cluster', 0)
        cluster_chars = cluster_characteristics.get(cluster_id, {})
        
        # Lógica de clasificación más granular y balanceada
        scores = {}
        
        # Score para Mainstream Explorer
        mainstream_score = 0
        if avg_pop > 65:  # Alta popularidad
            mainstream_score += 3
        elif avg_pop > 55:
            mainstream_score += 1
        
        if intensity > 3 and intensity < 15:  # Actividad moderada
            mainstream_score += 2
        
        if peak_hour >= 9 and peak_hour <= 18:  # Horario diurno
            mainstream_score += 2
            
        scores['mainstream_explorer'] = mainstream_score
        
        # Score para Underground Hunter  
        underground_score = 0
        if avg_pop < 45:  # Baja popularidad
            underground_score += 3
        elif avg_pop < 55:
            underground_score += 1
            
        if exploration > 25:  # Alta exploración
            underground_score += 2
        elif exploration > 15:
            underground_score += 1
            
        if diversity > 30:  # Alta diversidad
            underground_score += 2
            
        scores['underground_hunter'] = underground_score
        
        # Score para Music Addict
        addict_score = 0
        if intensity > 15:  # Muy alta actividad
            addict_score += 4
        elif intensity > 8:
            addict_score += 2
            
        if diversity > 50:  # Muy alta diversidad
            addict_score += 2
        elif diversity > 25:
            addict_score += 1
            
        if exploration > 20:  # Buena exploración
            addict_score += 1
            
        scores['music_addict'] = addict_score
        
        # Score para Night Owl
        night_score = 0
        if night_pref > 40:  # Alta preferencia nocturna
            night_score += 3
        elif night_pref > 25:
            night_score += 1
            
        if peak_hour >= 22 or peak_hour <= 6:  # Escucha nocturna
            night_score += 3
        elif peak_hour >= 20:
            night_score += 1
            
        if weekend_pref > 60:  # Prefiere fines de semana
            night_score += 1
            
        scores['night_owl'] = night_score
        
        # Score para Casual Listener (default/catchall mejorado)
        casual_score = 0
        if intensity < 8:  # Baja actividad
            casual_score += 2
            
        if avg_pop >= 50 and avg_pop <= 70:  # Popularidad moderada
            casual_score += 2
            
        if exploration < 20:  # Baja exploración
            casual_score += 1
            
        if like_ratio < 10:  # Baja interacción
            casual_score += 1
            
        scores['casual_listener'] = casual_score
        
        # Seleccionar el perfil con mayor score
        # Si hay empate, usar un orden de prioridad
        priority_order = ['music_addict', 'underground_hunter', 'night_owl', 'mainstream_explorer', 'casual_listener']
        
        max_score = max(scores.values())
        if max_score == 0:
            # Si no hay scores positivos, asignar Casual Listener
            selected_profile = 'casual_listener'
        else:
            # Buscar el perfil con mayor score siguiendo el orden de prioridad
            selected_profile = 'casual_listener'
            for profile in priority_order:
                if scores[profile] == max_score:
                    selected_profile = profile
                    break
        
        return PROFILE_MAPPING[selected_profile]
    
    def analyze_clusters(self, df):
        """Analiza las características de cada cluster"""
        logger.info("Analizando distribución de perfiles...")
        
        # Agrupar por perfil (no por cluster)
        profile_stats = []
        for profile_key, profile_info in PROFILE_MAPPING.items():
            profile_users = df[df['profile'].apply(lambda x: x['name'] == profile_info['name'])]
            
            if len(profile_users) > 0:
                stats = {
                    'profile_name': profile_info['name'],
                    'profile_emoji': profile_info['emoji'],
                    'user_count': len(profile_users),
                    'percentage': len(profile_users) / len(df) * 100,
                    'avg_popularity_mean': profile_users['avg_popularity'].mean(),
                    'daily_intensity_mean': profile_users.get('daily_listening_intensity', pd.Series([0])).mean(),
                    'artist_diversity_mean': profile_users['artist_diversity'].mean(),
                    'night_preference_mean': profile_users.get('night_preference_ratio', pd.Series([0])).mean(),
                }
                profile_stats.append(stats)
                
                logger.info(f"{profile_info['emoji']} {profile_info['name']}: {len(profile_users)} usuarios ({stats['percentage']:.1f}%)")
        
        return pd.DataFrame(profile_stats)
    
    def save_results(self, df, profile_stats, output_path='ml/data/user_music_profiles.csv'):
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
        output_df['profile_criteria'] = df['profile'].apply(lambda x: x['criteria'])
        
        # Agregar métricas clave normalizadas
        key_metrics = ['avg_popularity', 'daily_listening_intensity', 'artist_diversity', 
                      'night_preference_ratio', 'exploration_ratio', 'peak_hour']
        for metric in key_metrics:
            if metric in df.columns:
                output_df[metric] = df[metric].round(2)
        
        # Agregar timestamp
        output_df['generated_at'] = datetime.now().isoformat()
        
        # Guardar perfiles de usuario
        output_df.to_csv(output_path, index=False)
        logger.info(f"Resultados guardados en: {output_path}")
        
        # Guardar estadísticas de perfiles (mantener nombre original para compatibilidad)
        stats_path = output_path.replace('.csv', '_cluster_stats.csv')
        profile_stats.to_csv(stats_path, index=False)
        logger.info(f"Estadísticas de perfiles guardadas en: {stats_path}")
        
        return output_path, stats_path
    
    def upload_to_s3(self, local_path, bucket='itam-analytics-ragp', s3_key=None):
        """Sube el archivo de perfiles a S3"""
        try:
            if s3_key is None:
                filename = os.path.basename(local_path)
                s3_key = f'spotifire/ml/{filename}'
            
            self.s3_client.upload_file(local_path, bucket, s3_key)
            logger.info(f"Archivo subido a S3: s3://{bucket}/{s3_key}")
            return f"s3://{bucket}/{s3_key}"
        except Exception as e:
            logger.error(f"Error subiendo a S3: {str(e)}")
            return None

def main():
    parser = argparse.ArgumentParser(description='Genera perfiles musicales usando clustering K-Means')
    parser.add_argument('--output', default='ml/data/user_music_profiles.csv', help='Archivo de salida')
    parser.add_argument('--upload-s3', action='store_true', help='Subir resultados a S3')
    parser.add_argument('--n-clusters', type=int, default=5, help='Número de clusters (default: 5)')
    args = parser.parse_args()
    
    logger.info("=== Iniciando generación de perfiles musicales ===")
    
    try:
        # Inicializar generador
        generator = MusicProfileGenerator()
        
        # Extraer features
        logger.info("Paso 1: Extrayendo features de usuarios...")
        df = generator.extract_user_features()
        
        if len(df) == 0:
            logger.error("No se encontraron datos de usuarios")
            return 1
        
        logger.info(f"Features extraídas para {len(df)} usuarios")
        
        # Mostrar estadísticas básicas de los datos
        logger.info("\nEstadísticas de features principales:")
        key_cols = ['avg_popularity', 'daily_listening_intensity', 'artist_diversity', 'night_preference_ratio']
        for col in key_cols:
            if col in df.columns:
                logger.info(f"  {col}: min={df[col].min():.1f}, max={df[col].max():.1f}, mean={df[col].mean():.1f}")
        
        # Generar clusters
        logger.info("\nPaso 2: Generando clusters...")
        df_clustered, kmeans, scaler = generator.generate_clusters(df, n_clusters=args.n_clusters)
        
        # Analizar perfiles
        logger.info("\nPaso 3: Analizando perfiles...")
        profile_stats = generator.analyze_clusters(df_clustered)
        
        logger.info("\nDistribución de perfiles:")
        for _, row in profile_stats.iterrows():
            logger.info(f"  {row['profile_emoji']} {row['profile_name']}: {row['user_count']} usuarios ({row['percentage']:.1f}%)")
        
        # Guardar resultados
        logger.info("\nPaso 4: Guardando resultados...")
        output_path, stats_path = generator.save_results(df_clustered, profile_stats, args.output)
        
        # Subir a S3 si se solicita
        if args.upload_s3:
            logger.info("\nPaso 5: Subiendo a S3...")
            s3_path = generator.upload_to_s3(output_path)
            stats_s3_path = generator.upload_to_s3(stats_path)
            if s3_path:
                logger.info(f"✅ Perfiles disponibles en: {s3_path}")
            if stats_s3_path:
                logger.info(f"✅ Estadísticas disponibles en: {stats_s3_path}")
        
        logger.info("\n=== Generación de perfiles completada exitosamente ===")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit(main())