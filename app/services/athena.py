import boto3
import pandas as pd
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class AthenaInsightsService:
    """
    Servicio para obtener insights musicales desde AWS Athena.
    
    Esta clase maneja todas las consultas necesarias para generar los cuatro insights
    fundamentales del dashboard: artistas top, patrones temporales, y análisis de popularidad.
    """
    
    def __init__(self, region_name='us-east-1', database_name='spotify_analytics', 
                 table_name='user_tracks', s3_output_bucket='itam-analytics-ragp'):
        """
        Inicializa el servicio de Athena con la configuración necesaria.
        
        Args:
            region_name: Región de AWS donde están los servicios
            database_name: Nombre de la base de datos en Glue Catalog
            table_name: Nombre de la tabla con los datos de Spotify
            s3_output_bucket: Bucket para almacenar resultados temporales de Athena
        """
        try:
            self.athena_client = boto3.client('athena', region_name=region_name)
            self.s3_client = boto3.client('s3', region_name=region_name)
            
            self.database_name = database_name
            self.table_name = table_name
            self.s3_output_location = f's3://{s3_output_bucket}/athena-results/'
            
            # Configuración de timeouts y reintentos
            self.query_timeout_seconds = 60
            self.max_retries = 3
            
            logger.info(f"Servicio Athena inicializado - DB: {database_name}, Tabla: {table_name}")
            
        except Exception as e:
            logger.error(f"Error inicializando servicio Athena: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Ejecuta una consulta en Athena y devuelve los resultados como DataFrame.
        
        Esta función maneja todo el ciclo de vida de una consulta Athena: envío,
        monitoreo del estado, y recuperación de resultados.
        
        Args:
            query: Consulta SQL para ejecutar
            
        Returns:
            DataFrame con los resultados de la consulta
        """
        logger.debug(f"Ejecutando consulta Athena: {query[:100]}...")
        
        try:
            # Enviar la consulta a Athena
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.debug(f"Query execution ID: {query_execution_id}")
            
            # Monitorear el estado de la consulta
            execution_status = self._wait_for_query_completion(query_execution_id)
            
            if execution_status != 'SUCCEEDED':
                raise Exception(f"Query falló con estado: {execution_status}")
            
            # Obtener los resultados
            results = self._get_query_results(query_execution_id)
            
            # Convertir a DataFrame para facilitar el manejo
            df = self._results_to_dataframe(results)
            
            logger.info(f"Consulta completada exitosamente. Filas obtenidas: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"Error ejecutando consulta Athena: {str(e)}")
            raise
    
    def _wait_for_query_completion(self, query_execution_id: str) -> str:
        """
        Espera a que una consulta Athena complete su ejecución.
        
        Args:
            query_execution_id: ID de ejecución de la consulta
            
        Returns:
            Estado final de la consulta (SUCCEEDED, FAILED, CANCELLED)
        """
        start_time = time.time()
        
        while True:
            # Verificar timeout
            if time.time() - start_time > self.query_timeout_seconds:
                logger.error(f"Query timeout después de {self.query_timeout_seconds} segundos")
                raise Exception("Query timeout")
            
            # Obtener estado actual
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'FAILED':
                    error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Query falló: {error_message}")
                return status
            
            # Esperar antes de verificar de nuevo
            time.sleep(2)
    
    def _get_query_results(self, query_execution_id: str) -> Dict:
        """
        Obtiene los resultados de una consulta completada.
        
        Args:
            query_execution_id: ID de ejecución de la consulta
            
        Returns:
            Diccionario con los resultados de la consulta
        """
        try:
            response = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            return response
        except Exception as e:
            logger.error(f"Error obteniendo resultados: {str(e)}")
            raise
    
    def _results_to_dataframe(self, results: Dict) -> pd.DataFrame:
        """
        Convierte los resultados de Athena a un DataFrame de pandas.
        
        Args:
            results: Resultados raw de Athena
            
        Returns:
            DataFrame con los datos procesados
        """
        try:
            # Extraer las filas de datos
            rows = results['ResultSet']['Rows']
            
            if len(rows) == 0:
                return pd.DataFrame()
            
            # La primera fila contiene los headers
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            
            # Las siguientes filas contienen los datos
            data_rows = []
            for row in rows[1:]:
                row_data = []
                for col in row['Data']:
                    # Manejar valores nulos
                    value = col.get('VarCharValue', None)
                    row_data.append(value)
                data_rows.append(row_data)
            
            # Crear DataFrame
            df = pd.DataFrame(data_rows, columns=headers)
            
            # Intentar convertir tipos de datos apropiados
            df = self._optimize_dataframe_types(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error convirtiendo resultados a DataFrame: {str(e)}")
            raise
    
    def _optimize_dataframe_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimiza los tipos de datos del DataFrame para mejor rendimiento.
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame con tipos optimizados
        """
        for col in df.columns:
            # Intentar convertir columnas numéricas
            if df[col].dtype == 'object':
                # Intentar convertir a numérico
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
        
        return df
    
    def get_top_artists(self, user_id: str, limit: int = 10, 
                       days_back: int = 90) -> List[Dict]:
        """
        Obtiene los artistas más escuchados por un usuario.
        
        Args:
            user_id: ID del usuario de Spotify
            limit: Número máximo de artistas a retornar
            days_back: Número de días hacia atrás para considerar
            
        Returns:
            Lista de diccionarios con artista y número de reproducciones
        """
        # Calcular fecha de inicio
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            artist_name,
            COUNT(*) as play_count,
            COUNT(DISTINCT track_id) as unique_tracks
        FROM {self.database_name}.{self.table_name}
        WHERE user_id = '{user_id}'
            AND date(played_at_mexico) >= date('{start_date}')
        GROUP BY artist_name
        ORDER BY play_count DESC
        LIMIT {limit}
        """
        
        try:
            df = self.execute_query(query)
            
            # Convertir a formato apropiado para el frontend
            result = []
            for _, row in df.iterrows():
                result.append({
                    'artist_name': row['artist_name'],
                    'play_count': int(row['play_count']),
                    'unique_tracks': int(row['unique_tracks'])
                })
            
            logger.info(f"Obtenidos {len(result)} artistas top para usuario {user_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error obteniendo artistas top: {str(e)}")
            return []
    
    def get_daily_listening_pattern(self, user_id: str, 
                                  days_back: int = 30) -> List[Dict]:
        """
        Obtiene el patrón de escucha por hora del día.
        
        Args:
            user_id: ID del usuario de Spotify
            days_back: Número de días hacia atrás para considerar
            
        Returns:
            Lista con las reproducciones por cada hora del día (0-23)
        """
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            play_hour,
            COUNT(*) as play_count
        FROM {self.database_name}.{self.table_name}
        WHERE user_id = '{user_id}'
            AND date(played_at_mexico) >= date('{start_date}')
        GROUP BY play_hour
        ORDER BY play_hour
        """
        
        try:
            df = self.execute_query(query)
            
            # Crear array completo de 24 horas (inicializado en 0)
            hourly_counts = [0] * 24
            
            # Llenar con datos reales
            for _, row in df.iterrows():
                hour = int(row['play_hour'])
                count = int(row['play_count'])
                hourly_counts[hour] = count
            
            # Convertir a formato para visualización
            result = []
            for hour in range(24):
                result.append({
                    'hour': hour,
                    'play_count': hourly_counts[hour],
                    'hour_label': f"{hour:02d}:00"
                })
            
            logger.info(f"Obtenido patrón diario para usuario {user_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error obteniendo patrón diario: {str(e)}")
            return []
    
    def get_weekday_vs_weekend_pattern(self, user_id: str, 
                                     days_back: int = 90) -> Dict:
        """
        Compara los patrones de escucha entre días de semana y fines de semana.
        
        Args:
            user_id: ID del usuario de Spotify
            days_back: Número de días hacia atrás para considerar
            
        Returns:
            Diccionario con estadísticas de días laborales vs fines de semana
        """
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            CASE 
                WHEN play_day_of_week IN (1, 7) THEN 'weekend'
                ELSE 'weekday'
            END as day_type,
            COUNT(*) as play_count,
            COUNT(DISTINCT date(played_at_mexico)) as active_days,
            AVG(CAST(popularity as DOUBLE)) as avg_popularity
        FROM {self.database_name}.{self.table_name}
        WHERE user_id = '{user_id}'
            AND date(played_at_mexico) >= date('{start_date}')
        GROUP BY 
            CASE 
                WHEN play_day_of_week IN (1, 7) THEN 'weekend'
                ELSE 'weekday'
            END
        """
        
        try:
            df = self.execute_query(query)
            
            # Estructura inicial con valores por defecto
            result = {
                'weekday': {
                    'play_count': 0, 
                    'active_days': 0, 
                    'avg_popularity': 0,
                    'percentage': 0.0
                },
                'weekend': {
                    'play_count': 0, 
                    'active_days': 0, 
                    'avg_popularity': 0,
                    'percentage': 0.0
                }
            }
            
            # Llenar con datos reales si existen
            for _, row in df.iterrows():
                day_type = row['day_type']
                if day_type in result:
                    result[day_type].update({
                        'play_count': int(row['play_count']),
                        'active_days': int(row['active_days']),
                        'avg_popularity': round(float(row['avg_popularity']) if row['avg_popularity'] is not None else 0, 1)
                    })
            
            # Calcular porcentajes
            total_plays = result['weekday']['play_count'] + result['weekend']['play_count']
            if total_plays > 0:
                result['weekday']['percentage'] = round((result['weekday']['play_count'] / total_plays) * 100, 1)
                result['weekend']['percentage'] = round((result['weekend']['play_count'] / total_plays) * 100, 1)
            
            logger.info(f"Obtenido patrón semanal para usuario {user_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error obteniendo patrón semanal: {str(e)}")
            # Devolver estructura válida en caso de error
            return {
                'weekday': {
                    'play_count': 0, 
                    'active_days': 0, 
                    'avg_popularity': 0,
                    'percentage': 0.0
                },
                'weekend': {
                    'play_count': 0, 
                    'active_days': 0, 
                    'avg_popularity': 0,
                    'percentage': 0.0
                }
            }
    
    def get_popularity_distribution(self, user_id: str, 
                                  days_back: int = 90) -> Dict:
        """
        Analiza la distribución de popularidad de los artistas escuchados.
        
        Args:
            user_id: ID del usuario de Spotify
            days_back: Número de días hacia atrás para considerar
            
        Returns:
            Diccionario con estadísticas de artistas emergentes vs establecidos
        """
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            CASE 
                WHEN popularity <= 40 THEN 'emergent'
                WHEN popularity <= 70 THEN 'growing'
                ELSE 'established'
            END as popularity_tier,
            COUNT(*) as play_count,
            COUNT(DISTINCT artist_id) as unique_artists,
            AVG(CAST(popularity as DOUBLE)) as avg_popularity
        FROM {self.database_name}.{self.table_name}
        WHERE user_id = '{user_id}'
            AND date(played_at_mexico) >= date('{start_date}')
            AND popularity IS NOT NULL
        GROUP BY 
            CASE 
                WHEN popularity <= 40 THEN 'emergent'
                WHEN popularity <= 70 THEN 'growing'
                ELSE 'established'
            END
        ORDER BY avg_popularity
        """
        
        try:
            df = self.execute_query(query)
            
            # Estructura inicial con valores por defecto
            result = {
                'emergent': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                'growing': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                'established': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0}
            }
            
            total_plays = 0
            
            # Llenar con datos reales si existen
            for _, row in df.iterrows():
                tier = row['popularity_tier']
                if tier in result:
                    play_count = int(row['play_count'])
                    total_plays += play_count
                    
                    result[tier].update({
                        'play_count': play_count,
                        'unique_artists': int(row['unique_artists']),
                        'avg_popularity': round(float(row['avg_popularity']) if row['avg_popularity'] is not None else 0, 1)
                    })
            
            # Calcular porcentajes
            if total_plays > 0:
                for tier in result:
                    result[tier]['percentage'] = round((result[tier]['play_count'] / total_plays) * 100, 1)
            
            # Determinar tier dominante
            dominant_tier = 'unknown'
            if total_plays > 0:
                dominant_tier = max(result.keys(), key=lambda x: result[x]['play_count'])
            
            # Agregar resumen general
            result['summary'] = {
                'total_plays': total_plays,
                'dominant_tier': dominant_tier
            }
            
            logger.info(f"Obtenida distribución de popularidad para usuario {user_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error obteniendo distribución de popularidad: {str(e)}")
            # Devolver estructura válida en caso de error
            return {
                'emergent': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                'growing': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                'established': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                'summary': {'total_plays': 0, 'dominant_tier': 'unknown'}
            }
    
    def get_user_insights_summary(self, user_id: str, days_back: int = 90) -> Dict:
        """
        Obtiene todos los insights principales para un usuario en una sola llamada.
        
        Esta función es útil para cargar el dashboard completo de manera eficiente.
        
        Args:
            user_id: ID del usuario de Spotify
            days_back: Número de días hacia atrás para considerar
            
        Returns:
            Diccionario con todos los insights principales
        """
        logger.info(f"Obteniendo resumen completo de insights para usuario {user_id}")
        
        try:
            insights = {
                'user_id': user_id,
                'period_days': days_back,
                'generated_at': datetime.now().isoformat(),
                'top_artists': self.get_top_artists(user_id, limit=10, days_back=days_back),
                'daily_pattern': self.get_daily_listening_pattern(user_id, days_back=min(days_back, 30)),
                'weekly_pattern': self.get_weekday_vs_weekend_pattern(user_id, days_back=days_back),
                'popularity_distribution': self.get_popularity_distribution(user_id, days_back=days_back)
            }
            
            logger.info(f"Resumen de insights completado para usuario {user_id}")
            return insights
            
        except Exception as e:
            logger.error(f"Error obteniendo resumen de insights: {str(e)}")
            # Devolver estructura válida con datos vacíos en caso de error
            return {
                'user_id': user_id,
                'period_days': days_back,
                'generated_at': datetime.now().isoformat(),
                'error': 'No se pudieron obtener los insights en este momento',
                'top_artists': [],
                'daily_pattern': [],
                'weekly_pattern': {
                    'weekday': {'play_count': 0, 'active_days': 0, 'avg_popularity': 0, 'percentage': 0.0},
                    'weekend': {'play_count': 0, 'active_days': 0, 'avg_popularity': 0, 'percentage': 0.0}
                },
                'popularity_distribution': {
                    'emergent': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                    'growing': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                    'established': {'play_count': 0, 'unique_artists': 0, 'percentage': 0.0, 'avg_popularity': 0.0},
                    'summary': {'total_plays': 0, 'dominant_tier': 'unknown'}
                }
            }

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