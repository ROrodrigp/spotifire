#!/usr/bin/env python3
"""
Analizador de dimensiones musicales usando AWS Bedrock.
Enfoque incremental inteligente: Usa LEFT JOIN para procesar solo canciones nuevas.
MODIFICADO: Preserva track_id para hacer JOIN correcto con tracks_psychological_analysis.
"""

import boto3
import json
import pandas as pd
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
import argparse

logger = logging.getLogger('bedrock_music_analyzer')

class MusicDimensionsAnalyzer:
    """
    Analizador simple que usa LEFT JOIN para evitar duplicados.
    Mucho más limpio que mantener archivos de tracking.
    MODIFICADO: Preserva track_id correctamente.
    """
    
    def __init__(self, region_name='us-east-1', database_name='spotify_analytics', aws_profile=None, model_id=None, batch_size=5):
        """Inicializa el analizador simple."""
        try:
            # Configuración de timeouts para Bedrock
            bedrock_config = boto3.session.Config(
                read_timeout=300,  # 5 minutos
                connect_timeout=60,  # 1 minuto para conectar
                retries={'max_attempts': 3}
            )
            
            athena_config = boto3.session.Config(
                read_timeout=120,  # 2 minutos para Athena
                connect_timeout=30
            )
            
            # Crear sesión de boto3 con perfil específico si se proporciona
            if aws_profile:
                logger.info(f"Usando perfil AWS: {aws_profile}")
                session = boto3.Session(profile_name=aws_profile)
                self.bedrock_runtime = session.client('bedrock-runtime', region_name=region_name, config=bedrock_config)
                self.athena_client = session.client('athena', region_name=region_name, config=athena_config)
            else:
                logger.info("Usando perfil AWS por defecto")
                self.bedrock_runtime = boto3.client('bedrock-runtime', region_name=region_name, config=bedrock_config)
                self.athena_client = boto3.client('athena', region_name=region_name, config=athena_config)
            
            self.region_name = region_name
            self.database_name = database_name
            self.aws_profile = aws_profile
            self.s3_output_location = 's3://itam-analytics-ragp/athena-results/'
            
            # Model ID con fallback a inference profile
            self.model_id = model_id or "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
            
            # Configuración optimizada para timeouts
            self.batch_size = batch_size  # Ahora configurable
            self.max_retries = 3
            self.delay_between_requests = 3  # Aumentado de 2 a 3 segundos
            self.request_timeout = 300  # 5 minutos para requests largos
            
            profile_info = f" (perfil: {aws_profile})" if aws_profile else " (perfil: default)"
            logger.info(f"Analizador simple inicializado para DB: {database_name}{profile_info}")
            logger.info(f"Usando modelo: {self.model_id}")
            logger.info(f"Tamaño de lote: {self.batch_size} canciones")
            
        except Exception as e:
            logger.error(f"Error inicializando analizador: {str(e)}")
            if aws_profile:
                logger.error(f"Verifica que el perfil '{aws_profile}' existe en ~/.aws/credentials")
            raise
    
    def get_unprocessed_songs_smart(self, limit: int = 50) -> pd.DataFrame:
        """
        Obtiene canciones que están en user_tracks pero NO en tracks_psychological_analysis.
        ¡Este es el enfoque elegante que sugeriste!
        IMPORTANTE: Incluye track_id para el JOIN posterior.
        """
        logger.info(f"Buscando canciones sin análisis psicológico (límite: {limit})")
        
        query = f"""
        SELECT DISTINCT 
            u.track_id,
            u.track_name,
            u.artist_name,
            u.album_name
        FROM {self.database_name}.user_tracks u
        LEFT JOIN {self.database_name}.tracks_psychological_analysis p 
            ON u.track_id = p.track_id
        WHERE u.track_name IS NOT NULL 
            AND u.artist_name IS NOT NULL
            AND u.album_name IS NOT NULL
            AND u.track_id IS NOT NULL
            AND p.track_id IS NULL  -- ¡Aquí está la magia!
        ORDER BY u.track_name
        LIMIT {limit}
        """
        
        try:
            # Ejecutar consulta
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Esperar resultado
            while True:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    raise Exception(f"Query falló: {error_msg}")
                
                time.sleep(2)
            
            # Obtener resultados
            results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Convertir a DataFrame
            if len(results['ResultSet']['Rows']) <= 1:
                logger.info("🎉 ¡No hay canciones pendientes de análisis!")
                return pd.DataFrame()
            
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = []
            
            for row in results['ResultSet']['Rows'][1:]:  # Skip header
                row_data = []
                for field in row['Data']:
                    row_data.append(field.get('VarCharValue', ''))
                rows.append(row_data)
            
            df = pd.DataFrame(rows, columns=columns)
            
            logger.info(f"✅ Encontradas {len(df)} canciones pendientes de análisis")
            logger.debug(f"Columnas obtenidas: {list(df.columns)}")
            
            # Verificar que tenemos track_id
            if 'track_id' not in df.columns:
                raise Exception("ERROR: track_id no está en los resultados de Athena")
            
            # Log de muestra para verificar datos
            if len(df) > 0:
                sample = df.iloc[0]
                logger.debug(f"Muestra de datos: track_id='{sample['track_id']}', track_name='{sample['track_name']}'")
            
            return df
            
        except Exception as e:
            logger.error(f"Error obteniendo canciones pendientes: {str(e)}")
            raise
    
    def get_analysis_statistics(self) -> Dict:
        """
        Obtiene estadísticas usando LEFT JOIN.
        ¡También más simple que mantener archivos!
        """
        query = f"""
        WITH analysis_stats AS (
            SELECT 
                COUNT(DISTINCT u.track_id) as total_tracks_available,
                COUNT(DISTINCT p.track_id) as tracks_analyzed,
                COUNT(DISTINCT CASE WHEN p.track_id IS NULL THEN u.track_id END) as tracks_pending
            FROM {self.database_name}.user_tracks u
            LEFT JOIN {self.database_name}.tracks_psychological_analysis p 
                ON u.track_id = p.track_id
            WHERE u.track_name IS NOT NULL 
                AND u.artist_name IS NOT NULL
                AND u.album_name IS NOT NULL
                AND u.track_id IS NOT NULL
        )
        SELECT 
            total_tracks_available,
            tracks_analyzed,
            tracks_pending,
            ROUND((tracks_analyzed * 100.0 / total_tracks_available), 2) as completion_percentage
        FROM analysis_stats
        """
        
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database_name},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            
            while True:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    return {'error': 'No se pudieron obtener estadísticas'}
                
                time.sleep(2)
            
            results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            if len(results['ResultSet']['Rows']) > 1:
                data = results['ResultSet']['Rows'][1]['Data']
                return {
                    'total_tracks_available': int(data[0]['VarCharValue']),
                    'tracks_analyzed': int(data[1]['VarCharValue']),
                    'tracks_pending': int(data[2]['VarCharValue']),
                    'completion_percentage': float(data[3]['VarCharValue'])
                }
            
            return {'error': 'No se encontraron datos'}
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {str(e)}")
            return {'error': str(e)}
    
    def create_analysis_prompt(self, songs_batch: List[Dict]) -> str:
        """
        Crea prompt para analizar lote de canciones.
        MODIFICADO: Incluye track_id explícitamente en cada canción.
        """
        songs_text = ""
        for i, song in enumerate(songs_batch, 1):
            # IMPORTANTE: Incluir track_id en la descripción para que Claude lo preserve
            songs_text += f"{i}. '{song['track_name']}' por {song['artist_name']} (Álbum: {song['album_name']}) [ID: {song['track_id']}]\n"
        
        # Crear ejemplos con los track_ids reales para el formato JSON
        json_examples = []
        for song in songs_batch:
            json_examples.append(f"""    {{
      "track_name": "{song['track_name']}",
      "artist_name": "{song['artist_name']}",
      "album_name": "{song['album_name']}",
      "track_id": "{song['track_id']}",
      "dimensiones": {{
        "energia_alta": 85,
        "energia_media": 20,
        "energia_baja": 5,
        // ... todas las dimensiones ...
      }}
    }}""")
        
        json_example_text = ",\n".join(json_examples[:2])  # Mostrar solo 2 ejemplos para no saturar
        
        prompt = f"""
Eres un musicólogo experto con profundo conocimiento en psicología musical y análisis emocional. Analiza las siguientes canciones y asigna porcentajes precisos (0-100) a cada dimensión.

CANCIONES A ANALIZAR:
{songs_text}

Para cada canción, analiza estas 29 dimensiones:

**ENERGÍA Y TEMPO:**
- energia_alta: Música que genera activación física y mental intensa
- energia_media: Música con energía moderada
- energia_baja: Música calmada, relajante
- tempo_rapido: Ritmo acelerado que invita al movimiento
- tempo_medio: Ritmo moderado, cómodo para diversas actividades
- tempo_lento: Ritmo pausado, ideal para reflexión

**ESPECTRO EMOCIONAL:**
- euforia: Alegría intensa, celebración, éxtasis musical
- melancolia: Tristeza bella, nostalgia, reflexión emocional
- serenidad: Paz interior, calma, equilibrio emocional
- intensidad_dramatica: Emociones fuertes, drama, pasión
- misterio: Atmósferas enigmáticas, suspense
- calidez: Sensación de confort, hogar, abrazo emocional

**CONTEXTOS SITUACIONALES:**
- ejercicio_deporte: Perfecta para actividad física y entrenamiento
- trabajo_concentracion: Ideal para tareas que requieren focus mental
- social_fiesta: Música para compartir, bailar, celebrar en grupo
- introspección: Para momentos de reflexión personal
- relajacion_descanso: Para descomprimir y liberar tensiones
- viaje_movimiento: Banda sonora ideal para desplazamientos

**DIMENSIONES CULTURALES:**
- nostalgia_retro: Evoca épocas pasadas, referencias vintage
- vanguardia_experimental: Sonidos innovadores, ruptura de convenciones
- authenticity_underground: Autenticidad cultural, alejado del mainstream
- universalidad: Apela ampliamente, trasciende barreras culturales
- regionalidad: Fuertemente conectado a una cultura específica
- atemporalidad: Trasciende épocas, suena relevante siempre

**EFECTOS PSICOLÓGICOS:**
- estimulacion_creativa: Cataliza pensamiento creativo
- procesamiento_emocional: Ayuda a procesar emociones complejas
- escape_mental: Proporciona desconexión de la realidad cotidiana
- motivacion_impulso: Genera determinación y fuerza de voluntad
- contemplacion_filosofica: Invita a reflexiones profundas sobre la existencia
- conexion_social: Facilita sentimientos de pertenencia y comunidad

CRÍTICO: Debes usar EXACTAMENTE los mismos track_id que aparecen entre [ID: ...] en la lista de canciones.

FORMATO DE RESPUESTA (JSON estricto):
```json
{{
  "analisis_musical": [
{json_example_text}
  ]
}}
```

Responde ÚNICAMENTE con el JSON válido, sin texto adicional.
Asegúrate de incluir TODOS los track_id correctos tal como aparecen en la lista.
"""
        return prompt
    
    def call_bedrock_claude(self, prompt: str) -> str:
        """Llama a Claude en Bedrock para analizar canciones."""
        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 8000,
                "temperature": 0.1,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            logger.debug(f"Enviando request a {self.model_id}...")
            logger.debug(f"Tamaño del prompt: ~{len(prompt)} caracteres")
            
            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json"
            )
            
            response_body = json.loads(response['body'].read().decode('utf-8'))
            logger.debug("Respuesta recibida exitosamente")
            return response_body['content'][0]['text']
            
        except Exception as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower():
                logger.error(f"Timeout al llamar a Bedrock (modelo puede estar sobrecargado): {error_msg}")
                logger.info("💡 Sugerencia: Prueba con un modelo más rápido como claude-3-5-haiku")
            elif "throttling" in error_msg.lower():
                logger.error(f"Rate limiting en Bedrock: {error_msg}")
                logger.info("💡 Sugerencia: Espera un momento antes de reintentar")
            else:
                logger.error(f"Error al llamar a Bedrock: {error_msg}")
            raise
    
    def parse_llm_response(self, response_text: str, expected_track_ids: List[str]) -> List[Dict]:
        """
        Parsea respuesta de Claude y valida que los track_ids coincidan.
        MODIFICADO: Validación adicional de track_ids.
        """
        try:
            response_text = response_text.strip()
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                raise ValueError("No se encontró JSON válido")
            
            json_text = response_text[start_idx:end_idx]
            parsed_response = json.loads(json_text)
            
            if 'analisis_musical' not in parsed_response:
                raise ValueError("Respuesta no contiene 'analisis_musical'")
            
            analyses = parsed_response['analisis_musical']
            
            # VALIDACIÓN CRÍTICA: Verificar que todos los track_ids están presentes
            returned_track_ids = [analysis.get('track_id') for analysis in analyses]
            missing_track_ids = set(expected_track_ids) - set(returned_track_ids)
            unexpected_track_ids = set(returned_track_ids) - set(expected_track_ids)
            
            if missing_track_ids:
                logger.warning(f"⚠️  Track IDs faltantes en respuesta: {missing_track_ids}")
            
            if unexpected_track_ids:
                logger.warning(f"⚠️  Track IDs inesperados en respuesta: {unexpected_track_ids}")
            
            # Filtrar solo análisis con track_ids válidos
            valid_analyses = []
            for analysis in analyses:
                if analysis.get('track_id') in expected_track_ids:
                    valid_analyses.append(analysis)
                    logger.debug(f"✅ Análisis válido para track_id: {analysis.get('track_id')}")
                else:
                    logger.warning(f"❌ Análisis descartado por track_id inválido: {analysis.get('track_id')}")
            
            logger.info(f"📊 Análisis válidos: {len(valid_analyses)}/{len(expected_track_ids)}")
            return valid_analyses
            
        except Exception as e:
            logger.error(f"Error parseando respuesta: {str(e)}")
            logger.debug(f"Respuesta problemática: {response_text[:500]}...")
            raise
    
    def analyze_songs_batch(self, songs_batch: List[Dict]) -> List[Dict]:
        """
        Analiza un lote de canciones usando Bedrock.
        MODIFICADO: Incluye validación de track_ids.
        """
        logger.info(f"🧠 Analizando lote de {len(songs_batch)} canciones con Claude...")
        
        # Extraer track_ids esperados para validación
        expected_track_ids = [song['track_id'] for song in songs_batch]
        logger.debug(f"Track IDs esperados: {expected_track_ids}")
        
        for attempt in range(self.max_retries):
            try:
                # Crear prompt y llamar a Bedrock
                prompt = self.create_analysis_prompt(songs_batch)
                response = self.call_bedrock_claude(prompt)
                analyses = self.parse_llm_response(response, expected_track_ids)
                
                logger.info(f"✅ Análisis completado para {len(analyses)} canciones")
                
                # Verificar que todos los track_ids fueron procesados
                if len(analyses) < len(expected_track_ids):
                    missing_count = len(expected_track_ids) - len(analyses)
                    logger.warning(f"⚠️  {missing_count} canciones no fueron analizadas completamente")
                
                return analyses
                
            except Exception as e:
                error_msg = str(e)
                
                if "timeout" in error_msg.lower():
                    wait_time = self.delay_between_requests * (attempt + 2)  # Delay progresivo para timeouts
                    logger.warning(f"⏰ Timeout en intento {attempt + 1}/{self.max_retries}")
                    logger.info(f"🔄 Reintentando en {wait_time} segundos...")
                elif "throttling" in error_msg.lower():
                    wait_time = self.delay_between_requests * (attempt + 3)  # Delay mayor para throttling
                    logger.warning(f"🚦 Rate limiting en intento {attempt + 1}/{self.max_retries}")
                    logger.info(f"🔄 Reintentando en {wait_time} segundos...")
                else:
                    wait_time = self.delay_between_requests * (attempt + 1)
                    logger.warning(f"❌ Intento {attempt + 1}/{self.max_retries} falló: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Análisis falló después de {self.max_retries} intentos")
                    
                    # Sugerencias específicas según el tipo de error
                    if "timeout" in error_msg.lower():
                        logger.error("💡 Soluciones sugeridas:")
                        logger.error("   • Usar --model-id us.anthropic.claude-3-5-haiku-20241022-v1:0 (más rápido)")
                        logger.error("   • Reducir --max-songs a 5 o menos")
                        logger.error("   • Esperar unos minutos y reintentar")
                    
                    raise
    
    def process_unprocessed_songs(self, max_songs: int = 50) -> Dict:
        """
        Procesa canciones que no tienen análisis psicológico.
        ¡Usa el enfoque simple con LEFT JOIN!
        MODIFICADO: Preserva track_ids para JOIN correcto.
        """
        logger.info(f"🚀 Procesando hasta {max_songs} canciones sin análisis")
        
        start_time = datetime.now()
        
        # Obtener estadísticas iniciales
        initial_stats = self.get_analysis_statistics()
        logger.info(f"📊 Estado inicial:")
        logger.info(f"   • Total canciones: {initial_stats.get('total_tracks_available', 0):,}")
        logger.info(f"   • Ya analizadas: {initial_stats.get('tracks_analyzed', 0):,}")
        logger.info(f"   • Pendientes: {initial_stats.get('tracks_pending', 0):,}")
        logger.info(f"   • Progreso: {initial_stats.get('completion_percentage', 0):.1f}%")
        
        # Obtener canciones pendientes usando LEFT JOIN
        unprocessed_df = self.get_unprocessed_songs_smart(limit=max_songs)
        
        if len(unprocessed_df) == 0:
            logger.info("🎉 ¡Todas las canciones ya tienen análisis psicológico!")
            return {
                'success': True,
                'songs_processed': 0,
                'message': 'No hay canciones pendientes de análisis',
                'stats': initial_stats
            }
        
        logger.info(f"🎯 Analizando {len(unprocessed_df)} canciones pendientes")
        
        # VERIFICACIÓN CRÍTICA: Asegurar que tenemos track_ids
        if 'track_id' not in unprocessed_df.columns:
            raise Exception("ERROR CRÍTICO: track_id no está disponible en los datos")
        
        # Verificar que no hay track_ids nulos
        null_track_ids = unprocessed_df['track_id'].isnull().sum()
        if null_track_ids > 0:
            logger.warning(f"⚠️  Encontrados {null_track_ids} track_ids nulos, filtrando...")
            unprocessed_df = unprocessed_df.dropna(subset=['track_id'])
        
        # Convertir a lista y procesar en lotes
        songs_list = unprocessed_df.to_dict('records')
        all_analyses = []
        
        # Log de muestra para verificar estructura
        if songs_list:
            sample_song = songs_list[0]
            logger.info(f"📝 Muestra de canción: {sample_song['track_name']} (ID: {sample_song['track_id']})")
        
        for i in range(0, len(songs_list), self.batch_size):
            batch = songs_list[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(songs_list) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"📦 Procesando lote {batch_num}/{total_batches}")
            
            # Log de track_ids del lote para debugging
            batch_track_ids = [song['track_id'] for song in batch]
            logger.debug(f"Track IDs del lote: {batch_track_ids}")
            
            try:
                batch_analyses = self.analyze_songs_batch(batch)
                all_analyses.extend(batch_analyses)
                
                # Verificar track_ids en el resultado del lote
                result_track_ids = [analysis['track_id'] for analysis in batch_analyses]
                logger.debug(f"Track IDs analizados: {result_track_ids}")
                
                # Pausa entre lotes
                if i + self.batch_size < len(songs_list):
                    time.sleep(self.delay_between_requests)
                    
            except Exception as e:
                logger.error(f"❌ Error en lote {batch_num}: {str(e)}")
                continue
        
        # Verificación final de track_ids
        final_track_ids = [analysis.get('track_id') for analysis in all_analyses]
        original_track_ids = [song['track_id'] for song in songs_list]
        
        logger.info(f"🔍 Verificación final:")
        logger.info(f"   • Track IDs originales: {len(original_track_ids)}")
        logger.info(f"   • Track IDs analizados: {len(final_track_ids)}")
        logger.info(f"   • Track IDs únicos analizados: {len(set(final_track_ids))}")
        
        missing_track_ids = set(original_track_ids) - set(final_track_ids)
        if missing_track_ids:
            logger.warning(f"⚠️  Track IDs no analizados: {len(missing_track_ids)}")
            logger.debug(f"Track IDs faltantes: {list(missing_track_ids)[:5]}...")
        
        # Calcular métricas de integridad para logging (no se guardan en el JSON)
        track_id_integrity = {
            'original_count': len(original_track_ids),
            'analyzed_count': len(final_track_ids),
            'missing_count': len(missing_track_ids),
            'success_rate': round((len(final_track_ids) / len(original_track_ids)) * 100, 1) if original_track_ids else 0
        }
        
        # Guardar resultados (solo metadata básica, sin métricas de integridad)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"analysis_with_track_ids_{timestamp}.json"
        
        output_data = {
            'metadata': {
                'generated_at': end_time.isoformat(),
                'duration_seconds': duration,
                'songs_processed': len(all_analyses),
                'approach': 'LEFT_JOIN_incremental_with_track_id_validation',
                'database': self.database_name,
                'aws_profile': self.aws_profile or 'default',
                'model_id': self.model_id
            },
            'analyses': all_analyses
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        # Obtener estadísticas finales
        final_stats = self.get_analysis_statistics()
        
        logger.info(f"🎉 Análisis completado!")
        logger.info(f"   • Canciones procesadas: {len(all_analyses)}")
        logger.info(f"   • Track IDs preservados: {len(set(final_track_ids))}")
        logger.info(f"   • Tasa de éxito: {track_id_integrity['success_rate']:.1f}%")
        logger.info(f"   • Duración: {duration:.1f} segundos")
        logger.info(f"   • Progreso actual: {final_stats.get('completion_percentage', 0):.1f}%")
        logger.info(f"   • Archivo guardado: {output_file}")
        
        return {
            'success': True,
            'songs_processed': len(all_analyses),
            'output_file': output_file,
            'duration_seconds': duration,
            'initial_stats': initial_stats,
            'final_stats': final_stats,
            'progress_improvement': final_stats.get('completion_percentage', 0) - initial_stats.get('completion_percentage', 0),
            'track_id_integrity': track_id_integrity
        }


def main():
    """Función principal con enfoque simple."""
    parser = argparse.ArgumentParser(
        description='Analiza canciones usando AWS Bedrock para generar dimensiones musicales avanzadas'
    )
    parser.add_argument(
        '--max-songs',
        type=int,
        default=50,
        help='Máximo de canciones a procesar (default: 50)'
    )
    parser.add_argument(
        '--database-name',
        default='spotify_analytics',
        help='Nombre de la base de datos (default: spotify_analytics)'
    )
    parser.add_argument(
        '--show-stats',
        action='store_true',
        help='Mostrar solo estadísticas sin procesar'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='Región AWS (default: us-east-1)'
    )
    parser.add_argument(
        '--aws-profile',
        help='Perfil AWS a usar (si no se especifica, usa el perfil por defecto)'
    )
    parser.add_argument(
        '--profile',
        dest='aws_profile',
        help='Alias para --aws-profile'
    )
    parser.add_argument(
        '--model-id',
        default="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        help='Model ID o Inference Profile ID a usar (default: us.anthropic.claude-3-5-sonnet-20241022-v2:0)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=5,
        help='Número de canciones por lote (default: 5, reduce si hay timeouts)'
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Mostrar información del perfil si se especifica
        if args.aws_profile:
            print(f"🔧 Usando perfil AWS: {args.aws_profile}")
        else:
            print(f"🔧 Usando perfil AWS por defecto")
        
        print(f"🤖 Usando modelo: {args.model_id}")
        print(f"📦 Tamaño de lote: {args.batch_size} canciones")
        
        # Inicializar analizador
        analyzer = MusicDimensionsAnalyzer(
            region_name=args.region,
            database_name=args.database_name,
            aws_profile=args.aws_profile,
            model_id=args.model_id,
            batch_size=args.batch_size
        )
        
        if args.show_stats:
            stats = analyzer.get_analysis_statistics()
            print("\n📊 Estadísticas de Análisis Musical")
            print("=" * 40)
            if 'error' in stats:
                print(f"❌ Error: {stats['error']}")
            else:
                print(f"Total canciones: {stats['total_tracks_available']:,}")
                print(f"Ya analizadas: {stats['tracks_analyzed']:,}")
                print(f"Pendientes: {stats['tracks_pending']:,}")
                print(f"Progreso: {stats['completion_percentage']:.1f}%")
                
                if stats['tracks_pending'] > 0:
                    profile_arg = f" --aws-profile {args.aws_profile}" if args.aws_profile else ""
                    model_arg = f" --model-id {args.model_id}" if args.model_id != "us.anthropic.claude-3-5-sonnet-20241022-v2:0" else ""
                    print(f"\n🚀 Para continuar:")
                    print(f"python3 {__file__}{profile_arg}{model_arg} --max-songs {min(stats['tracks_pending'], 100)}")
                else:
                    print(f"\n🎉 ¡Análisis completo!")
            return
        
        # Procesar canciones pendientes
        result = analyzer.process_unprocessed_songs(max_songs=args.max_songs)
        
        if result['success']:
            print(f"\n🎉 ¡Procesamiento completado!")
            print(f"📈 Canciones analizadas: {result['songs_processed']}")
            print(f"🔑 Track IDs preservados: {result['track_id_integrity']['analyzed_count']}")
            print(f"✅ Tasa de éxito: {result['track_id_integrity']['success_rate']:.1f}%")
            print(f"⏱️  Duración: {result['duration_seconds']:.1f} segundos")
            print(f"📄 Archivo: {result['output_file']}")
            
            if 'progress_improvement' in result:
                print(f"📊 Mejora en progreso: +{result['progress_improvement']:.1f}%")
            
            # Advertencias si hay problemas con track_ids
            track_integrity = result['track_id_integrity']
            if track_integrity['missing_count'] > 0:
                print(f"⚠️  Advertencia: {track_integrity['missing_count']} canciones no se procesaron completamente")
            
            final_stats = result.get('final_stats', {})
            if final_stats.get('tracks_pending', 0) > 0:
                profile_arg = f" --aws-profile {args.aws_profile}" if args.aws_profile else ""
                model_arg = f" --model-id {args.model_id}" if args.model_id != "us.anthropic.claude-3-5-sonnet-20241022-v2:0" else ""
                print(f"\n🔄 Para continuar:")
                print(f"python3 {__file__}{profile_arg}{model_arg} --max-songs 50")
            else:
                print(f"\n🎉 ¡Todas las canciones han sido analizadas!")
        else:
            print(f"❌ Error durante el procesamiento")
        
    except KeyboardInterrupt:
        print("\n⏹️  Proceso interrumpido por el usuario")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        if args.aws_profile:
            print(f"💡 Verifica que el perfil '{args.aws_profile}' existe en ~/.aws/credentials")
            print(f"💡 O usa: aws configure --profile {args.aws_profile}")
        
        # Sugerencias específicas para errores de Bedrock
        error_msg = str(e)
        if "ValidationException" in error_msg and "inference profile" in error_msg:
            print(f"\n🔧 Error de modelo Bedrock detectado. Prueba con:")
            print(f"   • --model-id anthropic.claude-3-sonnet-20240229-v1:0")
            print(f"   • --model-id us.anthropic.claude-3-5-sonnet-20241022-v2:0")
            print(f"   • Verificar modelos disponibles en AWS Bedrock Console")
        elif "timeout" in error_msg.lower():
            print(f"\n⏰ Error de timeout detectado. Prueba con:")
            print(f"   • --model-id us.anthropic.claude-3-5-haiku-20241022-v1:0 (más rápido)")
            print(f"   • --batch-size 3 (lotes más pequeños)")
            print(f"   • --max-songs 5 (menos canciones por ejecución)")
            print(f"   • Esperar unos minutos y reintentar")
        
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())