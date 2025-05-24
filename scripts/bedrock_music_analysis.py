#!/usr/bin/env python3
"""
Analizador de dimensiones musicales usando AWS Bedrock.
Enfoque incremental inteligente: Usa LEFT JOIN para procesar solo canciones nuevas.
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
    """
    
    def __init__(self, region_name='us-east-1', database_name='spotify_analytics', aws_profile=None, model_id=None):
        """Inicializa el analizador simple."""
        try:
            # Crear sesión de boto3 con perfil específico si se proporciona
            if aws_profile:
                logger.info(f"Usando perfil AWS: {aws_profile}")
                session = boto3.Session(profile_name=aws_profile)
                self.bedrock_runtime = session.client('bedrock-runtime', region_name=region_name)
                self.athena_client = session.client('athena', region_name=region_name)
            else:
                logger.info("Usando perfil AWS por defecto")
                self.bedrock_runtime = boto3.client('bedrock-runtime', region_name=region_name)
                self.athena_client = boto3.client('athena', region_name=region_name)
            
            self.region_name = region_name
            self.database_name = database_name
            self.aws_profile = aws_profile
            self.s3_output_location = 's3://itam-analytics-ragp/athena-results/'
            
            # Model ID con fallback a inference profile
            self.model_id = model_id or "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
            
            # Configuración
            self.batch_size = 10
            self.max_retries = 3
            self.delay_between_requests = 2
            
            profile_info = f" (perfil: {aws_profile})" if aws_profile else " (perfil: default)"
            logger.info(f"Analizador simple inicializado para DB: {database_name}{profile_info}")
            logger.info(f"Usando modelo: {self.model_id}")
            
        except Exception as e:
            logger.error(f"Error inicializando analizador: {str(e)}")
            if aws_profile:
                logger.error(f"Verifica que el perfil '{aws_profile}' existe en ~/.aws/credentials")
            raise
    
    def get_unprocessed_songs_smart(self, limit: int = 50) -> pd.DataFrame:
        """
        Obtiene canciones que están en user_tracks pero NO en tracks_psychological_analysis.
        ¡Este es el enfoque elegante que sugeriste!
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
        """Crea prompt para analizar lote de canciones (método original)."""
        songs_text = ""
        for i, song in enumerate(songs_batch, 1):
            songs_text += f"{i}. '{song['track_name']}' por {song['artist_name']} (Álbum: {song['album_name']})\n"
        
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

FORMATO DE RESPUESTA (JSON estricto):
```json
{{
  "analisis_musical": [
    {{
      "track_name": "nombre_exacto_cancion",
      "artist_name": "nombre_exacto_artista",
      "album_name": "nombre_exacto_album",
      "track_id": "track_id_exacto",
      "dimensiones": {{
        "energia_alta": 85,
        "energia_media": 20,
        "energia_baja": 5,
        "tempo_rapido": 90,
        "tempo_medio": 15,
        "tempo_lento": 5,
        "euforia": 80,
        "melancolia": 10,
        "serenidad": 15,
        "intensidad_dramatica": 70,
        "misterio": 20,
        "calidez": 40,
        "ejercicio_deporte": 95,
        "trabajo_concentracion": 30,
        "social_fiesta": 90,
        "introspección": 10,
        "relajacion_descanso": 5,
        "viaje_movimiento": 80,
        "nostalgia_retro": 20,
        "vanguardia_experimental": 40,
        "authenticity_underground": 60,
        "universalidad": 70,
        "regionalidad": 30,
        "atemporalidad": 50,
        "estimulacion_creativa": 75,
        "procesamiento_emocional": 25,
        "escape_mental": 80,
        "motivacion_impulso": 90,
        "contemplacion_filosofica": 15,
        "conexion_social": 85
      }}
    }}
  ]
}}
```

Responde ÚNICAMENTE con el JSON válido, sin texto adicional.
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
            
            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json"
            )
            
            response_body = json.loads(response['body'].read().decode('utf-8'))
            return response_body['content'][0]['text']
            
        except Exception as e:
            logger.error(f"Error llamando a Bedrock: {str(e)}")
            raise
    
    def parse_llm_response(self, response_text: str) -> List[Dict]:
        """Parsea respuesta de Claude."""
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
            
            return parsed_response['analisis_musical']
            
        except Exception as e:
            logger.error(f"Error parseando respuesta: {str(e)}")
            raise
    
    def analyze_songs_batch(self, songs_batch: List[Dict]) -> List[Dict]:
        """Analiza un lote de canciones usando Bedrock."""
        logger.info(f"🧠 Analizando lote de {len(songs_batch)} canciones con Claude...")
        
        for attempt in range(self.max_retries):
            try:
                # Crear prompt y llamar a Bedrock
                prompt = self.create_analysis_prompt(songs_batch)
                response = self.call_bedrock_claude(prompt)
                analyses = self.parse_llm_response(response)
                
                logger.info(f"✅ Análisis completado para {len(analyses)} canciones")
                return analyses
                
            except Exception as e:
                logger.warning(f"Intento {attempt + 1} falló: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay_between_requests * (attempt + 1))
                else:
                    logger.error(f"❌ Análisis falló después de {self.max_retries} intentos")
                    raise
    
    def process_unprocessed_songs(self, max_songs: int = 50) -> Dict:
        """
        Procesa canciones que no tienen análisis psicológico.
        ¡Usa el enfoque simple con LEFT JOIN!
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
        
        # Convertir a lista y procesar en lotes
        songs_list = unprocessed_df.to_dict('records')
        all_analyses = []
        
        for i in range(0, len(songs_list), self.batch_size):
            batch = songs_list[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(songs_list) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"📦 Procesando lote {batch_num}/{total_batches}")
            
            try:
                batch_analyses = self.analyze_songs_batch(batch)
                all_analyses.extend(batch_analyses)
                
                # Pausa entre lotes
                if i + self.batch_size < len(songs_list):
                    time.sleep(self.delay_between_requests)
                    
            except Exception as e:
                logger.error(f"❌ Error en lote {batch_num}: {str(e)}")
                continue
        
        # Guardar resultados
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"simple_analysis_{timestamp}.json"
        
        output_data = {
            'metadata': {
                'generated_at': end_time.isoformat(),
                'duration_seconds': duration,
                'songs_processed': len(all_analyses),
                'approach': 'LEFT_JOIN_incremental',
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
            'progress_improvement': final_stats.get('completion_percentage', 0) - initial_stats.get('completion_percentage', 0)
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
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Mostrar información del perfil si se especifica
        if args.aws_profile:
            print(f"🔧 Usando perfil AWS: {args.aws_profile}")
        else:
            print(f"🔧 Usando perfil AWS por defecto")
        
        print(f"🤖 Usando modelo: {args.model_id}")
        
        # Inicializar analizador
        analyzer = MusicDimensionsAnalyzer(
            region_name=args.region,
            database_name=args.database_name,
            aws_profile=args.aws_profile,
            model_id=args.model_id
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
            print(f"⏱️  Duración: {result['duration_seconds']:.1f} segundos")
            print(f"📄 Archivo: {result['output_file']}")
            
            if 'progress_improvement' in result:
                print(f"📊 Mejora en progreso: +{result['progress_improvement']:.1f}%")
            
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
        
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())