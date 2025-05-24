#!/usr/bin/env python3
"""
Sistema de análisis musical avanzado usando AWS Bedrock
Este módulo usa LLMs para categorizar canciones en dimensiones psicológicas y emocionales profundas
"""

import boto3
import json
import pandas as pd
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import argparse
import sys
from botocore.exceptions import ClientError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bedrock_music_analysis.log')
    ]
)
logger = logging.getLogger('bedrock_music_analyzer')

class MusicDimensionsAnalyzer:
    """
    Analizador de dimensiones musicales usando AWS Bedrock.
    
    Esta clase usa LLMs para analizar canciones y asignar porcentajes
    a diferentes dimensiones psicológicas, emocionales y culturales.
    """
    
    def __init__(self, region_name='us-east-1'):
        """
        Inicializa el analizador con los clientes de AWS necesarios.
        """
        try:
            self.bedrock_runtime = boto3.client('bedrock-runtime', region_name=region_name)
            self.athena_client = boto3.client('athena', region_name=region_name)
            self.glue_client = boto3.client('glue', region_name=region_name)
            self.s3_client = boto3.client('s3', region_name=region_name)
            
            self.region_name = region_name
            self.s3_output_location = 's3://itam-analytics-ragp/athena-results/'
            
            # Configuraciones para el análisis
            self.batch_size = 10  # Procesar de a 10 canciones por request
            self.max_retries = 3
            self.delay_between_requests = 2  # segundos
            
            logger.info(f"MusicDimensionsAnalyzer inicializado en región {region_name}")
            
        except Exception as e:
            logger.error(f"Error inicializando MusicDimensionsAnalyzer: {str(e)}")
            raise
    
    def get_music_dimensions_schema(self) -> Dict:
        """
        Define el esquema completo de dimensiones musicales para el análisis.
        
        Returns:
            Diccionario con todas las dimensiones y sus descripciones
        """
        return {
            "energia_y_tempo": {
                "energia_alta": "Música que genera activación física y mental intensa",
                "energia_media": "Música con energía moderada, ni muy intensa ni muy relajada", 
                "energia_baja": "Música calmada, relajante o contemplativa",
                "tempo_rapido": "Ritmo acelerado que invita al movimiento",
                "tempo_medio": "Ritmo moderado, cómodo para diversas actividades",
                "tempo_lento": "Ritmo pausado, ideal para reflexión o relajación"
            },
            "espectro_emocional": {
                "euforia": "Alegría intensa, celebración, éxtasis musical",
                "melancolia": "Tristeza bella, nostalgia, reflexión emocional",
                "serenidad": "Paz interior, calma, equilibrio emocional", 
                "intensidad_dramatica": "Emociones fuertes, drama, pasión",
                "misterio": "Atmósferas enigmáticas, suspense, lo inexplorado",
                "calidez": "Sensación de confort, hogar, abrazo emocional"
            },
            "contextos_situacionales": {
                "ejercicio_deporte": "Perfecta para actividad física y entrenamiento",
                "trabajo_concentracion": "Ideal para tareas que requieren focus mental",
                "social_fiesta": "Música para compartir, bailar, celebrar en grupo",
                "introspección": "Para momentos de reflexión personal y autoconocimiento",
                "relajacion_descanso": "Para descomprimir y liberar tensiones",
                "viaje_movimiento": "Banda sonora ideal para desplazamientos y aventuras"
            },
            "dimensiones_culturales": {
                "nostalgia_retro": "Evoca épocas pasadas, referencias vintage",
                "vanguardia_experimental": "Sonidos innovadores, ruptura de convenciones", 
                "authenticity_underground": "Autenticidad cultural, alejado del mainstream",
                "universalidad": "Apela ampliamente, trasciende barreras culturales",
                "regionalidad": "Fuertemente conectado a una cultura o región específica",
                "atemporalidad": "Trasciende épocas, suena relevante en cualquier momento"
            },
            "efectos_psicologicos": {
                "estimulacion_creativa": "Cataliza pensamiento creativo y artistic thinking",
                "procesamiento_emocional": "Ayuda a procesar y entender emociones complejas",
                "escape_mental": "Proporciona desconexión de la realidad cotidiana",
                "motivacion_impulso": "Genera determinación y fuerza de voluntad", 
                "contemplacion_filosofica": "Invita a reflexiones profundas sobre la existencia",
                "conexion_social": "Facilita sentimientos de pertenencia y comunidad"
            }
        }
    
    def create_analysis_prompt(self, songs_batch: List[Dict]) -> str:
        """
        Crea el prompt para el LLM que analizará las canciones.
        
        Args:
            songs_batch: Lista de canciones para analizar
            
        Returns:
            Prompt estructurado para el análisis
        """
        dimensions_schema = self.get_music_dimensions_schema()
        
        # Construir la lista de canciones para analizar
        songs_text = ""
        for i, song in enumerate(songs_batch, 1):
            songs_text += f"{i}. '{song['track_name']}' por {song['artist_name']} (Álbum: {song['album_name']})\n"
        
        prompt = f"""
Eres un musicólogo experto con profundo conocimiento en psicología musical, análisis emocional de audio y antropología cultural. Tu tarea es analizar las siguientes canciones y asignar porcentajes precisos a cada dimensión musical.

CANCIONES A ANALIZAR:
{songs_text}

DIMENSIONES PARA ANALIZAR:

**ENERGÍA Y TEMPO:**
- energia_alta: Música que genera activación física y mental intensa
- energia_media: Música con energía moderada, ni muy intensa ni muy relajada
- energia_baja: Música calmada, relajante o contemplativa  
- tempo_rapido: Ritmo acelerado que invita al movimiento
- tempo_medio: Ritmo moderado, cómodo para diversas actividades
- tempo_lento: Ritmo pausado, ideal para reflexión o relajación

**ESPECTRO EMOCIONAL:**
- euforia: Alegría intensa, celebración, éxtasis musical
- melancolia: Tristeza bella, nostalgia, reflexión emocional
- serenidad: Paz interior, calma, equilibrio emocional
- intensidad_dramatica: Emociones fuertes, drama, pasión
- misterio: Atmósferas enigmáticas, suspense, lo inexplorado
- calidez: Sensación de confort, hogar, abrazo emocional

**CONTEXTOS SITUACIONALES:**
- ejercicio_deporte: Perfecta para actividad física y entrenamiento
- trabajo_concentracion: Ideal para tareas que requieren focus mental
- social_fiesta: Música para compartir, bailar, celebrar en grupo
- introspección: Para momentos de reflexión personal y autoconocimiento
- relajacion_descanso: Para descomprimir y liberar tensiones
- viaje_movimiento: Banda sonora ideal para desplazamientos y aventuras

**DIMENSIONES CULTURALES:**
- nostalgia_retro: Evoca épocas pasadas, referencias vintage
- vanguardia_experimental: Sonidos innovadores, ruptura de convenciones
- authenticity_underground: Autenticidad cultural, alejado del mainstream
- universalidad: Apela ampliamente, trasciende barreras culturales
- regionalidad: Fuertemente conectado a una cultura o región específica
- atemporalidad: Trasciende épocas, suena relevante en cualquier momento

**EFECTOS PSICOLÓGICOS:**
- estimulacion_creativa: Cataliza pensamiento creativo y artistic thinking
- procesamiento_emocional: Ayuda a procesar y entender emociones complejas
- escape_mental: Proporciona desconexión de la realidad cotidiana
- motivacion_impulso: Genera determinación y fuerza de voluntad
- contemplacion_filosofica: Invita a reflexiones profundas sobre la existencia
- conexion_social: Facilita sentimientos de pertenencia y comunidad

INSTRUCCIONES:
1. Para cada canción, asigna un porcentaje (0-100) a cada una de las 29 dimensiones
2. Los porcentajes dentro de cada categoría NO necesitan sumar 100% - cada dimensión es independiente
3. Considera el género musical, letra (si la conoces), artista, contexto cultural y emocional
4. Sé preciso y fundamenta tus asignaciones en conocimiento musical real

FORMATO DE RESPUESTA (JSON estricto):
```json
{{
  "analisis_musical": [
    {{
      "track_name": "nombre_exacto_cancion",
      "artist_name": "nombre_exacto_artista", 
      "album_name": "nombre_exacto_album",
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
        """
        Llama a Claude en Bedrock para analizar las canciones.
        
        Args:
            prompt: Prompt preparado para el análisis
            
        Returns:
            Respuesta del modelo
        """
        try:
            # Configuración para Claude 3.5 Sonnet
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 8000,
                "temperature": 0.1,  # Baja temperatura para consistencia
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = self.bedrock_runtime.invoke_model(
                modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json"
            )
            
            response_body = json.loads(response['body'].read().decode('utf-8'))
            return response_body['content'][0]['text']
            
        except Exception as e:
            logger.error(f"Error llamando a Bedrock Claude: {str(e)}")
            raise
    
    def parse_llm_response(self, response_text: str) -> List[Dict]:
        """
        Parsea la respuesta del LLM y extrae los análisis de canciones.
        
        Args:
            response_text: Respuesta cruda del LLM
            
        Returns:
            Lista de análisis parseados
        """
        try:
            # Limpiar la respuesta para extraer solo el JSON
            response_text = response_text.strip()
            
            # Buscar el JSON en la respuesta
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                raise ValueError("No se encontró JSON válido en la respuesta")
            
            json_text = response_text[start_idx:end_idx]
            parsed_response = json.loads(json_text)
            
            if 'analisis_musical' not in parsed_response:
                raise ValueError("Respuesta no contiene la clave 'analisis_musical'")
            
            return parsed_response['analisis_musical']
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parseando JSON: {str(e)}")
            logger.error(f"Respuesta problemática: {response_text[:500]}...")
            raise
        except Exception as e:
            logger.error(f"Error procesando respuesta del LLM: {str(e)}")
            raise
    
    def get_songs_from_athena(self, limit: int = 100) -> pd.DataFrame:
        """
        Obtiene canciones únicas de Athena para analizar.
        
        Args:
            limit: Número máximo de canciones únicas a obtener
            
        Returns:
            DataFrame con las canciones para analizar
        """
        query = f"""
        SELECT DISTINCT 
            track_name,
            artist_name,
            album_name,
            track_id
        FROM spotify_analytics.user_tracks
        WHERE track_name IS NOT NULL 
            AND artist_name IS NOT NULL
            AND album_name IS NOT NULL
        LIMIT {limit}
        """
        
        try:
            # Ejecutar consulta en Athena
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': 'spotify_analytics'},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Esperar a que termine la consulta
            while True:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    raise Exception(f"Query falló con estado: {status}")
                
                time.sleep(2)
            
            # Obtener resultados
            results = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Convertir a DataFrame
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = []
            
            for row in results['ResultSet']['Rows'][1:]:  # Saltear header
                row_data = []
                for field in row['Data']:
                    row_data.append(field.get('VarCharValue', ''))
                rows.append(row_data)
            
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Obtenidas {len(df)} canciones únicas para analizar")
            
            return df
            
        except Exception as e:
            logger.error(f"Error obteniendo canciones de Athena: {str(e)}")
            raise
    
    def analyze_songs_batch(self, songs_batch: List[Dict]) -> List[Dict]:
        """
        Analiza un lote de canciones usando Bedrock.
        
        Args:
            songs_batch: Lista de canciones para analizar
            
        Returns:
            Lista de análisis completados
        """
        logger.info(f"Analizando lote de {len(songs_batch)} canciones...")
        
        # Crear prompt
        prompt = self.create_analysis_prompt(songs_batch)
        
        # Llamar a Bedrock con reintentos
        for attempt in range(self.max_retries):
            try:
                response = self.call_bedrock_claude(prompt)
                analyses = self.parse_llm_response(response)
                
                logger.info(f"Análisis completado exitosamente para {len(analyses)} canciones")
                return analyses
                
            except Exception as e:
                logger.warning(f"Intento {attempt + 1} falló: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay_between_requests * (attempt + 1))
                else:
                    logger.error(f"Falló análisis después de {self.max_retries} intentos")
                    raise
    
    def save_analyses_to_json(self, analyses: List[Dict], output_file: str):
        """
        Guarda los análisis en un archivo JSON.
        
        Args:
            analyses: Lista de análisis para guardar
            output_file: Ruta del archivo de salida
        """
        try:
            output_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_songs': len(analyses),
                    'dimensions_count': 29,
                    'schema_version': '1.0'
                },
                'analyses': analyses
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Análisis guardados en {output_file}")
            
        except Exception as e:
            logger.error(f"Error guardando análisis: {str(e)}")
            raise
    
    def process_songs(self, max_songs: int = 100, output_file: str = None) -> str:
        """
        Procesa canciones desde Athena y genera análisis completos.
        
        Args:
            max_songs: Número máximo de canciones a procesar
            output_file: Archivo donde guardar los resultados
            
        Returns:
            Ruta del archivo de salida
        """
        logger.info(f"Iniciando procesamiento de hasta {max_songs} canciones...")
        
        # Obtener canciones de Athena
        songs_df = self.get_songs_from_athena(limit=max_songs)
        
        if len(songs_df) == 0:
            raise ValueError("No se encontraron canciones para procesar")
        
        # Convertir a lista de diccionarios
        songs_list = songs_df.to_dict('records')
        
        # Procesar en lotes
        all_analyses = []
        total_batches = (len(songs_list) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(songs_list), self.batch_size):
            batch_num = (i // self.batch_size) + 1
            batch = songs_list[i:i + self.batch_size]
            
            logger.info(f"Procesando lote {batch_num}/{total_batches}")
            
            try:
                batch_analyses = self.analyze_songs_batch(batch)
                all_analyses.extend(batch_analyses)
                
                # Pausa entre lotes para evitar rate limiting
                if i + self.batch_size < len(songs_list):
                    time.sleep(self.delay_between_requests)
                    
            except Exception as e:
                logger.error(f"Error procesando lote {batch_num}: {str(e)}")
                # Continuar con el siguiente lote
                continue
        
        # Guardar resultados
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"music_analysis_{timestamp}.json"
        
        self.save_analyses_to_json(all_analyses, output_file)
        
        logger.info(f"Procesamiento completado. {len(all_analyses)} canciones analizadas.")
        logger.info(f"Resultados guardados en: {output_file}")
        
        return output_file


def main():
    """Función principal para ejecutar el análisis desde línea de comandos."""
    parser = argparse.ArgumentParser(
        description='Analiza canciones usando AWS Bedrock para generar dimensiones musicales avanzadas'
    )
    parser.add_argument(
        '--max-songs', 
        type=int, 
        default=50,
        help='Número máximo de canciones a procesar (default: 50)'
    )
    parser.add_argument(
        '--output-file',
        type=str,
        help='Archivo de salida para los resultados (default: generado automáticamente)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='Región de AWS (default: us-east-1)'
    )
    
    args = parser.parse_args()
    
    try:
        # Inicializar analizador
        logger.info("Inicializando MusicDimensionsAnalyzer...")
        analyzer = MusicDimensionsAnalyzer(region_name=args.region)
        
        # Procesar canciones
        output_file = analyzer.process_songs(
            max_songs=args.max_songs,
            output_file=args.output_file
        )
        
        print(f"\n✅ Análisis completado exitosamente!")
        print(f"📄 Resultados guardados en: {output_file}")
        print(f"🎵 Canciones procesadas: ver el log para detalles")
        print(f"\nPróximos pasos:")
        print(f"1. Revisar el archivo {output_file}")
        print(f"2. Ejecutar el script de carga a Glue")
        print(f"3. Usar create_enhanced_glue_table.py para crear la tabla en Glue")
        
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error durante el procesamiento: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()