#!/usr/bin/env python3
"""
Script para procesar el catálogo de artistas de Spotify y subirlo a S3 como Parquet particionado.

Este script:
1. Lee el archivo JSON del catálogo de artistas
2. Procesa y limpia los datos
3. Agrega columnas de partición óptimas usando categorización avanzada de géneros
4. Convierte a formato Parquet con compresión Snappy
5. Sube los archivos particionados a S3

CARACTERÍSTICAS AVANZADAS:
- Categorización inteligente de géneros que analiza TODOS los géneros del artista
- Sistema de puntuación para determinar la categoría principal más precisa
- Manejo específico de géneros latinos, subgéneros de rock, electronic, etc.
- Particionado optimizado para queries eficientes en Athena

Uso:
    python etl_artists_catalog.py [--input-file INPUT] [--bucket BUCKET] [--dry-run]

Ejemplo:
    python etl_artists_catalog.py --input-file ./data/catalogo_artistas.json --bucket itam-analytics-ragp
"""

import os
import sys
import pandas as pd
import boto3
import argparse
import logging
from datetime import datetime
import json
import tempfile
import shutil
from pathlib import Path
from botocore.exceptions import ClientError

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('artists_catalog_processor.log')
    ]
)
logger = logging.getLogger(__name__)

class ArtistsCatalogProcessor:
    """
    Procesador del catálogo de artistas para convertir a Parquet particionado y subir a S3.
    """
    
    def __init__(self, bucket_name='itam-analytics-ragp', region='us-east-1'):
        """
        Inicializa el procesador.
        
        Args:
            bucket_name: Nombre del bucket de S3
            region: Región de AWS
        """
        self.bucket_name = bucket_name
        self.region = region
        self.s3_prefix = 'spotifire/processed/artists_catalog'
        
        # Inicializar cliente S3
        try:
            self.s3_client = boto3.client('s3', region_name=region)
            logger.info(f"Cliente S3 inicializado para bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Error inicializando cliente S3: {e}")
            raise
    
    def load_artists_data(self, json_file_path):
        """
        Carga los datos de artistas desde el archivo JSON.
        
        Args:
            json_file_path: Ruta al archivo JSON del catálogo
            
        Returns:
            DataFrame con los datos de artistas
        """
        logger.info(f"Cargando datos desde: {json_file_path}")
        
        try:
            # Cargar JSON
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convertir a DataFrame
            df = pd.DataFrame(data)
            
            logger.info(f"Datos cargados: {len(df)} artistas")
            logger.info(f"Columnas disponibles: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error cargando datos: {e}")
            raise
    
    def process_artists_data(self, df):
        """
        Procesa y limpia los datos de artistas, agregando columnas de partición.
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame procesado con columnas de partición
        """
        logger.info("Procesando datos de artistas...")
        
        # Crear copia para no modificar el original
        processed_df = df.copy()
        
        # Limpieza básica
        processed_df = processed_df.dropna(subset=['id', 'name'])
        processed_df['name'] = processed_df['name'].str.strip()
        
        # Manejo de valores nulos
        processed_df['followers'] = processed_df['followers'].fillna(0).astype(int)
        processed_df['popularity'] = processed_df['popularity'].fillna(0).astype(int)
        processed_df['genres'] = processed_df['genres'].apply(
            lambda x: x if isinstance(x, list) and x else ['unknown']
        )
        
        # Agregar columnas de partición
        
        # 1. Rango de popularidad (partición principal)
        def get_popularity_range(popularity):
            if popularity >= 80:
                return 'very_high'
            elif popularity >= 60:
                return 'high'
            elif popularity >= 40:
                return 'medium'
            elif popularity >= 20:
                return 'low'
            else:
                return 'very_low'
        
        processed_df['popularity_range'] = processed_df['popularity'].apply(get_popularity_range)
        
        # 2. Género principal (segunda partición) - MÉTODO AVANZADO
        def get_primary_genre(genres_list):
            """
            Categorización avanzada que analiza TODOS los géneros del artista
            usando un sistema de puntuación por categoría.
            """
            if not genres_list or len(genres_list) == 0:
                return 'unknown'
            
            # Convertir todos los géneros a minúsculas para análisis
            genres_lower = [g.lower() for g in genres_list]
            
            # Sistema de scoring por categoría
            category_scores = {
                'latin': 0, 'pop': 0, 'rock': 0, 'electronic': 0, 'hip_hop': 0,
                'jazz_blues': 0, 'country_folk': 0, 'classical': 0, 'reggae': 0, 'world': 0
            }
            
            # Reglas de scoring con pesos (patrones específicos tienen mayor peso)
            scoring_rules = [
                # Latin (peso alto para términos específicos)
                {'patterns': ['reggaeton', 'bachata', 'salsa', 'merengue', 'cumbia', 'tango'], 'category': 'latin', 'weight': 10},
                {'patterns': ['latin', 'latino', 'spanish', 'mexican', 'colombian', 'argentinian'], 'category': 'latin', 'weight': 8},
                {'patterns': ['bossa nova', 'flamenco', 'fado'], 'category': 'latin', 'weight': 9},
                
                # Pop (después de subgéneros específicos)
                {'patterns': ['pop'], 'category': 'pop', 'weight': 7},
                {'patterns': ['k-pop', 'j-pop'], 'category': 'pop', 'weight': 9},
                {'patterns': ['mainstream', 'chart', 'commercial'], 'category': 'pop', 'weight': 5},
                
                # Rock & Metal (con subgéneros específicos)
                {'patterns': ['heavy metal', 'death metal', 'black metal', 'power metal'], 'category': 'rock', 'weight': 10},
                {'patterns': ['indie rock', 'alternative rock', 'punk rock', 'prog rock'], 'category': 'rock', 'weight': 9},
                {'patterns': ['rock', 'metal'], 'category': 'rock', 'weight': 7},
                {'patterns': ['alternative', 'indie', 'punk', 'grunge'], 'category': 'rock', 'weight': 6},
                
                # Electronic
                {'patterns': ['house', 'techno', 'trance', 'dubstep', 'drum and bass'], 'category': 'electronic', 'weight': 10},
                {'patterns': ['electronic', 'edm', 'electronica'], 'category': 'electronic', 'weight': 8},
                {'patterns': ['dance', 'club', 'synthesizer', 'ambient'], 'category': 'electronic', 'weight': 5},
                
                # Hip Hop
                {'patterns': ['hip hop', 'rap', 'trap', 'drill', 'grime'], 'category': 'hip_hop', 'weight': 10},
                {'patterns': ['urban', 'street', 'conscious rap'], 'category': 'hip_hop', 'weight': 6},
                
                # Jazz & Blues
                {'patterns': ['jazz', 'blues', 'soul', 'funk'], 'category': 'jazz_blues', 'weight': 8},
                {'patterns': ['neo soul', 'jazz fusion', 'smooth jazz', 'bebop'], 'category': 'jazz_blues', 'weight': 9},
                {'patterns': ['swing', 'dixieland', 'gospel'], 'category': 'jazz_blues', 'weight': 7},
                
                # Country & Folk
                {'patterns': ['country', 'folk', 'americana', 'bluegrass'], 'category': 'country_folk', 'weight': 8},
                {'patterns': ['country pop', 'folk rock', 'alt-country'], 'category': 'country_folk', 'weight': 7},
                {'patterns': ['acoustic', 'roots', 'singer-songwriter'], 'category': 'country_folk', 'weight': 4},
                
                # World Music
                {'patterns': ['world', 'traditional', 'ethnic', 'international'], 'category': 'world', 'weight': 8},
                {'patterns': ['african', 'indian', 'middle eastern', 'celtic'], 'category': 'world', 'weight': 9},
                
                # Reggae
                {'patterns': ['reggae', 'ska', 'dancehall'], 'category': 'reggae', 'weight': 10},
                {'patterns': ['jamaican', 'dub', 'roots reggae'], 'category': 'reggae', 'weight': 8},
                
                # Classical
                {'patterns': ['classical', 'orchestral', 'opera', 'symphony', 'baroque'], 'category': 'classical', 'weight': 10},
                {'patterns': ['instrumental', 'chamber music', 'contemporary classical'], 'category': 'classical', 'weight': 8}
            ]
            
            # Calcular scores para cada categoría analizando TODOS los géneros
            for genre in genres_lower:
                for rule in scoring_rules:
                    for pattern in rule['patterns']:
                        if pattern in genre:
                            category_scores[rule['category']] += rule['weight']
            
            # Encontrar la categoría con mayor score
            max_score = 0
            best_category = 'other'
            
            for category, score in category_scores.items():
                if score > max_score:
                    max_score = score
                    best_category = category
            
            return best_category if max_score > 0 else 'other'
        
        processed_df['primary_genre'] = processed_df['genres'].apply(get_primary_genre)
        
        # 3. Rango de seguidores (tercera partición para casos específicos)
        def get_followers_tier(followers):
            if followers >= 10000000:  # 10M+
                return 'mega'
            elif followers >= 1000000:  # 1M+
                return 'major'
            elif followers >= 100000:   # 100K+
                return 'established'
            elif followers >= 10000:    # 10K+
                return 'emerging'
            else:
                return 'niche'
        
        processed_df['followers_tier'] = processed_df['followers'].apply(get_followers_tier)
        
        # Agregar metadatos de procesamiento
        processed_df['processed_at'] = datetime.now().isoformat()
        processed_df['data_source'] = 'spotify_api'
        
        # Reordenar columnas para optimizar el almacenamiento
        column_order = [
            'id', 'name', 'popularity', 'followers', 'genres',
            'popularity_range', 'primary_genre', 'followers_tier',
            'processed_at', 'data_source'
        ]
        
        processed_df = processed_df[column_order]
        
        logger.info(f"Datos procesados: {len(processed_df)} artistas")
        logger.info(f"Distribución por popularidad: {processed_df['popularity_range'].value_counts().to_dict()}")
        logger.info(f"Distribución por género (método avanzado): {processed_df['primary_genre'].value_counts().to_dict()}")
        
        # Log adicional sobre la efectividad de la categorización
        otros_count = (processed_df['primary_genre'] == 'other').sum()
        total_count = len(processed_df)
        categorization_rate = ((total_count - otros_count) / total_count * 100) if total_count > 0 else 0
        logger.info(f"Tasa de categorización exitosa: {categorization_rate:.1f}% ({total_count - otros_count}/{total_count})")
        
        return processed_df
    
    def save_partitioned_parquet(self, df, temp_dir):
        """
        Guarda el DataFrame como archivos Parquet particionados.
        
        Args:
            df: DataFrame a guardar
            temp_dir: Directorio temporal para los archivos
            
        Returns:
            Ruta del directorio con los archivos particionados
        """
        logger.info("Guardando como Parquet particionado...")
        
        output_path = os.path.join(temp_dir, 'partitioned_data')
        
        try:
            # Guardar particionado por popularidad_range y primary_genre
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                partition_cols=['popularity_range', 'primary_genre'],
                index=False
            )
            
            logger.info(f"Archivos Parquet guardados en: {output_path}")
            
            # Mostrar estructura de particiones creadas
            self._log_partition_structure(output_path)
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error guardando Parquet: {e}")
            raise
    
    def _log_partition_structure(self, parquet_path):
        """Registra la estructura de particiones creada."""
        try:
            for root, dirs, files in os.walk(parquet_path):
                level = root.replace(parquet_path, '').count(os.sep)
                indent = ' ' * 2 * level
                logger.info(f"{indent}{os.path.basename(root)}/")
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        file_size = os.path.getsize(file_path)
                        logger.info(f"{subindent}{file} ({file_size:,} bytes)")
        except Exception as e:
            logger.warning(f"Error mostrando estructura: {e}")
    
    def upload_to_s3(self, local_path, dry_run=False):
        """
        Sube los archivos particionados a S3.
        
        Args:
            local_path: Ruta local de los archivos
            dry_run: Si True, solo simula la subida
            
        Returns:
            Lista de objetos subidos exitosamente
        """
        logger.info(f"Subiendo archivos a S3: s3://{self.bucket_name}/{self.s3_prefix}")
        
        uploaded_files = []
        
        try:
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.endswith('.parquet'):
                        local_file_path = os.path.join(root, file)
                        
                        # Construir la ruta S3 manteniendo la estructura de particiones
                        relative_path = os.path.relpath(local_file_path, local_path)
                        s3_key = f"{self.s3_prefix}/{relative_path}".replace('\\', '/')
                        
                        if dry_run:
                            logger.info(f"[DRY RUN] Subiría: {local_file_path} -> s3://{self.bucket_name}/{s3_key}")
                        else:
                            try:
                                self.s3_client.upload_file(
                                    local_file_path, 
                                    self.bucket_name, 
                                    s3_key
                                )
                                logger.info(f"Subido: s3://{self.bucket_name}/{s3_key}")
                                uploaded_files.append(s3_key)
                            except ClientError as e:
                                logger.error(f"Error subiendo {local_file_path}: {e}")
            
            if not dry_run:
                logger.info(f"Subida completada: {len(uploaded_files)} archivos")
            
            return uploaded_files
            
        except Exception as e:
            logger.error(f"Error durante la subida: {e}")
            raise
    
    def create_glue_table_definition(self):
        """
        Genera la definición de tabla para AWS Glue Data Catalog.
        
        Returns:
            Diccionario con la definición de la tabla
        """
        table_definition = {
            'Name': 'artists_catalog',
            'Description': 'Catálogo de artistas de Spotify con información de popularidad y géneros',
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'EXTERNAL': 'TRUE',
                'parquet.compression': 'SNAPPY',
                'classification': 'parquet',
                'created_by': 'artists_catalog_processor',
                'created_at': datetime.now().isoformat(),
                'data_source': 'spotify_api'
            },
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'id', 'Type': 'string', 'Comment': 'Unique Spotify artist ID'},
                    {'Name': 'name', 'Type': 'string', 'Comment': 'Artist name'},
                    {'Name': 'popularity', 'Type': 'int', 'Comment': 'Spotify popularity score (0-100)'},
                    {'Name': 'followers', 'Type': 'bigint', 'Comment': 'Number of followers'},
                    {'Name': 'genres', 'Type': 'array<string>', 'Comment': 'List of genres'},
                    {'Name': 'followers_tier', 'Type': 'string', 'Comment': 'Follower count tier'},
                    {'Name': 'processed_at', 'Type': 'timestamp', 'Comment': 'Processing timestamp'},
                    {'Name': 'data_source', 'Type': 'string', 'Comment': 'Source of the data'}
                ],
                'Location': f's3://{self.bucket_name}/{self.s3_prefix}/',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {'serialization.format': '1'}
                },
                'Parameters': {
                    'classification': 'parquet',
                    'compressionType': 'snappy'
                }
            },
            'PartitionKeys': [
                {'Name': 'popularity_range', 'Type': 'string', 'Comment': 'Popularity tier (very_low, low, medium, high, very_high)'},
                {'Name': 'primary_genre', 'Type': 'string', 'Comment': 'Primary music genre category'}
            ]
        }
        
        return table_definition
    
    def process_and_upload(self, json_file_path, dry_run=False):
        """
        Ejecuta el proceso completo de carga, procesamiento y subida.
        
        Args:
            json_file_path: Ruta al archivo JSON de entrada
            dry_run: Si True, no sube realmente a S3
            
        Returns:
            Diccionario con el resumen del proceso
        """
        start_time = datetime.now()
        
        try:
            # 1. Cargar datos
            df = self.load_artists_data(json_file_path)
            original_count = len(df)
            
            # 2. Procesar datos
            processed_df = self.process_artists_data(df)
            processed_count = len(processed_df)
            
            # 3. Crear directorio temporal
            with tempfile.TemporaryDirectory() as temp_dir:
                logger.info(f"Usando directorio temporal: {temp_dir}")
                
                # 4. Guardar como Parquet particionado
                parquet_path = self.save_partitioned_parquet(processed_df, temp_dir)
                
                # 5. Subir a S3
                uploaded_files = self.upload_to_s3(parquet_path, dry_run)
            
            # 6. Preparar resumen
            end_time = datetime.now()
            duration = end_time - start_time
            
            summary = {
                'success': True,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'original_records': original_count,
                'processed_records': processed_count,
                'files_uploaded': len(uploaded_files),
                's3_location': f's3://{self.bucket_name}/{self.s3_prefix}/',
                'glue_table_definition': self.create_glue_table_definition()
            }
            
            logger.info("="*60)
            logger.info("PROCESO COMPLETADO EXITOSAMENTE")
            logger.info("="*60)
            logger.info(f"Registros originales: {original_count:,}")
            logger.info(f"Registros procesados: {processed_count:,}")
            logger.info(f"Archivos subidos: {len(uploaded_files)}")
            logger.info(f"Ubicación S3: s3://{self.bucket_name}/{self.s3_prefix}/")
            logger.info(f"Duración: {duration.total_seconds():.2f} segundos")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error en el proceso: {e}")
            return {
                'success': False,
                'error': str(e),
                'end_time': datetime.now().isoformat()
            }

def main():
    """Función principal con manejo de argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description='Procesar catálogo de artistas y subir a S3 como Parquet particionado',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Procesar archivo local y subir a S3:
  python %(prog)s --input-file ./data/catalogo_artistas.json

  # Usar bucket personalizado:
  python %(prog)s --input-file ./data/catalogo_artistas.json --bucket mi-bucket-s3

  # Modo de prueba (no subir a S3):
  python %(prog)s --input-file ./data/catalogo_artistas.json --dry-run

  # Especificar región de AWS:
  python %(prog)s --input-file ./data/catalogo_artistas.json --region us-west-2
        """
    )
    
    parser.add_argument(
        '--input-file',
        required=True,
        help='Ruta al archivo JSON del catálogo de artistas'
    )
    parser.add_argument(
        '--bucket',
        default='itam-analytics-ragp',
        help='Nombre del bucket S3 (default: itam-analytics-ragp)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='Región de AWS (default: us-east-1)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Procesar datos pero no subir a S3'
    )
    
    args = parser.parse_args()
    
    # Verificar que el archivo de entrada existe
    if not os.path.exists(args.input_file):
        logger.error(f"El archivo de entrada no existe: {args.input_file}")
        sys.exit(1)
    
    logger.info("Iniciando procesamiento del catálogo de artistas")
    logger.info(f"Archivo de entrada: {args.input_file}")
    logger.info(f"Bucket S3: {args.bucket}")
    logger.info(f"Región: {args.region}")
    if args.dry_run:
        logger.info("MODO DRY-RUN: No se subirán archivos a S3")
    
    try:
        # Inicializar procesador
        processor = ArtistsCatalogProcessor(
            bucket_name=args.bucket,
            region=args.region
        )
        
        # Ejecutar proceso
        result = processor.process_and_upload(
            json_file_path=args.input_file,
            dry_run=args.dry_run
        )
        
        if result['success']:
            logger.info("✅ Proceso completado exitosamente")
            
            # Mostrar información de la tabla de Glue si no es dry-run
            if not args.dry_run:
                logger.info("\n" + "="*60)
                logger.info("CONFIGURACIÓN PARA GLUE DATA CATALOG")
                logger.info("="*60)
                logger.info("Para crear la tabla en Glue, ejecuta:")
                logger.info(f"aws glue create-table --database-input Name=spotify_analytics \\")
                logger.info(f"  --table-input file://glue_table_definition.json")
                logger.info("\nDonde glue_table_definition.json contiene la definición generada.")
                
                # Opcionalmente, guardar la definición en un archivo
                glue_def_file = "artists_catalog_glue_table.json"
                with open(glue_def_file, 'w') as f:
                    json.dump(result['glue_table_definition'], f, indent=2)
                logger.info(f"Definición de tabla guardada en: {glue_def_file}")
            
            sys.exit(0)
        else:
            logger.error("❌ Proceso falló")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("🛑 Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()