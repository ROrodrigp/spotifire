#!/usr/bin/env python3
"""
Creador de tabla AWS Glue para almacenar análisis musical enriquecido con dimensiones psicológicas.
Este script crea la estructura de tabla que contendrá los análisis generados por Bedrock.
"""

import boto3
import logging
import argparse
import sys
from datetime import datetime
from botocore.exceptions import ClientError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('enhanced_glue_table_creation.log')
    ]
)
logger = logging.getLogger(__name__)

class EnhancedGlueTableCreator:
    """
    Creador de tabla Glue para almacenar análisis musical enriquecido.
    
    Esta clase maneja la creación de una tabla optimizada para almacenar
    tanto los metadatos básicos de las canciones como las 29 dimensiones
    de análisis psicológico y emocional generadas por Bedrock.
    """
    
    def __init__(self, region_name='us-east-1'):
        """
        Inicializa el creador de tabla con los clientes AWS necesarios.
        """
        try:
            self.glue_client = boto3.client('glue', region_name=region_name)
            self.region_name = region_name
            logger.info(f"EnhancedGlueTableCreator inicializado en región {region_name}")
        except Exception as e:
            logger.error(f"Error inicializando el creador de tabla: {str(e)}")
            raise
    
    def get_enhanced_table_schema(self):
        """
        Define el esquema completo para la tabla de análisis musical enriquecido.
        
        Esta tabla incluye:
        - Metadatos básicos de la canción (track_id, names, etc.)
        - 29 dimensiones de análisis como columnas numéricas (double)
        - Metadatos del análisis (cuándo se analizó, versión del modelo, etc.)
        
        Returns:
            Lista de definiciones de columnas para la tabla
        """
        schema = [
            # === IDENTIFICADORES Y METADATOS BÁSICOS ===
            {
                'Name': 'track_id',
                'Type': 'string',
                'Comment': 'Spotify unique identifier for the track'
            },
            {
                'Name': 'track_name',
                'Type': 'string', 
                'Comment': 'Name of the music track'
            },
            {
                'Name': 'artist_name',
                'Type': 'string',
                'Comment': 'Name of the artist'
            },
            {
                'Name': 'album_name',
                'Type': 'string',
                'Comment': 'Name of the album'
            },
            
            # === DIMENSIONES ENERGÉTICAS Y TEMPORALES ===
            {
                'Name': 'energia_alta',
                'Type': 'double',
                'Comment': 'Porcentaje: Música que genera activación física y mental intensa (0-100)'
            },
            {
                'Name': 'energia_media',
                'Type': 'double',
                'Comment': 'Porcentaje: Música con energía moderada, ni muy intensa ni muy relajada (0-100)'
            },
            {
                'Name': 'energia_baja',
                'Type': 'double',
                'Comment': 'Porcentaje: Música calmada, relajante o contemplativa (0-100)'
            },
            {
                'Name': 'tempo_rapido',
                'Type': 'double',
                'Comment': 'Porcentaje: Ritmo acelerado que invita al movimiento (0-100)'
            },
            {
                'Name': 'tempo_medio',
                'Type': 'double',
                'Comment': 'Porcentaje: Ritmo moderado, cómodo para diversas actividades (0-100)'
            },
            {
                'Name': 'tempo_lento',
                'Type': 'double',
                'Comment': 'Porcentaje: Ritmo pausado, ideal para reflexión o relajación (0-100)'
            },
            
            # === DIMENSIONES EMOCIONALES PROFUNDAS ===
            {
                'Name': 'euforia',
                'Type': 'double',
                'Comment': 'Porcentaje: Alegría intensa, celebración, éxtasis musical (0-100)'
            },
            {
                'Name': 'melancolia',
                'Type': 'double',
                'Comment': 'Porcentaje: Tristeza bella, nostalgia, reflexión emocional (0-100)'
            },
            {
                'Name': 'serenidad',
                'Type': 'double',
                'Comment': 'Porcentaje: Paz interior, calma, equilibrio emocional (0-100)'
            },
            {
                'Name': 'intensidad_dramatica',
                'Type': 'double',
                'Comment': 'Porcentaje: Emociones fuertes, drama, pasión (0-100)'
            },
            {
                'Name': 'misterio',
                'Type': 'double',
                'Comment': 'Porcentaje: Atmósferas enigmáticas, suspense, lo inexplorado (0-100)'
            },
            {
                'Name': 'calidez',
                'Type': 'double',
                'Comment': 'Porcentaje: Sensación de confort, hogar, abrazo emocional (0-100)'
            },
            
            # === CONTEXTOS SITUACIONALES ===
            {
                'Name': 'ejercicio_deporte',
                'Type': 'double',
                'Comment': 'Porcentaje: Perfecta para actividad física y entrenamiento (0-100)'
            },
            {
                'Name': 'trabajo_concentracion',
                'Type': 'double',
                'Comment': 'Porcentaje: Ideal para tareas que requieren focus mental (0-100)'
            },
            {
                'Name': 'social_fiesta',
                'Type': 'double',
                'Comment': 'Porcentaje: Música para compartir, bailar, celebrar en grupo (0-100)'
            },
            {
                'Name': 'introspección',
                'Type': 'double',
                'Comment': 'Porcentaje: Para momentos de reflexión personal y autoconocimiento (0-100)'
            },
            {
                'Name': 'relajacion_descanso',
                'Type': 'double',
                'Comment': 'Porcentaje: Para descomprimir y liberar tensiones (0-100)'
            },
            {
                'Name': 'viaje_movimiento',
                'Type': 'double',
                'Comment': 'Porcentaje: Banda sonora ideal para desplazamientos y aventuras (0-100)'
            },
            
            # === DIMENSIONES CULTURALES ===
            {
                'Name': 'nostalgia_retro',
                'Type': 'double',
                'Comment': 'Porcentaje: Evoca épocas pasadas, referencias vintage (0-100)'
            },
            {
                'Name': 'vanguardia_experimental',
                'Type': 'double',
                'Comment': 'Porcentaje: Sonidos innovadores, ruptura de convenciones (0-100)'
            },
            {
                'Name': 'authenticity_underground',
                'Type': 'double',
                'Comment': 'Porcentaje: Autenticidad cultural, alejado del mainstream (0-100)'
            },
            {
                'Name': 'universalidad',
                'Type': 'double',
                'Comment': 'Porcentaje: Apela ampliamente, trasciende barreras culturales (0-100)'
            },
            {
                'Name': 'regionalidad',
                'Type': 'double',
                'Comment': 'Porcentaje: Fuertemente conectado a una cultura o región específica (0-100)'
            },
            {
                'Name': 'atemporalidad',
                'Type': 'double',
                'Comment': 'Porcentaje: Trasciende épocas, suena relevante en cualquier momento (0-100)'
            },
            
            # === EFECTOS PSICOLÓGICOS ===
            {
                'Name': 'estimulacion_creativa',
                'Type': 'double',
                'Comment': 'Porcentaje: Cataliza pensamiento creativo y artistic thinking (0-100)'
            },
            {
                'Name': 'procesamiento_emocional',
                'Type': 'double',
                'Comment': 'Porcentaje: Ayuda a procesar y entender emociones complejas (0-100)'
            },
            {
                'Name': 'escape_mental',
                'Type': 'double',
                'Comment': 'Porcentaje: Proporciona desconexión de la realidad cotidiana (0-100)'
            },
            {
                'Name': 'motivacion_impulso',
                'Type': 'double',
                'Comment': 'Porcentaje: Genera determinación y fuerza de voluntad (0-100)'
            },
            {
                'Name': 'contemplacion_filosofica',
                'Type': 'double',
                'Comment': 'Porcentaje: Invita a reflexiones profundas sobre la existencia (0-100)'
            },
            {
                'Name': 'conexion_social',
                'Type': 'double',
                'Comment': 'Porcentaje: Facilita sentimientos de pertenencia y comunidad (0-100)'
            },
            
            # === METADATOS DEL ANÁLISIS ===
            {
                'Name': 'analysis_timestamp',
                'Type': 'timestamp',
                'Comment': 'Cuándo se realizó el análisis de dimensiones musicales'
            },
            {
                'Name': 'analysis_model',
                'Type': 'string',
                'Comment': 'Modelo de IA utilizado para el análisis (ej: claude-3-5-sonnet)'
            },
            {
                'Name': 'analysis_version',
                'Type': 'string',
                'Comment': 'Versión del esquema de análisis utilizado'
            },
            {
                'Name': 'confidence_score',
                'Type': 'double',
                'Comment': 'Puntuación de confianza del análisis (0-100)'
            }
        ]
        
        return schema
    
    def create_enhanced_database(self, database_name):
        """
        Crea la base de datos para análisis musical enriquecido si no existe.
        
        Args:
            database_name: Nombre de la base de datos a crear
            
        Returns:
            bool: True si se creó o ya existía, False si hubo error
        """
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': database_name,
                    'Description': f'Base de datos para análisis musical enriquecido con dimensiones psicológicas - Creada {datetime.now().isoformat()}',
                    'Parameters': {
                        'created_by': 'enhanced_music_analysis_pipeline',
                        'created_at': datetime.now().isoformat(),
                        'purpose': 'advanced_music_analytics',
                        'data_format': 'parquet',
                        'analysis_dimensions': '29',
                        'schema_version': '1.0'
                    }
                }
            )
            logger.info(f"Base de datos {database_name} creada exitosamente")
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info(f"Base de datos {database_name} ya existe")
            return True
            
        except Exception as e:
            logger.error(f"Error creando base de datos: {str(e)}")
            return False
    
    def create_enhanced_table(self, database_name, table_name, s3_location):
        """
        Crea la tabla para almacenar análisis musical enriquecido.
        
        Args:
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla a crear
            s3_location: Ubicación S3 donde se almacenarán los datos
            
        Returns:
            bool: True si se creó exitosamente, False en caso contrario
        """
        logger.info(f"Creando tabla enriquecida: {database_name}.{table_name}")
        logger.info(f"Ubicación S3: {s3_location}")
        
        # Obtener esquema de la tabla
        table_schema = self.get_enhanced_table_schema()
        
        try:
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': 'Tabla de análisis musical enriquecido con 29 dimensiones psicológicas y emocionales',
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY',
                        'classification': 'parquet',
                        'created_by': 'enhanced_music_analysis_pipeline',
                        'created_at': datetime.now().isoformat(),
                        'data_source': 'spotify_api_plus_bedrock_analysis',
                        'analysis_dimensions': '29',
                        'schema_version': '1.0',
                        'model_type': 'claude-3-5-sonnet',
                        'update_frequency': 'on_demand'
                    },
                    'StorageDescriptor': {
                        'Columns': table_schema,
                        'Location': s3_location,
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {
                                'serialization.format': '1'
                            }
                        },
                        'Parameters': {
                            'classification': 'parquet',
                            'compressionType': 'snappy',
                            'typeOfData': 'file'
                        }
                    }
                }
            )
            
            logger.info(f"Tabla {database_name}.{table_name} creada exitosamente")
            logger.info(f"Esquema incluye {len(table_schema)} columnas:")
            logger.info(f"  - 4 columnas de metadatos básicos")
            logger.info(f"  - 29 columnas de dimensiones de análisis")
            logger.info(f"  - 4 columnas de metadatos del análisis")
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Tabla {database_name}.{table_name} ya existe")
            return self.update_existing_enhanced_table(database_name, table_name, s3_location)
            
        except Exception as e:
            logger.error(f"Error creando tabla: {str(e)}")
            return False
    
    def update_existing_enhanced_table(self, database_name, table_name, s3_location):
        """
        Actualiza una tabla existente con el esquema más reciente.
        
        Args:
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla a actualizar
            s3_location: Ubicación S3 de los datos
            
        Returns:
            bool: True si se actualizó exitosamente
        """
        logger.info(f"Actualizando tabla existente: {database_name}.{table_name}")
        
        table_schema = self.get_enhanced_table_schema()
        
        try:
            self.glue_client.update_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': 'Tabla de análisis musical enriquecido con 29 dimensiones psicológicas y emocionales (ACTUALIZADA)',
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY',
                        'classification': 'parquet',
                        'updated_by': 'enhanced_music_analysis_pipeline',
                        'updated_at': datetime.now().isoformat(),
                        'data_source': 'spotify_api_plus_bedrock_analysis',
                        'analysis_dimensions': '29',
                        'schema_version': '1.0',
                        'model_type': 'claude-3-5-sonnet',
                        'update_frequency': 'on_demand'
                    },
                    'StorageDescriptor': {
                        'Columns': table_schema,
                        'Location': s3_location,
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {
                                'serialization.format': '1'
                            }
                        },
                        'Parameters': {
                            'classification': 'parquet',
                            'compressionType': 'snappy',
                            'typeOfData': 'file'
                        }
                    }
                }
            )
            
            logger.info(f"Tabla {database_name}.{table_name} actualizada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error actualizando tabla: {str(e)}")
            return False
    
    def verify_enhanced_table(self, database_name, table_name):
        """
        Verifica que la tabla se haya creado correctamente y muestra información.
        
        Args:
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla a verificar
            
        Returns:
            bool: True si la verificación es exitosa
        """
        logger.info("Verificando creación de tabla enriquecida...")
        
        try:
            # Verificar base de datos
            db_response = self.glue_client.get_database(Name=database_name)
            logger.info(f"✅ Base de datos verificada: {db_response['Database']['Name']}")
            
            # Verificar tabla
            table_response = self.glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )
            
            table_info = table_response['Table']
            columns = table_info['StorageDescriptor']['Columns']
            
            logger.info(f"✅ Tabla verificada: {table_info['Name']}")
            logger.info(f"📍 Ubicación: {table_info['StorageDescriptor']['Location']}")
            logger.info(f"📊 Total de columnas: {len(columns)}")
            
            # Verificar dimensiones específicas
            dimension_columns = [col for col in columns if col['Name'] not in 
                               ['track_id', 'track_name', 'artist_name', 'album_name', 
                                'analysis_timestamp', 'analysis_model', 'analysis_version', 'confidence_score']]
            
            logger.info(f"🎵 Dimensiones de análisis: {len(dimension_columns)}")
            
            # Mostrar algunas dimensiones de ejemplo
            example_dimensions = dimension_columns[:5]
            logger.info("📋 Ejemplos de dimensiones:")
            for dim in example_dimensions:
                logger.info(f"   • {dim['Name']}: {dim['Comment']}")
            
            if len(dimension_columns) != 29:
                logger.warning(f"⚠️  Se esperaban 29 dimensiones, encontradas {len(dimension_columns)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en verificación: {str(e)}")
            return False
    
    def get_sample_queries(self, database_name, table_name):
        """
        Genera consultas de ejemplo para usar con la nueva tabla.
        
        Args:
            database_name: Nombre de la base de datos
            table_name: Nombre de la tabla
            
        Returns:
            Lista de consultas SQL de ejemplo
        """
        queries = [
            f"""
-- 🎵 Canciones más energéticas (alta energía + tempo rápido)
SELECT 
    track_name,
    artist_name,
    energia_alta,
    tempo_rapido,
    (energia_alta + tempo_rapido) / 2 as energia_total
FROM {database_name}.{table_name}
ORDER BY energia_total DESC
LIMIT 10;
""",
            f"""
-- 😌 Canciones perfectas para relajación
SELECT 
    track_name,
    artist_name,
    energia_baja,
    serenidad,
    relajacion_descanso,
    (energia_baja + serenidad + relajacion_descanso) / 3 as relax_score
FROM {database_name}.{table_name}
ORDER BY relax_score DESC
LIMIT 10;
""",
            f"""
-- 💪 Música ideal para ejercicio
SELECT 
    track_name,
    artist_name,
    ejercicio_deporte,
    motivacion_impulso,
    energia_alta,
    (ejercicio_deporte + motivacion_impulso + energia_alta) / 3 as workout_score
FROM {database_name}.{table_name}
ORDER BY workout_score DESC
LIMIT 10;
""",
            f"""
-- 🎭 Canciones con mayor intensidad emocional
SELECT 
    track_name,
    artist_name,
    intensidad_dramatica,
    procesamiento_emocional,
    melancolia,
    euforia
FROM {database_name}.{table_name}
ORDER BY intensidad_dramatica DESC
LIMIT 10;
""",
            f"""
-- 🔍 Distribución de géneros por dimensión cultural
SELECT 
    CASE 
        WHEN nostalgia_retro > 70 THEN 'Nostálgico/Retro'
        WHEN vanguardia_experimental > 70 THEN 'Experimental'
        WHEN authenticity_underground > 70 THEN 'Underground'
        WHEN universalidad > 70 THEN 'Universal'
        ELSE 'Mixto'
    END as categoria_cultural,
    COUNT(*) as cantidad,
    AVG(universalidad) as promedio_universalidad
FROM {database_name}.{table_name}
GROUP BY 1
ORDER BY cantidad DESC;
"""
        ]
        
        return queries


def main():
    """Función principal para crear la tabla desde línea de comandos."""
    parser = argparse.ArgumentParser(
        description='Crea tabla AWS Glue para análisis musical enriquecido con dimensiones psicológicas'
    )
    parser.add_argument(
        '--database-name',
        default='spotify_analytics_enhanced',
        help='Nombre de la base de datos Glue (default: spotify_analytics_enhanced)'
    )
    parser.add_argument(
        '--table-name',
        default='tracks_psychological_analysis',
        help='Nombre de la tabla (default: tracks_psychological_analysis)'
    )
    parser.add_argument(
        '--s3-location',
        default='s3://itam-analytics-ragp/spotifire/enhanced/psychological_analysis/',
        help='Ubicación S3 para los datos'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='Región AWS (default: us-east-1)'
    )
    parser.add_argument(
        '--show-queries',
        action='store_true',
        help='Mostrar consultas de ejemplo después de crear la tabla'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("🚀 Iniciando creación de tabla de análisis musical enriquecido...")
        logger.info(f"📊 Base de datos: {args.database_name}")
        logger.info(f"📋 Tabla: {args.table_name}")
        logger.info(f"🗂️  Ubicación S3: {args.s3_location}")
        
        # Inicializar creador
        creator = EnhancedGlueTableCreator(region_name=args.region)
        
        # Crear base de datos
        db_success = creator.create_enhanced_database(args.database_name)
        if not db_success:
            logger.error("❌ Falló la creación de la base de datos")
            sys.exit(1)
        
        # Crear tabla
        table_success = creator.create_enhanced_table(
            database_name=args.database_name,
            table_name=args.table_name,
            s3_location=args.s3_location
        )
        if not table_success:
            logger.error("❌ Falló la creación de la tabla")
            sys.exit(1)
        
        # Verificar creación
        verification_success = creator.verify_enhanced_table(
            database_name=args.database_name,
            table_name=args.table_name
        )
        
        if verification_success:
            print(f"\n🎉 ¡Tabla de análisis musical enriquecido creada exitosamente!")
            print(f"📊 Base de datos: {args.database_name}")
            print(f"📋 Tabla: {args.table_name}")
            print(f"🔢 Dimensiones de análisis: 29")
            print(f"🗂️  Ubicación: {args.s3_location}")
            
            if args.show_queries:
                print(f"\n📝 Consultas de ejemplo para usar con AWS Athena:")
                queries = creator.get_sample_queries(args.database_name, args.table_name)
                for i, query in enumerate(queries, 1):
                    print(f"\n--- Consulta {i} ---")
                    print(query.strip())
            
            print(f"\n🔄 Próximos pasos:")
            print(f"1. Ejecutar bedrock_music_analysis.py para generar análisis")
            print(f"2. Convertir JSON resultante a Parquet")
            print(f"3. Subir archivos Parquet a {args.s3_location}")
            print(f"4. Consultar datos usando AWS Athena")
            
        else:
            logger.error("❌ Falló la verificación de la tabla")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⏹️  Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error durante la creación: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()