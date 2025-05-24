#!/usr/bin/env python3
"""
Enfoque MEJORADO: Usar la base de datos existente con tabla adicional
Mantiene todo unificado pero organizado.
"""

import boto3
import logging
import argparse
from datetime import datetime

class UnifiedGlueTableCreator:
    """
    Crea tabla psicológica en la MISMA base de datos existente.
    Ventajas: Unificado, consultas JOIN simples, menos complejidad.
    """
    
    def __init__(self, region_name='us-east-1'):
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.region_name = region_name
        logging.info("UnifiedGlueTableCreator inicializado")
    
    def create_psychological_table_in_existing_db(self, database_name='spotify_analytics', 
                                                table_name='tracks_psychological_analysis',
                                                s3_location=None):
        """
        Crea tabla psicológica en la base de datos EXISTENTE.
        
        Ventajas:
        - Una sola base de datos
        - JOINs simples entre tablas
        - Menos configuración
        - Mantiene organización existente
        """
        
        if not s3_location:
            s3_location = 's3://itam-analytics-ragp/spotifire/enhanced/psychological_analysis/'
        
        logging.info(f"Creando tabla psicológica en DB EXISTENTE: {database_name}")
        
        # Esquema optimizado que complementa (no duplica) datos existentes
        enhanced_schema = [
            # === IDENTIFICADOR PRINCIPAL ===
            {
                'Name': 'track_id',
                'Type': 'string',
                'Comment': 'Spotify track ID - ENLAZA con tabla user_tracks existente'
            },
            
            # === METADATOS BÁSICOS (solo si no están en otra tabla) ===
            {
                'Name': 'track_name',
                'Type': 'string',
                'Comment': 'Nombre de la canción'
            },
            {
                'Name': 'artist_name', 
                'Type': 'string',
                'Comment': 'Nombre del artista'
            },
            {
                'Name': 'album_name',
                'Type': 'string',
                'Comment': 'Nombre del álbum'
            },
            
            # === 29 DIMENSIONES PSICOLÓGICAS ===
            # Energía y Tempo
            {
                'Name': 'energia_alta',
                'Type': 'double',
                'Comment': 'Música que genera activación física y mental intensa (0-100)'
            },
            {
                'Name': 'energia_media',
                'Type': 'double', 
                'Comment': 'Música con energía moderada (0-100)'
            },
            {
                'Name': 'energia_baja',
                'Type': 'double',
                'Comment': 'Música calmada, relajante (0-100)'
            },
            {
                'Name': 'tempo_rapido',
                'Type': 'double',
                'Comment': 'Ritmo acelerado que invita al movimiento (0-100)'
            },
            {
                'Name': 'tempo_medio',
                'Type': 'double',
                'Comment': 'Ritmo moderado (0-100)'
            },
            {
                'Name': 'tempo_lento',
                'Type': 'double',
                'Comment': 'Ritmo pausado, ideal para reflexión (0-100)'
            },
            
            # Espectro Emocional
            {
                'Name': 'euforia',
                'Type': 'double',
                'Comment': 'Alegría intensa, celebración (0-100)'
            },
            {
                'Name': 'melancolia',
                'Type': 'double',
                'Comment': 'Tristeza bella, nostalgia (0-100)'
            },
            {
                'Name': 'serenidad',
                'Type': 'double',
                'Comment': 'Paz interior, calma (0-100)'
            },
            {
                'Name': 'intensidad_dramatica',
                'Type': 'double',
                'Comment': 'Emociones fuertes, drama, pasión (0-100)'
            },
            {
                'Name': 'misterio',
                'Type': 'double',
                'Comment': 'Atmósferas enigmáticas, suspense (0-100)'
            },
            {
                'Name': 'calidez',
                'Type': 'double',
                'Comment': 'Sensación de confort, abrazo emocional (0-100)'
            },
            
            # Contextos Situacionales
            {
                'Name': 'ejercicio_deporte',
                'Type': 'double',
                'Comment': 'Perfecta para actividad física (0-100)'
            },
            {
                'Name': 'trabajo_concentracion',
                'Type': 'double',
                'Comment': 'Ideal para focus mental (0-100)'
            },
            {
                'Name': 'social_fiesta',
                'Type': 'double',
                'Comment': 'Música para compartir, celebrar en grupo (0-100)'
            },
            {
                'Name': 'introspección',
                'Type': 'double',
                'Comment': 'Para reflexión personal (0-100)'
            },
            {
                'Name': 'relajacion_descanso',
                'Type': 'double',
                'Comment': 'Para descomprimir tensiones (0-100)'
            },
            {
                'Name': 'viaje_movimiento',
                'Type': 'double',
                'Comment': 'Banda sonora de aventuras (0-100)'
            },
            
            # Dimensiones Culturales
            {
                'Name': 'nostalgia_retro',
                'Type': 'double',
                'Comment': 'Evoca épocas pasadas (0-100)'
            },
            {
                'Name': 'vanguardia_experimental',
                'Type': 'double',
                'Comment': 'Sonidos innovadores (0-100)'
            },
            {
                'Name': 'authenticity_underground',
                'Type': 'double',
                'Comment': 'Autenticidad cultural, alejado del mainstream (0-100)'
            },
            {
                'Name': 'universalidad',
                'Type': 'double',
                'Comment': 'Trasciende barreras culturales (0-100)'
            },
            {
                'Name': 'regionalidad',
                'Type': 'double',
                'Comment': 'Conectado a cultura específica (0-100)'
            },
            {
                'Name': 'atemporalidad',
                'Type': 'double',
                'Comment': 'Relevante en cualquier momento (0-100)'
            },
            
            # Efectos Psicológicos
            {
                'Name': 'estimulacion_creativa',
                'Type': 'double',
                'Comment': 'Cataliza pensamiento creativo (0-100)'
            },
            {
                'Name': 'procesamiento_emocional',
                'Type': 'double',
                'Comment': 'Ayuda a procesar emociones complejas (0-100)'
            },
            {
                'Name': 'escape_mental',
                'Type': 'double',
                'Comment': 'Desconexión de realidad cotidiana (0-100)'
            },
            {
                'Name': 'motivacion_impulso',
                'Type': 'double',
                'Comment': 'Genera determinación (0-100)'
            },
            {
                'Name': 'contemplacion_filosofica',
                'Type': 'double',
                'Comment': 'Reflexiones profundas sobre existencia (0-100)'
            },
            {
                'Name': 'conexion_social',
                'Type': 'double',
                'Comment': 'Facilita sentimientos de pertenencia (0-100)'
            },
            
            # === METADATOS DEL ANÁLISIS ===
            {
                'Name': 'analysis_timestamp',
                'Type': 'timestamp',
                'Comment': 'Cuándo se realizó el análisis'
            },
            {
                'Name': 'analysis_model',
                'Type': 'string',
                'Comment': 'Modelo de IA utilizado (claude-3-5-sonnet)'
            },
            {
                'Name': 'confidence_score',
                'Type': 'double',
                'Comment': 'Puntuación de confianza del análisis (0-100)'
            }
        ]
        
        try:
            self.glue_client.create_table(
                DatabaseName=database_name,  # ¡MISMA DB que ya usas!
                TableInput={
                    'Name': table_name,
                    'Description': 'Análisis psicológico de canciones - complementa user_tracks existente',
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY',
                        'classification': 'parquet',
                        'created_by': 'spotifire_enhanced_unified',
                        'created_at': datetime.now().isoformat(),
                        'relationship': 'ENHANCES_user_tracks_via_track_id',
                        'join_key': 'track_id'
                    },
                    'StorageDescriptor': {
                        'Columns': enhanced_schema,
                        'Location': s3_location,
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {
                                'serialization.format': '1'
                            }
                        }
                    }
                }
            )
            
            logging.info(f"✅ Tabla {table_name} creada exitosamente en DB existente {database_name}")
            logging.info(f"🔗 Se puede hacer JOIN con user_tracks usando track_id")
            
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logging.warning(f"Tabla {table_name} ya existe en {database_name}")
            return self.update_existing_table(database_name, table_name, s3_location, enhanced_schema)
            
        except Exception as e:
            logging.error(f"Error creando tabla unificada: {str(e)}")
            return False
    
    def update_existing_table(self, database_name, table_name, s3_location, schema):
        """Actualiza tabla existente con nuevo esquema."""
        try:
            self.glue_client.update_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': 'Análisis psicológico de canciones - complementa user_tracks existente (ACTUALIZADO)',
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY', 
                        'classification': 'parquet',
                        'updated_at': datetime.now().isoformat(),
                        'relationship': 'ENHANCES_user_tracks_via_track_id'
                    },
                    'StorageDescriptor': {
                        'Columns': schema,
                        'Location': s3_location,
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {
                                'serialization.format': '1'
                            }
                        }
                    }
                }
            )
            
            logging.info(f"✅ Tabla {table_name} actualizada en {database_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error actualizando tabla: {str(e)}")
            return False
    
    def generate_unified_queries(self, database_name='spotify_analytics'):
        """
        Genera consultas de ejemplo que aprovechan AMBAS tablas en una sola DB.
        ¡Esta es la ventaja principal del enfoque unificado!
        """
        
        queries = {
            "user_psychological_profile": f"""
-- Perfil psicológico completo del usuario (JOIN entre tablas)
SELECT 
    u.user_id,
    COUNT(DISTINCT u.track_id) as total_tracks_played,
    SUM(u.play_count) as total_plays,
    
    -- Promedios ponderados de dimensiones psicológicas
    SUM(p.energia_alta * u.play_count) / SUM(u.play_count) as avg_energia_alta,
    SUM(p.melancolia * u.play_count) / SUM(u.play_count) as avg_melancolia,
    SUM(p.estimulacion_creativa * u.play_count) / SUM(u.play_count) as avg_creatividad,
    SUM(p.serenidad * u.play_count) / SUM(u.play_count) as avg_serenidad,
    SUM(p.social_fiesta * u.play_count) / SUM(u.play_count) as avg_social,
    
    -- Metadata
    MAX(u.played_at_mexico) as last_played,
    COUNT(DISTINCT p.artist_name) as unique_artists
    
FROM (
    SELECT 
        user_id, 
        track_id, 
        COUNT(*) as play_count,
        MAX(played_at_mexico) as played_at_mexico
    FROM {database_name}.user_tracks 
    WHERE date(played_at_mexico) >= current_date - interval '90' day
    GROUP BY user_id, track_id
) u
INNER JOIN {database_name}.tracks_psychological_analysis p ON u.track_id = p.track_id
WHERE u.user_id = 'TU_USER_ID'
GROUP BY u.user_id;
""",

            "mood_recommendations_unified": f"""
-- Recomendaciones de mood usando AMBAS tablas
WITH user_recent_tracks AS (
    SELECT DISTINCT track_id
    FROM {database_name}.user_tracks
    WHERE user_id = 'TU_USER_ID'
        AND date(played_at_mexico) >= current_date - interval '30' day
),
mood_compatible_tracks AS (
    SELECT 
        p.track_name,
        p.artist_name,
        p.album_name,
        p.track_id,
        
        -- Score para mood "workout"
        (p.ejercicio_deporte + p.energia_alta + p.motivacion_impulso + p.tempo_rapido) / 4 as workout_score,
        
        -- Score para mood "focus"  
        (p.trabajo_concentracion + p.energia_media + p.estimulacion_creativa) / 3 as focus_score,
        
        -- Score para mood "relax"
        (p.relajacion_descanso + p.serenidad + p.energia_baja) / 3 as relax_score
        
    FROM {database_name}.tracks_psychological_analysis p
    LEFT JOIN user_recent_tracks urt ON p.track_id = urt.track_id
    WHERE urt.track_id IS NULL  -- Excluir canciones ya escuchadas recientemente
)
SELECT 
    track_name,
    artist_name,
    'workout' as recommended_mood,
    workout_score as compatibility_score
FROM mood_compatible_tracks
WHERE workout_score > 70
ORDER BY workout_score DESC
LIMIT 10

UNION ALL

SELECT 
    track_name,
    artist_name, 
    'focus' as recommended_mood,
    focus_score as compatibility_score
FROM mood_compatible_tracks  
WHERE focus_score > 70
ORDER BY focus_score DESC
LIMIT 10;
""",

            "trending_psychological_dimensions": f"""
-- Análisis de tendencias psicológicas por tiempo
SELECT 
    date_trunc('week', u.played_at_mexico) as week_start,
    
    -- Tendencias emocionales
    AVG(p.euforia) as avg_euphoria,
    AVG(p.melancolia) as avg_melancholy,
    AVG(p.serenidad) as avg_serenity,
    
    -- Tendencias energéticas  
    AVG(p.energia_alta) as avg_high_energy,
    AVG(p.energia_baja) as avg_low_energy,
    
    -- Tendencias culturales
    AVG(p.nostalgia_retro) as avg_nostalgia,
    AVG(p.vanguardia_experimental) as avg_experimental,
    
    -- Volume
    COUNT(DISTINCT u.user_id) as unique_users,
    COUNT(*) as total_plays
    
FROM {database_name}.user_tracks u
INNER JOIN {database_name}.tracks_psychological_analysis p ON u.track_id = p.track_id
WHERE date(u.played_at_mexico) >= current_date - interval '12' week
GROUP BY date_trunc('week', u.played_at_mexico)
ORDER BY week_start DESC;
""",

            "artist_psychological_signature": f"""
-- Signature psicológica por artista
SELECT 
    p.artist_name,
    COUNT(DISTINCT p.track_id) as total_tracks_analyzed,
    COUNT(DISTINCT u.user_id) as unique_listeners,
    SUM(play_counts.total_plays) as total_plays,
    
    -- Signature emocional del artista
    AVG(p.energia_alta) as signature_energy,
    AVG(p.melancolia) as signature_melancholy, 
    AVG(p.vanguardia_experimental) as signature_experimental,
    AVG(p.estimulacion_creativa) as signature_creative,
    
    -- Clasificación automática
    CASE 
        WHEN AVG(p.energia_alta) > 70 THEN 'High Energy Artist'
        WHEN AVG(p.melancolia) > 60 THEN 'Melancholic Artist'  
        WHEN AVG(p.vanguardia_experimental) > 70 THEN 'Experimental Artist'
        WHEN AVG(p.estimulacion_creativa) > 70 THEN 'Creative Catalyst'
        ELSE 'Balanced Artist'
    END as artist_psychological_type
    
FROM {database_name}.tracks_psychological_analysis p
INNER JOIN (
    SELECT 
        track_id, 
        COUNT(DISTINCT user_id) as unique_listeners,
        COUNT(*) as total_plays
    FROM {database_name}.user_tracks
    WHERE date(played_at_mexico) >= current_date - interval '90' day  
    GROUP BY track_id
) play_counts ON p.track_id = play_counts.track_id
LEFT JOIN {database_name}.user_tracks u ON p.track_id = u.track_id
GROUP BY p.artist_name
HAVING COUNT(DISTINCT p.track_id) >= 3  -- Solo artistas con suficientes tracks
ORDER BY total_plays DESC
LIMIT 50;
"""
        }
        
        return queries


def main():
    """Función principal para crear tabla unificada."""
    parser = argparse.ArgumentParser(
        description='Crear tabla psicológica en base de datos EXISTENTE (enfoque unificado)'
    )
    parser.add_argument(
        '--database-name',
        default='spotify_analytics',  # ¡TU DB EXISTENTE!
        help='Nombre de tu base de datos EXISTENTE'
    )
    parser.add_argument(
        '--table-name', 
        default='tracks_psychological_analysis',
        help='Nombre de la nueva tabla psicológica'
    )
    parser.add_argument(
        '--s3-location',
        default='s3://itam-analytics-ragp/spotifire/enhanced/psychological_analysis/',
        help='Ubicación S3 para los datos psicológicos'
    )
    parser.add_argument(
        '--show-sample-queries',
        action='store_true',
        help='Mostrar consultas de ejemplo que aprovechan ambas tablas'
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    print("🔗 Enfoque UNIFICADO - Tabla Psicológica en DB Existente")
    print("=" * 60)
    print(f"Base de datos: {args.database_name} (EXISTENTE)")
    print(f"Nueva tabla: {args.table_name}")
    print(f"Ubicación S3: {args.s3_location}")
    print()
    
    # Crear tabla unificada
    creator = UnifiedGlueTableCreator()
    
    success = creator.create_psychological_table_in_existing_db(
        database_name=args.database_name,
        table_name=args.table_name,
        s3_location=args.s3_location
    )
    
    if success:
        print("✅ Tabla psicológica creada exitosamente en tu DB existente!")
        print()
        print("🔗 Ventajas del enfoque unificado:")
        print("   • Una sola base de datos para todo")
        print("   • JOINs simples entre user_tracks y psychological_analysis")
        print("   • Menos configuración y complejidad")
        print("   • Consultas más eficientes")
        print()
        
        if args.show_sample_queries:
            print("📊 Consultas de ejemplo (aprovechan AMBAS tablas):")
            print("=" * 50)
            
            queries = creator.generate_unified_queries(args.database_name)
            
            for query_name, query_sql in queries.items():
                print(f"\n--- {query_name.replace('_', ' ').title()} ---")
                print(query_sql.strip())
                print()
        
        print("🚀 Próximos pasos:")
        print("1. Ejecutar análisis: python3 scripts/bedrock_music_analysis.py")
        print("2. Convertir a Parquet: python3 scripts/json_to_parquet_converter.py")
        print(f"3. Usar consultas JOIN entre user_tracks y {args.table_name}")
        print("4. Integrar con dashboard usando una sola base de datos")
        
    else:
        print("❌ Error creando tabla psicológica")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())