#!/usr/bin/env python3
"""
Convertidor de datos de análisis musical de JSON a Parquet.
Este script toma los resultados del análisis de Bedrock y los convierte al formato Parquet
optimizado para consultas en AWS Athena.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging
import argparse
import sys
import boto3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('json_to_parquet_conversion.log')
    ]
)
logger = logging.getLogger(__name__)

class MusicAnalysisConverter:
    """
    Convertidor de análisis musical de JSON a Parquet.
    
    Esta clase maneja la conversión de los resultados JSON del análisis de Bedrock
    a formato Parquet optimizado para consultas analíticas en AWS Athena.
    """
    
    def __init__(self, region_name='us-east-1'):
        """
        Inicializa el convertidor con configuraciones necesarias.
        """
        self.region_name = region_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        
        # Dimensiones esperadas en el análisis
        self.expected_dimensions = [
            # Energía y tempo
            'energia_alta', 'energia_media', 'energia_baja',
            'tempo_rapido', 'tempo_medio', 'tempo_lento',
            
            # Espectro emocional
            'euforia', 'melancolia', 'serenidad', 'intensidad_dramatica', 'misterio', 'calidez',
            
            # Contextos situacionales
            'ejercicio_deporte', 'trabajo_concentracion', 'social_fiesta',
            'introspección', 'relajacion_descanso', 'viaje_movimiento',
            
            # Dimensiones culturales
            'nostalgia_retro', 'vanguardia_experimental', 'authenticity_underground',
            'universalidad', 'regionalidad', 'atemporalidad',
            
            # Efectos psicológicos
            'estimulacion_creativa', 'procesamiento_emocional', 'escape_mental',
            'motivacion_impulso', 'contemplacion_filosofica', 'conexion_social'
        ]
        
        logger.info("MusicAnalysisConverter inicializado")
    
    def load_json_analysis(self, json_file_path: str) -> Dict:
        """
        Carga el archivo JSON con los análisis musicales.
        
        Args:
            json_file_path: Ruta al archivo JSON con los análisis
            
        Returns:
            Diccionario con los datos cargados
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Archivo JSON cargado: {json_file_path}")
            
            if 'analyses' in data:
                logger.info(f"Encontrados {len(data['analyses'])} análisis en el archivo")
                return data
            else:
                logger.warning("El archivo JSON no tiene la estructura esperada")
                return {'analyses': []}
            
        except Exception as e:
            logger.error(f"Error cargando archivo JSON: {str(e)}")
            raise
    
    def validate_analysis_data(self, analysis: Dict) -> bool:
        """
        Valida que un análisis individual tenga todos los campos necesarios.
        
        Args:
            analysis: Diccionario con el análisis de una canción
            
        Returns:
            bool: True si el análisis es válido
        """
        required_fields = ['track_name', 'artist_name', 'album_name', 'dimensiones']
        
        # Verificar campos básicos
        for field in required_fields:
            if field not in analysis:
                logger.warning(f"Análisis faltante campo requerido: {field}")
                return False
        
        # Verificar dimensiones
        dimensiones = analysis.get('dimensiones', {})
        missing_dimensions = []
        
        for dim in self.expected_dimensions:
            if dim not in dimensiones:
                missing_dimensions.append(dim)
        
        if missing_dimensions:
            logger.warning(f"Análisis faltante dimensiones: {missing_dimensions[:5]}...")
            return False
        
        return True
    
    def normalize_analysis_data(self, analyses: List[Dict]) -> pd.DataFrame:
        """
        Normaliza los datos de análisis y los convierte a DataFrame.
        
        Args:
            analyses: Lista de análisis de canciones
            
        Returns:
            DataFrame normalizado listo para conversión a Parquet
        """
        logger.info("Normalizando datos de análisis...")
        
        normalized_data = []
        skipped_count = 0
        
        for analysis in analyses:
            if not self.validate_analysis_data(analysis):
                skipped_count += 1
                continue
            
            # Extraer datos básicos
            row_data = {
                'track_id': analysis.get('track_id', f"generated_{hash(analysis['track_name'] + analysis['artist_name'])}"),
                'track_name': analysis['track_name'],
                'artist_name': analysis['artist_name'],
                'album_name': analysis['album_name']
            }
            
            # Agregar todas las dimensiones
            dimensiones = analysis['dimensiones']
            for dimension in self.expected_dimensions:
                # Convertir a float y validar rango 0-100
                value = float(dimensiones.get(dimension, 0))
                value = max(0, min(100, value))  # Clamp a rango 0-100
                row_data[dimension] = value
            
            # Agregar metadatos del análisis
            row_data.update({
                'analysis_timestamp': datetime.now(),
                'analysis_model': 'claude-3-5-sonnet',
                'analysis_version': '1.0',
                'confidence_score': 85.0  # Score por defecto, se puede ajustar según necesidades
            })
            
            normalized_data.append(row_data)
        
        if skipped_count > 0:
            logger.warning(f"Se omitieron {skipped_count} análisis por datos incompletos")
        
        df = pd.DataFrame(normalized_data)
        logger.info(f"DataFrame creado con {len(df)} filas y {len(df.columns)} columnas")
        
        return df
    
    def optimize_dataframe_for_analytics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimiza el DataFrame para consultas analíticas.
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame optimizado
        """
        logger.info("Optimizando DataFrame para análisis...")
        
        # Convertir tipos de datos para optimización
        df = df.copy()
        
        # Strings categóricos para mejor compresión
        categorical_columns = ['track_name', 'artist_name', 'album_name', 'analysis_model', 'analysis_version']
        for col in categorical_columns:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        # Asegurar que las dimensiones sean float64
        for dimension in self.expected_dimensions:
            if dimension in df.columns:
                df[dimension] = df[dimension].astype('float64')
        
        # Confidence score como float
        if 'confidence_score' in df.columns:
            df['confidence_score'] = df['confidence_score'].astype('float64')
        
        # Ordenar por artista y track para mejor compresión
        df = df.sort_values(['artist_name', 'track_name'])
        
        logger.info("Optimización completada")
        return df
    
    def add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega métricas derivadas útiles para análisis.
        
        Args:
            df: DataFrame base
            
        Returns:
            DataFrame con métricas adicionales
        """
        logger.info("Agregando métricas derivadas...")
        
        df = df.copy()
        
        # Métricas de energía total
        df['energia_total'] = (df['energia_alta'] + df['energia_media'] + df['energia_baja']) / 3
        df['tempo_dominante'] = df[['tempo_rapido', 'tempo_medio', 'tempo_lento']].idxmax(axis=1)
        
        # Score de valencia emocional
        df['valencia_positiva'] = (df['euforia'] + df['calidez'] + df['serenidad']) / 3
        df['valencia_negativa'] = (df['melancolia'] + df['intensidad_dramatica']) / 2
        
        # Índices de contexto
        df['fitness_score'] = (df['ejercicio_deporte'] + df['motivacion_impulso'] + df['energia_alta']) / 3
        df['relaxation_score'] = (df['relajacion_descanso'] + df['serenidad'] + df['energia_baja']) / 3
        df['social_score'] = (df['social_fiesta'] + df['conexion_social'] + df['universalidad']) / 3
        df['creative_score'] = (df['estimulacion_creativa'] + df['vanguardia_experimental']) / 2
        
        # Perfil cultural
        df['mainstream_factor'] = (df['universalidad'] - df['authenticity_underground']) / 2
        df['temporal_relevance'] = (df['atemporalidad'] + df['nostalgia_retro']) / 2
        
        logger.info(f"Agregadas métricas derivadas. Total columnas: {len(df.columns)}")
        return df
    
    def save_to_parquet(self, df: pd.DataFrame, output_path: str, 
                       include_derived_metrics: bool = True) -> str:
        """
        Guarda el DataFrame como archivo Parquet optimizado.
        
        Args:
            df: DataFrame para guardar
            output_path: Ruta de salida para el archivo Parquet
            include_derived_metrics: Si incluir métricas derivadas
            
        Returns:
            Ruta del archivo generado
        """
        logger.info(f"Guardando DataFrame como Parquet: {output_path}")
        
        # Agregar métricas derivadas si se solicita
        if include_derived_metrics:
            df = self.add_derived_metrics(df)
        
        # Optimizar DataFrame
        df = self.optimize_dataframe_for_analytics(df)
        
        try:
            # Configurar opciones de Parquet para compresión y rendimiento
            table = pa.Table.from_pandas(df)
            
            pq.write_table(
                table,
                output_path,
                compression='snappy',  # Buena balance entre compresión y velocidad
                use_dictionary=True,   # Mejor compresión para strings repetidos
                write_statistics=True,  # Estadísticas para optimización de consultas
                row_group_size=10000   # Tamaño óptimo para consultas analíticas
            )
            
            file_size = Path(output_path).stat().st_size / (1024 * 1024)  # MB
            logger.info(f"Archivo Parquet creado exitosamente")
            logger.info(f"  - Ruta: {output_path}")
            logger.info(f"  - Tamaño: {file_size:.2f} MB")
            logger.info(f"  - Filas: {len(df):,}")
            logger.info(f"  - Columnas: {len(df.columns)}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error guardando archivo Parquet: {str(e)}")
            raise
    
    def upload_to_s3(self, local_path: str, s3_bucket: str, s3_key: str) -> bool:
        """
        Sube el archivo Parquet a S3.
        
        Args:
            local_path: Ruta local del archivo
            s3_bucket: Bucket de S3 destino
            s3_key: Clave (path) en S3
            
        Returns:
            bool: True si la subida fue exitosa
        """
        try:
            logger.info(f"Subiendo archivo a S3: s3://{s3_bucket}/{s3_key}")
            
            self.s3_client.upload_file(local_path, s3_bucket, s3_key)
            
            logger.info("Archivo subido exitosamente a S3")
            return True
            
        except Exception as e:
            logger.error(f"Error subiendo archivo a S3: {str(e)}")
            return False
    
    def get_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """
        Genera un reporte de calidad de datos.
        
        Args:
            df: DataFrame a analizar
            
        Returns:
            Diccionario con métricas de calidad
        """
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'dimension_columns': len(self.expected_dimensions),
            'missing_values': df.isnull().sum().sum(),
            'duplicate_tracks': df.duplicated(subset=['track_name', 'artist_name']).sum(),
            'dimension_statistics': {}
        }
        
        # Estadísticas por dimensión
        for dimension in self.expected_dimensions:
            if dimension in df.columns:
                series = df[dimension]
                report['dimension_statistics'][dimension] = {
                    'mean': float(series.mean()),
                    'std': float(series.std()),
                    'min': float(series.min()),
                    'max': float(series.max()),
                    'zero_values': int((series == 0).sum())
                }
        
        # Top artistas
        report['top_artists'] = df['artist_name'].value_counts().head(5).to_dict()
        
        # Rangos de valores por categoría
        energia_cols = ['energia_alta', 'energia_media', 'energia_baja']
        if all(col in df.columns for col in energia_cols):
            report['avg_energy_distribution'] = {
                col: float(df[col].mean()) for col in energia_cols
            }
        
        return report
    
    def convert_json_to_parquet(self, json_file: str, output_parquet: str, 
                              s3_bucket: str = None, s3_key: str = None,
                              include_derived_metrics: bool = True) -> Dict:
        """
        Proceso completo de conversión de JSON a Parquet.
        
        Args:
            json_file: Archivo JSON de entrada
            output_parquet: Archivo Parquet de salida
            s3_bucket: Bucket S3 opcional para subir el archivo
            s3_key: Clave S3 para el archivo
            include_derived_metrics: Si incluir métricas derivadas
            
        Returns:
            Diccionario con resultados del proceso
        """
        logger.info("🚀 Iniciando conversión completa JSON → Parquet")
        
        try:
            # Cargar datos JSON
            json_data = self.load_json_analysis(json_file)
            
            if not json_data['analyses']:
                raise ValueError("No se encontraron análisis válidos en el archivo JSON")
            
            # Normalizar datos
            df = self.normalize_analysis_data(json_data['analyses'])
            
            if len(df) == 0:
                raise ValueError("No se pudieron procesar análisis válidos")
            
            # Guardar como Parquet
            parquet_path = self.save_to_parquet(
                df, output_parquet, include_derived_metrics
            )
            
            # Generar reporte de calidad
            quality_report = self.get_data_quality_report(df)
            
            # Subir a S3 si se especifica
            s3_success = False
            if s3_bucket and s3_key:
                s3_success = self.upload_to_s3(parquet_path, s3_bucket, s3_key)
            
            result = {
                'success': True,
                'input_file': json_file,
                'output_file': parquet_path,
                'records_processed': len(df),
                'quality_report': quality_report,
                's3_upload': s3_success,
                's3_location': f"s3://{s3_bucket}/{s3_key}" if s3_bucket and s3_key else None
            }
            
            logger.info("✅ Conversión completada exitosamente")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error en conversión: {str(e)}")
            raise


def main():
    """Función principal para ejecutar la conversión desde línea de comandos."""
    parser = argparse.ArgumentParser(
        description='Convierte análisis musical de JSON a Parquet optimizado para Athena'
    )
    parser.add_argument(
        'json_file',
        help='Archivo JSON con análisis musicales de entrada'
    )
    parser.add_argument(
        '--output',
        help='Archivo Parquet de salida (default: auto-generado)'
    )
    parser.add_argument(
        '--s3-bucket',
        help='Bucket S3 para subir el archivo'
    )
    parser.add_argument(
        '--s3-key',
        help='Clave S3 para el archivo (ej: spotifire/enhanced/psychological_analysis/data.parquet)'
    )
    parser.add_argument(
        '--no-derived-metrics',
        action='store_true',
        help='No incluir métricas derivadas en el archivo final'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='Región AWS (default: us-east-1)'
    )
    
    args = parser.parse_args()
    
    try:
        # Determinar archivo de salida
        output_file = args.output
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"music_analysis_{timestamp}.parquet"
        
        # Inicializar convertidor
        converter = MusicAnalysisConverter(region_name=args.region)
        
        # Ejecutar conversión
        result = converter.convert_json_to_parquet(
            json_file=args.json_file,
            output_parquet=output_file,
            s3_bucket=args.s3_bucket,
            s3_key=args.s3_key,
            include_derived_metrics=not args.no_derived_metrics
        )
        
        # Mostrar resultados
        print(f"\n🎉 ¡Conversión completada exitosamente!")
        print(f"📄 Archivo entrada: {result['input_file']}")
        print(f"📄 Archivo salida: {result['output_file']}")
        print(f"📊 Registros procesados: {result['records_processed']:,}")
        
        if result['s3_upload']:
            print(f"☁️  Subido a S3: {result['s3_location']}")
        
        quality = result['quality_report']
        print(f"\n📋 Reporte de Calidad:")
        print(f"   • Total registros: {quality['total_records']:,}")
        print(f"   • Dimensiones analizadas: {quality['dimension_columns']}")
        print(f"   • Valores faltantes: {quality['missing_values']}")
        print(f"   • Tracks duplicados: {quality['duplicate_tracks']}")
        
        print(f"\n🎵 Top artistas procesados:")
        for artist, count in list(quality['top_artists'].items())[:3]:
            print(f"   • {artist}: {count} canciones")
        
        if quality.get('avg_energy_distribution'):
            print(f"\n⚡ Distribución promedio de energía:")
            for energy_type, avg in quality['avg_energy_distribution'].items():
                print(f"   • {energy_type}: {avg:.1f}%")
        
        print(f"\n🔄 Próximos pasos:")
        if result['s3_upload']:
            print(f"1. ✅ Archivo ya está en S3")
            print(f"2. Usar AWS Athena para consultar: {result['s3_location']}")
        else:
            print(f"1. Subir {output_file} a S3 manualmente")
            print(f"2. Configurar tabla en AWS Glue")
            print(f"3. Consultar datos usando AWS Athena")
        
    except KeyboardInterrupt:
        logger.info("⏹️  Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error durante la conversión: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()