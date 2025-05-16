#!/usr/bin/env python3
"""
Script para sincronizar los archivos de datos de Spotify con un bucket de S3.

Este script recorre todas las carpetas de usuarios dentro del directorio de datos
recolectados y sube los archivos CSV a un bucket de S3, manteniendo la estructura
de directorios por usuario.

Uso:
    python spotify_s3_uploader.py [--dry-run]

Opciones:
    --dry-run    Solo simula la operación sin realizar cambios en S3
    --help       Muestra este mensaje de ayuda

Configuración:
    El script asume que las credenciales de AWS están configuradas mediante
    las variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY,
    o mediante el archivo ~/.aws/credentials
"""

import os
import sys
import argparse
import logging
import glob
import boto3
from botocore.exceptions import ClientError

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("spotify_s3_uploader.log")
    ]
)
logger = logging.getLogger("spotify_s3_uploader")

# Configuraciones por defecto
DEFAULT_DATA_DIR = "/home/ec2-user/spotifire/data/collected_data"
DEFAULT_S3_BUCKET = "itam-analytics-ragp"
DEFAULT_S3_PREFIX = "spotifire/raw"

def parse_arguments():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description="Sincroniza los archivos de datos de Spotify con un bucket de S3"
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directorio donde se encuentran las carpetas de usuarios (default: {DEFAULT_DATA_DIR})"
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_S3_BUCKET,
        help=f"Nombre del bucket de S3 (default: {DEFAULT_S3_BUCKET})"
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_S3_PREFIX,
        help=f"Prefijo en el bucket de S3 (default: {DEFAULT_S3_PREFIX})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula la operación sin realizar cambios en S3"
    )
    
    return parser.parse_args()

def get_s3_client():
    """Configura y devuelve un cliente de S3"""
    try:
        s3_client = boto3.client('s3')
        return s3_client
    except Exception as e:
        logger.error(f"Error al configurar el cliente de S3: {e}")
        sys.exit(1)

def upload_file_to_s3(file_path, bucket, object_name, dry_run=False):
    """
    Sube un archivo a un bucket de S3
    
    Args:
        file_path: Ruta local del archivo
        bucket: Nombre del bucket de S3
        object_name: Ruta del objeto en S3
        dry_run: Si es True, solo simula la operación
        
    Returns:
        True si el archivo se subió correctamente, False en caso contrario
    """
    if dry_run:
        logger.info(f"[DRY RUN] Se subiría {file_path} a s3://{bucket}/{object_name}")
        return True
    
    s3_client = get_s3_client()
    try:
        logger.info(f"Subiendo {file_path} a s3://{bucket}/{object_name}")
        s3_client.upload_file(file_path, bucket, object_name)
        return True
    except ClientError as e:
        logger.error(f"Error al subir {file_path} a S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado al subir {file_path} a S3: {e}")
        return False

def check_existing_files(bucket, prefix, dry_run=False):
    """
    Obtiene un conjunto de rutas de archivos que ya existen en el bucket de S3
    
    Args:
        bucket: Nombre del bucket de S3
        prefix: Prefijo para filtrar los objetos
        dry_run: Si es True, devuelve un conjunto vacío
        
    Returns:
        Un conjunto de rutas de objetos existentes en S3
    """
    if dry_run:
        return set()
    
    s3_client = get_s3_client()
    existing_files = set()
    
    try:
        # Listar objetos con paginación
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    existing_files.add(obj['Key'])
        
        logger.info(f"Se encontraron {len(existing_files)} archivos existentes en s3://{bucket}/{prefix}")
        return existing_files
    except Exception as e:
        logger.error(f"Error al listar archivos existentes en S3: {e}")
        return set()

def sync_user_data(user_dir, user_id, bucket, s3_prefix, existing_files, dry_run=False):
    """
    Sincroniza los archivos CSV de un usuario con S3
    
    Args:
        user_dir: Directorio local del usuario
        user_id: ID del usuario
        bucket: Nombre del bucket de S3
        s3_prefix: Prefijo base en S3
        existing_files: Conjunto de archivos que ya existen en S3
        dry_run: Si es True, solo simula la operación
        
    Returns:
        Tupla con el número de archivos procesados, subidos y omitidos
    """
    # Obtener lista de archivos CSV en el directorio del usuario
    csv_pattern = os.path.join(user_dir, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.info(f"No se encontraron archivos CSV para el usuario {user_id}")
        return 0, 0, 0
    
    # Contadores para estadísticas
    processed = 0
    uploaded = 0
    skipped = 0
    
    for csv_file in csv_files:
        processed += 1
        file_name = os.path.basename(csv_file)
        
        # Construir la ruta completa en S3
        s3_object_name = f"{s3_prefix}/{user_id}/{file_name}"
        
        # Verificar si el archivo ya existe en S3
        if s3_object_name in existing_files:
            logger.debug(f"El archivo {file_name} ya existe en S3, omitiendo")
            skipped += 1
            continue
        
        # Subir el archivo a S3
        if upload_file_to_s3(csv_file, bucket, s3_object_name, dry_run):
            uploaded += 1
    
    logger.info(f"Usuario {user_id}: {processed} archivos procesados, {uploaded} subidos, {skipped} omitidos")
    return processed, uploaded, skipped

def main():
    """Función principal del script"""
    args = parse_arguments()
    
    # Verificar que el directorio de datos existe
    if not os.path.isdir(args.data_dir):
        logger.error(f"El directorio de datos no existe: {args.data_dir}")
        return 1
    
    # Modo de ejecución
    if args.dry_run:
        logger.info("Ejecutando en modo simulación (--dry-run)")
    
    # Obtener la lista de carpetas de usuarios
    user_dirs = [d for d in os.listdir(args.data_dir) 
                if os.path.isdir(os.path.join(args.data_dir, d))]
    
    if not user_dirs:
        logger.warning(f"No se encontraron carpetas de usuarios en {args.data_dir}")
        return 0
    
    logger.info(f"Se encontraron {len(user_dirs)} usuarios: {', '.join(user_dirs)}")
    
    # Obtener lista de archivos existentes en S3 para evitar subidas duplicadas
    existing_files = check_existing_files(args.bucket, args.prefix, args.dry_run)
    
    # Estadísticas globales
    total_users = len(user_dirs)
    total_processed = 0
    total_uploaded = 0
    total_skipped = 0
    
    # Procesar cada usuario
    for user_id in user_dirs:
        user_dir = os.path.join(args.data_dir, user_id)
        logger.info(f"Procesando usuario: {user_id}")
        
        processed, uploaded, skipped = sync_user_data(
            user_dir, user_id, args.bucket, args.prefix, existing_files, args.dry_run
        )
        
        total_processed += processed
        total_uploaded += uploaded
        total_skipped += skipped
    
    # Resumen final
    logger.info("=" * 50)
    logger.info("Resumen de sincronización:")
    logger.info(f"- Usuarios procesados: {total_users}")
    logger.info(f"- Archivos procesados: {total_processed}")
    logger.info(f"- Archivos subidos: {total_uploaded}")
    logger.info(f"- Archivos omitidos: {total_skipped}")
    
    return 0

if __name__ == "__main__":
    exit(main())