#!/usr/bin/env python3
"""
Production script for creating AWS Glue Data Catalog database and table
for Spotify analytics data stored in Parquet format on S3.

This script should be executed once to set up the catalog structure.
It includes error handling and logging for production environments.

Usage:
    python3 create_glue_catalog.py [--database-name DATABASE] [--table-name TABLE] [--s3-location S3_PATH]
"""

import boto3
import json
import logging
import argparse
import sys
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('glue_catalog_setup.log')
    ]
)
logger = logging.getLogger(__name__)

class GlueCatalogManager:
    """
    Manages AWS Glue Data Catalog operations for Spotify analytics data.
    
    This class handles the creation and management of databases and tables
    in the AWS Glue Data Catalog, specifically designed for Parquet files
    containing Spotify listening data.
    """
    
    def __init__(self, region_name='us-east-1'):
        """
        Initialize the Glue catalog manager.
        
        Args:
            region_name (str): AWS region for Glue operations
        """
        try:
            self.glue_client = boto3.client('glue', region_name=region_name)
            self.region_name = region_name
            logger.info(f"Initialized Glue client for region: {region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Glue client: {str(e)}")
            raise
    
    def create_database(self, database_name, description=None):
        """
        Create a database in AWS Glue Data Catalog.
        
        This method creates a new database that will contain our table definitions.
        In Glue, a database is a logical grouping of tables that typically
        represent related data sources.
        
        Args:
            database_name (str): Name of the database to create
            description (str): Optional description for the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        if description is None:
            description = f"Database for Spotify analytics data - Created {datetime.now().isoformat()}"
        
        logger.info(f"Creating database: {database_name}")
        
        try:
            # Attempt to create the database
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': database_name,
                    'Description': description,
                    'Parameters': {
                        'created_by': 'spotify_etl_pipeline',
                        'created_at': datetime.now().isoformat(),
                        'purpose': 'analytics',
                        'data_format': 'parquet'
                    }
                }
            )
            logger.info(f"Successfully created database: {database_name}")
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Database {database_name} already exists. Continuing...")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"AWS Client Error creating database: {error_code} - {error_message}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error creating database: {str(e)}")
            return False
    
    def get_table_schema(self, table_type):
        """
        Define the schema for the Spotify analytics table.
        
        This schema corresponds to the output structure of our ETL job,
        which processes CSV files and creates Parquet files with additional
        derived fields like play_hour, season, etc.
        
        Returns:
            list: List of column definitions for the table
        """

        if (table_type=="user_tracks"):
            return [
            {
                'Name': 'user_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify user'
            },
            {
                'Name': 'played_at_utc', 
                'Type': 'timestamp', 
                'Comment': 'When the track was played in UTC timezone'
            },
            {
                'Name': 'played_at_mexico', 
                'Type': 'timestamp', 
                'Comment': 'When the track was played in Mexico timezone for behavioral analysis'
            },
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
                'Name': 'artist_id', 
                'Type': 'string', 
                'Comment': 'Spotify unique identifier for the artist'
            },
            {
                'Name': 'artist_name', 
                'Type': 'string', 
                'Comment': 'Name of the artist'
            },
            {
                'Name': 'album_id', 
                'Type': 'string', 
                'Comment': 'Spotify unique identifier for the album'
            },
            {
                'Name': 'album_name', 
                'Type': 'string', 
                'Comment': 'Name of the album'
            },
            {
                'Name': 'duration_ms', 
                'Type': 'bigint', 
                'Comment': 'Track duration in milliseconds'
            },
            {
                'Name': 'duration_minutes', 
                'Type': 'double', 
                'Comment': 'Track duration in minutes (derived field)'
            },
            {
                'Name': 'popularity', 
                'Type': 'int', 
                'Comment': 'Spotify popularity score from 0 to 100'
            },
            {
                'Name': 'explicit', 
                'Type': 'boolean', 
                'Comment': 'Whether the track contains explicit content'
            },
            {
                'Name': 'play_hour', 
                'Type': 'int', 
                'Comment': 'Hour of day when played (0-23) in Mexico timezone'
            },
            {
                'Name': 'play_day_of_week', 
                'Type': 'int', 
                'Comment': 'Day of week when played (1=Sunday, 7=Saturday)'
            },
            {
                'Name': 'play_month', 
                'Type': 'int', 
                'Comment': 'Month when played (1-12)'
            },
            {
                'Name': 'play_year', 
                'Type': 'int', 
                'Comment': 'Year when played'
            },
            {
                'Name': 'season', 
                'Type': 'string', 
                'Comment': 'Season when played (Spring, Summer, Fall, Winter)'
            },
            {
                'Name': 'processed_at', 
                'Type': 'timestamp', 
                'Comment': 'When this record was processed by the ETL pipeline'
            }
        ]
        elif (table_type=="top_tracks"):
            return [
            {
                'Name': 'user_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify user'
            },
            {
                'Name': 'ith_preference', 
                'Type': 'int', 
                'Comment': 'Order of track in preferences'
            },
            {
                'Name': 'track_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify track'
            },
            {
                'Name': 'track_name', 
                'Type': 'string', 
                'Comment': 'Name of the Spotify track'
            },
            {
                'Name': 'artists_id', 
                'Type': 'array', 
                'Comment': 'Uniques identifiers for the Spotify artists'
            },
            {
                'Name': 'album_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify album'
            },
            {
                'Name': 'track_popularity', 
                'Type': 'int', 
                'Comment': 'Track`s percentage popularity'
            },
            {
                'Name': 'explicit', 
                'Type': 'boolean', 
                'Comment': 'Flag to indicate explicity'
            },
            {
                'Name': 'duration', 
                'Type': 'int', 
                'Comment': 'Track`s duration (milliseconds)'
            },
            {
                'Name': 'processed_at', 
                'Type': 'timestamp', 
                'Comment': 'Timestamp of processing'
            }]

        elif (table_type=="likes"):
            return [
            {
                'Name': 'user_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify user'
            },
            {
                'Name': 'added_at_utc', 
                'Type': 'timestamp', 
                'Comment': 'UTC timestamp of adding'
            },
            {
                'Name': 'added_at_mexico', 
                'Type': 'timestamp', 
                'Comment': 'UTC-6 timestamp of adding'
            },
            {
                'Name': 'track_id', 
                'Type': 'string', 
                'Comment': 'Uniques identifiers for the Spotify track'
            },
            {
                'Name': 'track_name', 
                'Type': 'string', 
                'Comment': 'Name of the Spotify track'
            },
            {
                'Name': 'artists_id', 
                'Type': 'array', 
                'Comment': 'Uniques identifiers for the Spotify artists'
            },
            {
                'Name': 'album_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify album'
            },
            {
                'Name': 'track_popularity', 
                'Type': 'int', 
                'Comment': 'Track`s percentage popularity'
            },
            {
                'Name': 'explicit', 
                'Type': 'boolean', 
                'Comment': 'Flag to indicate explicity'
            },
            {
                'Name': 'duration', 
                'Type': 'int', 
                'Comment': 'Track`s duration (milliseconds)'
            },
            {
                'Name': 'processed_at', 
                'Type': 'timestamp', 
                'Comment': 'Timestamp of processing'
            }]
        elif (table_type=="followed_artists"):
            return [
            {
                'Name': 'user_id', 
                'Type': 'string', 
                'Comment': 'Unique identifier for the Spotify user'
            },
            {
                'Name': 'artist_id', 
                'Type': 'string', 
                'Comment': 'Uniques identifiers for the Spotify artist'
            },
            {
                'Name': 'processed_at', 
                'Type': 'timestamp', 
                'Comment': 'Timestamp of processing'
            }]
        
    
    def create_table(self, database_name, table_name, s3_location):
        """
        Create a table in AWS Glue Data Catalog that points to Parquet files in S3.
        
        This creates an external table definition that tells query engines like
        Athena where to find the data and how to interpret it. The table doesn't
        store data itself - it's a metadata definition that points to S3.
        
        Args:
            database_name (str): Name of the database to contain the table
            table_name (str): Name of the table to create. One of: ['user_tracks', 'top_tracks', 'likes', 'followed_artists']
            s3_location (str): S3 path where Parquet files are stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Creating table: {database_name}.{table_name}")
        logger.info(f"S3 location: {s3_location}")
        
        # Get the schema definition

        table_schema = self.get_table_schema(table_name)
        table_descr = {
            "user_tracks":'Unified table containing Spotify listening data for all users',
            "top_tracks": 'Unified table containing Spotify top tracks data for all users',
            "likes": 'Unified table containing Spotify tracks liked data for all users',
            "followed_artists": 'Unified table containing Spotify followed artist data for all users'
        }
        
        try:
            # Create the table with full configuration for Parquet files
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': table_descr[table_name],
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY',
                        'classification': 'parquet',
                        'created_by': 'spotify_etl_pipeline',
                        'created_at': datetime.now().isoformat(),
                        'data_source': 'spotify_api',
                        'update_frequency': 'daily'
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
            
            logger.info(f"Successfully created table: {database_name}.{table_name}")
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Table {database_name}.{table_name} already exists")
            
            # For production, we should handle existing tables carefully
            # We'll update the table with the current schema
            return self.update_existing_table(database_name, table_name, s3_location)
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"AWS Client Error creating table: {error_code} - {error_message}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error creating table: {str(e)}")
            return False
    
    def update_existing_table(self, database_name, table_name, s3_location):
        """
        Update an existing table with the current schema definition.
        
        This is important for production environments where the table might
        already exist but we want to ensure it has the latest schema.
        
        Args:
            database_name (str): Name of the database containing the table
            table_name (str): Name of the table to update
            s3_location (str): S3 path where Parquet files are stored
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Updating existing table: {database_name}.{table_name}")
        
        table_schema = self.get_table_schema()
        
        try:
            self.glue_client.update_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': 'Unified table containing Spotify listening data for all users (UPDATED)',
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY',
                        'classification': 'parquet',
                        'updated_by': 'spotify_etl_pipeline',
                        'updated_at': datetime.now().isoformat(),
                        'data_source': 'spotify_api',
                        'update_frequency': 'daily'
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
            
            logger.info(f"Successfully updated table: {database_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating existing table: {str(e)}")
            return False
    
    def verify_setup(self, database_name, table_name):
        """
        Verify that the database and table were created successfully.
        
        This performs validation checks to ensure everything is properly
        configured before declaring success.
        
        Args:
            database_name (str): Name of the database to verify
            table_name (str): Name of the table to verify
            
        Returns:
            bool: True if verification passes, False otherwise
        """
        logger.info("Verifying database and table creation...")
        
        try:
            # Verify database exists
            db_response = self.glue_client.get_database(Name=database_name)
            logger.info(f"Database verified: {db_response['Database']['Name']}")
            
            # Verify table exists and get its details
            table_response = self.glue_client.get_table(
                DatabaseName=database_name, 
                Name=table_name
            )
            
            table_info = table_response['Table']
            logger.info(f"Table verified: {table_info['Name']}")
            logger.info(f"Table location: {table_info['StorageDescriptor']['Location']}")
            logger.info(f"Number of columns: {len(table_info['StorageDescriptor']['Columns'])}")
            
            return True
            
        except ClientError as e:
            logger.error(f"Verification failed: {e.response['Error']['Message']}")
            return False
        except Exception as e:
            logger.error(f"Verification error: {str(e)}")
            return False

def main():
    """
    Main execution function with command line argument parsing.
    
    This function handles the complete workflow of setting up the Glue
    Data Catalog for Spotify analytics data.
    """
    parser = argparse.ArgumentParser(
        description='Create AWS Glue Data Catalog database and table for Spotify analytics'
    )
    parser.add_argument(
        '--database-name', 
        default='spotify_analytics',
        help='Name of the Glue database to create (default: spotify_analytics)'
    )
    parser.add_argument(
        '--table-name',
        default='user_tracks',
        help='Name of the Glue table to create (default: user_tracks)'
    )
    parser.add_argument(
        '--s3-location',
        default='s3://itam-analytics-ragp/spotifire/processed/individual/',
        help='S3 location where Parquet files are stored'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    
    args = parser.parse_args()
    
    logger.info("Starting AWS Glue Data Catalog setup for Spotify analytics")
    logger.info(f"Database: {args.database_name}")
    logger.info(f"Table: {args.table_name}")
    logger.info(f"S3 Location: {args.s3_location}")
    logger.info(f"Region: {args.region}")
    
    try:
        # Initialize the catalog manager
        catalog_manager = GlueCatalogManager(region_name=args.region)
        
        # Create database
        database_success = catalog_manager.create_database(
            database_name=args.database_name,
            description=f"Database for Spotify analytics data processing and analysis"
        )
        
        if not database_success:
            logger.error("Failed to create database. Exiting.")
            sys.exit(1)
        
        # Create table
        table_success = catalog_manager.create_table(
            database_name=args.database_name,
            table_name=args.table_name,
            s3_location=args.s3_location
        )
        
        if not table_success:
            logger.error("Failed to create table. Exiting.")
            sys.exit(1)
        
        # Verify setup
        verification_success = catalog_manager.verify_setup(
            database_name=args.database_name,
            table_name=args.table_name
        )
        
        if verification_success:
            logger.info("AWS Glue Data Catalog setup completed successfully")
            logger.info(f"You can now query the data using AWS Athena:")
            logger.info(f"  SELECT user_id, COUNT(*) as total_plays")
            logger.info(f"  FROM {args.database_name}.{args.table_name}")
            logger.info(f"  GROUP BY user_id")
            logger.info(f"  ORDER BY total_plays DESC;")
        else:
            logger.error("Setup verification failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during setup: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()