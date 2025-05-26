#!/usr/bin/env python3
"""
Production script for creating AWS Glue Data Catalog database and tables
for Spotify analytics data stored in Parquet format on S3.

This script creates multiple tables with predefined S3 locations.
It includes error handling and logging for production environments.

Updated to include the new artists_catalog table with partitioning.

Usage:
    python3 create_glue_catalog.py [--database-name DATABASE] [--region REGION]
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
    containing Spotify listening data and artist catalog data.
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
        
        # Define table configurations with their S3 locations
        self.table_configs = {
            'user_tracks': {
                's3_location': 's3://itam-analytics-ragp/spotifire/processed/individual/',
                'description': 'Unified table containing Spotify listening data for all users',
                'partitioned': False
            },
            'top_tracks': {
                's3_location': 's3://itam-analytics-ragp/spotifire/processed/top_tracks/',
                'description': 'Unified table containing Spotify top tracks data for all users',
                'partitioned': False
            },
            'likes': {
                's3_location': 's3://itam-analytics-ragp/spotifire/processed/likes/',
                'description': 'Unified table containing Spotify tracks liked data for all users',
                'partitioned': False
            },
            'followed_artists': {
                's3_location': 's3://itam-analytics-ragp/spotifire/processed/followed_artists/',
                'description': 'Unified table containing Spotify followed artist data for all users',
                'partitioned': False
            },
            'artists_catalog': {
                's3_location': 's3://itam-analytics-ragp/spotifire/processed/artists_catalog/',
                'description': 'Catálogo de artistas de Spotify con información de popularidad y géneros',
                'partitioned': True
            }
        }
    
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
        
        Args:
            table_type (str): Type of table ('user_tracks', 'top_tracks', 'likes', 'followed_artists', 'artists_catalog')
        
        Returns:
            list: List of column definitions for the table
        """

        if table_type == "user_tracks":
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
        elif table_type == "top_tracks":
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
                    'Type': 'array<string>', 
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
                }
            ]
        elif table_type == "likes":
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
                    'Type': 'array<string>', 
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
                }
            ]
        elif table_type == "followed_artists":
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
                }
            ]
        elif table_type == "artists_catalog":
            return [
                {
                    'Name': 'id', 
                    'Type': 'string', 
                    'Comment': 'Unique Spotify artist ID'
                },
                {
                    'Name': 'name', 
                    'Type': 'string', 
                    'Comment': 'Artist name'
                },
                {
                    'Name': 'popularity', 
                    'Type': 'int', 
                    'Comment': 'Spotify popularity score (0-100)'
                },
                {
                    'Name': 'followers', 
                    'Type': 'bigint', 
                    'Comment': 'Number of followers'
                },
                {
                    'Name': 'genres', 
                    'Type': 'array<string>', 
                    'Comment': 'List of genres'
                },
                {
                    'Name': 'followers_tier', 
                    'Type': 'string', 
                    'Comment': 'Follower count tier (niche, emerging, established, major, mega)'
                },
                {
                    'Name': 'processed_at', 
                    'Type': 'timestamp', 
                    'Comment': 'Processing timestamp'
                },
                {
                    'Name': 'data_source', 
                    'Type': 'string', 
                    'Comment': 'Source of the data'
                }
            ]
        else:
            raise ValueError(f"Unknown table type: {table_type}")
    
    def get_partition_keys(self, table_type):
        """
        Define partition keys for tables that support partitioning.
        
        Args:
            table_type (str): Type of table
            
        Returns:
            list: List of partition key definitions, empty list if table is not partitioned
        """
        if table_type == "artists_catalog":
            return [
                {
                    'Name': 'popularity_range', 
                    'Type': 'string', 
                    'Comment': 'Popularity tier (very_low, low, medium, high, very_high)'
                },
                {
                    'Name': 'primary_genre', 
                    'Type': 'string', 
                    'Comment': 'Primary music genre category'
                }
            ]
        else:
            # Other tables are not partitioned
            return []
    
    def create_table(self, database_name, table_name):
        """
        Create a table in AWS Glue Data Catalog that points to Parquet files in S3.
        
        This creates an external table definition that tells query engines like
        Athena where to find the data and how to interpret it. The table doesn't
        store data itself - it's a metadata definition that points to S3.
        
        Args:
            database_name (str): Name of the database to contain the table
            table_name (str): Name of the table to create. One of: ['user_tracks', 'top_tracks', 'likes', 'followed_artists', 'artists_catalog']
            
        Returns:
            bool: True if successful, False otherwise
        """
        if table_name not in self.table_configs:
            logger.error(f"Unknown table name: {table_name}")
            return False
        
        table_config = self.table_configs[table_name]
        s3_location = table_config['s3_location']
        description = table_config['description']
        is_partitioned = table_config['partitioned']
        
        logger.info(f"Creating table: {database_name}.{table_name}")
        logger.info(f"S3 location: {s3_location}")
        logger.info(f"Partitioned: {is_partitioned}")
        
        # Get the schema definition
        table_schema = self.get_table_schema(table_name)
        partition_keys = self.get_partition_keys(table_name)
        
        # Build table input structure
        table_input = {
            'Name': table_name,
            'Description': description,
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
        
        # Add partition keys if the table is partitioned
        if partition_keys:
            table_input['PartitionKeys'] = partition_keys
            logger.info(f"Table will be partitioned by: {[pk['Name'] for pk in partition_keys]}")
        
        try:
            # Create the table with full configuration for Parquet files
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            
            logger.info(f"Successfully created table: {database_name}.{table_name}")
            return True
            
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Table {database_name}.{table_name} already exists")
            
            # For production, we should handle existing tables carefully
            # We'll update the table with the current schema
            return self.update_existing_table(database_name, table_name)
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"AWS Client Error creating table: {error_code} - {error_message}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error creating table: {str(e)}")
            return False
    
    def update_existing_table(self, database_name, table_name):
        """
        Update an existing table with the current schema definition.
        
        This is important for production environments where the table might
        already exist but we want to ensure it has the latest schema.
        
        Args:
            database_name (str): Name of the database containing the table
            table_name (str): Name of the table to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Updating existing table: {database_name}.{table_name}")
        
        if table_name not in self.table_configs:
            logger.error(f"Unknown table name: {table_name}")
            return False
        
        table_config = self.table_configs[table_name]
        s3_location = table_config['s3_location']
        description = table_config['description'] + " (UPDATED)"
        is_partitioned = table_config['partitioned']
        
        table_schema = self.get_table_schema(table_name)
        partition_keys = self.get_partition_keys(table_name)
        
        # Build table input structure for update
        table_input = {
            'Name': table_name,
            'Description': description,
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
        
        # Add partition keys if the table is partitioned
        if partition_keys:
            table_input['PartitionKeys'] = partition_keys
        
        try:
            self.glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input
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
        logger.info(f"Verifying table: {database_name}.{table_name}")
        
        try:
            # Verify database exists
            db_response = self.glue_client.get_database(Name=database_name)
            
            # Verify table exists and get its details
            table_response = self.glue_client.get_table(
                DatabaseName=database_name, 
                Name=table_name
            )
            
            table_info = table_response['Table']
            logger.info(f"Table verified: {table_info['Name']}")
            logger.info(f"Table location: {table_info['StorageDescriptor']['Location']}")
            logger.info(f"Number of columns: {len(table_info['StorageDescriptor']['Columns'])}")
            
            # Check if table has partitions
            if 'PartitionKeys' in table_info and table_info['PartitionKeys']:
                partition_names = [pk['Name'] for pk in table_info['PartitionKeys']]
                logger.info(f"Table partitions: {partition_names}")
            else:
                logger.info("Table is not partitioned")
            
            return True
            
        except ClientError as e:
            logger.error(f"Verification failed for {table_name}: {e.response['Error']['Message']}")
            return False
        except Exception as e:
            logger.error(f"Verification error for {table_name}: {str(e)}")
            return False
    
    def create_all_tables(self, database_name):
        """
        Create all tables defined in table_configs.
        
        Args:
            database_name (str): Name of the database to contain the tables
            
        Returns:
            dict: Results for each table creation
        """
        results = {}
        
        for table_name in self.table_configs.keys():
            logger.info(f"Processing table: {table_name}")
            
            # Create table
            table_success = self.create_table(database_name, table_name)
            
            # Verify table
            verification_success = False
            if table_success:
                verification_success = self.verify_setup(database_name, table_name)
            
            results[table_name] = {
                'created': table_success,
                'verified': verification_success,
                's3_location': self.table_configs[table_name]['s3_location'],
                'partitioned': self.table_configs[table_name]['partitioned']
            }
            
            logger.info(f"Table {table_name}: Created={table_success}, Verified={verification_success}")
        
        return results

def main():
    """
    Main execution function with command line argument parsing.
    
    This function handles the complete workflow of setting up the Glue
    Data Catalog for Spotify analytics data.
    """
    parser = argparse.ArgumentParser(
        description='Create AWS Glue Data Catalog database and tables for Spotify analytics'
    )
    parser.add_argument(
        '--database-name', 
        default='spotify_analytics',
        help='Name of the Glue database to create (default: spotify_analytics)'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    
    args = parser.parse_args()
    
    logger.info("Starting AWS Glue Data Catalog setup for Spotify analytics")
    logger.info(f"Database: {args.database_name}")
    logger.info(f"Region: {args.region}")
    
    try:
        # Initialize the catalog manager
        catalog_manager = GlueCatalogManager(region_name=args.region)
        
        logger.info(f"Tables to be created: {list(catalog_manager.table_configs.keys())}")
        for table_name, config in catalog_manager.table_configs.items():
            partition_status = " (PARTITIONED)" if config['partitioned'] else ""
            logger.info(f"  - {table_name}: {config['s3_location']}{partition_status}")
        
        # Create database
        database_success = catalog_manager.create_database(
            database_name=args.database_name,
            description=f"Database for Spotify analytics data processing and analysis"
        )
        
        if not database_success:
            logger.error("Failed to create database. Exiting.")
            sys.exit(1)
        
        # Create all tables
        table_results = catalog_manager.create_all_tables(args.database_name)
        
        # Summary
        successful_tables = [name for name, result in table_results.items() if result['created'] and result['verified']]
        failed_tables = [name for name, result in table_results.items() if not (result['created'] and result['verified'])]
        
        logger.info("="*60)
        logger.info("SETUP SUMMARY")
        logger.info("="*60)
        logger.info(f"Database: {args.database_name}")
        logger.info(f"Total tables processed: {len(table_results)}")
        logger.info(f"Successful tables: {len(successful_tables)}")
        logger.info(f"Failed tables: {len(failed_tables)}")
        
        if successful_tables:
            logger.info("\n✅ Successfully created tables:")
            for table_name in successful_tables:
                s3_location = table_results[table_name]['s3_location']
                partitioned = " (PARTITIONED)" if table_results[table_name]['partitioned'] else ""
                logger.info(f"  - {table_name}: {s3_location}{partitioned}")
        
        if failed_tables:
            logger.error("\n❌ Failed tables:")
            for table_name in failed_tables:
                logger.error(f"  - {table_name}")
        
        if successful_tables:
            logger.info(f"\n🎉 You can now query the data using AWS Athena:")
            logger.info(f"Example queries:")
            logger.info(f"  -- User listening history")
            logger.info(f"  SELECT user_id, COUNT(*) as total_plays")
            logger.info(f"  FROM {args.database_name}.user_tracks")
            logger.info(f"  GROUP BY user_id ORDER BY total_plays DESC;")
            logger.info(f"")
            logger.info(f"  -- Top tracks analysis")
            logger.info(f"  SELECT user_id, track_name, ith_preference")
            logger.info(f"  FROM {args.database_name}.top_tracks")
            logger.info(f"  WHERE ith_preference <= 5;")
            logger.info(f"")
            logger.info(f"  -- Artists by popularity and genre (NEW TABLE)")
            logger.info(f"  SELECT name, popularity, followers, primary_genre")
            logger.info(f"  FROM {args.database_name}.artists_catalog")
            logger.info(f"  WHERE popularity_range = 'very_high' AND primary_genre = 'latin'")
            logger.info(f"  ORDER BY followers DESC;")
            logger.info(f"")
            logger.info(f"  -- Genre distribution analysis")
            logger.info(f"  SELECT primary_genre, COUNT(*) as artist_count,")
            logger.info(f"         AVG(popularity) as avg_popularity")
            logger.info(f"  FROM {args.database_name}.artists_catalog")
            logger.info(f"  GROUP BY primary_genre ORDER BY artist_count DESC;")
        
        # Exit with appropriate code
        if failed_tables:
            sys.exit(1)
        else:
            logger.info("✅ All tables created successfully!")
            
    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during setup: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()