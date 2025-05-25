import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from urllib.parse import urlparse

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_BUCKET',
    'OUTPUT_BUCKET',
    'USER_ID'  # Specific user to process, or 'ALL' for all users
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
INPUT_BUCKET = args['INPUT_BUCKET']  # e.g., 'itam-analytics-ragp'
OUTPUT_BUCKET = args['OUTPUT_BUCKET']  # e.g., 'itam-analytics-ragp'
USER_ID = args['USER_ID']  # Specific user ID or 'ALL'

# S3 paths
RAW_BASE_PATH = f"s3://{INPUT_BUCKET}/spotifire/raw/"
PROCESSED_BASE_PATH = f"s3://{OUTPUT_BUCKET}/spotifire/processed/history/"

def get_user_directories():
    """Get list of user directories in the raw data path"""
    s3_client = boto3.client('s3')
    
    if USER_ID != 'ALL':
        # Process specific user
        return [USER_ID]
    
    # List all user directories
    response = s3_client.list_objects_v2(
        Bucket=INPUT_BUCKET,
        Prefix='spotifire/raw/',
        Delimiter='/'
    )
    
    user_dirs = []
    for prefix in response.get('CommonPrefixes', []):
        # Extract user ID from path like 'spotifire/raw/12137259902/'
        user_id = prefix['Prefix'].strip('/').split('/')[-1]
        if user_id:  # Skip empty strings
            user_dirs.append(user_id)
    
    return user_dirs

def check_user_has_json_files(user_id):
    """Check if user directory contains JSON files"""
    s3_client = boto3.client('s3')
    
    response = s3_client.list_objects_v2(
        Bucket=INPUT_BUCKET,
        Prefix=f'spotifire/raw/{user_id}/',
        MaxKeys=1
    )
    
    # Check if any objects exist and at least one is a JSON
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.json'):
            return True
    return False

def define_schema_likes():
    """Define the schema for the JSON files 'likes' """
    return StructType([
        StructField("track_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("artists_id", ArrayType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("duration", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_popularity", IntegerType(), True),
        StructField("added_at", StringType(), True)
    ])

def define_schema_followed():
    """Define the schema for the JSON files 'followed artists' """
    return StructType([
        StructField("artists_id", StringType(), True)
    ])

def define_schema_top_tracks():
    """Define the schema for the JSON files 'top_tracks' """
    return StructType([
        StructField("ith_preference", IntegerType(), True),
        StructField("track_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("artists_id", ArrayType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("duration", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_popularity", IntegerType(), True)
    ])


def creates_likes_data(user_id):
    """Creates all JSON files for a specific user"""
    print(f"Processing user: {user_id}")

    # Check if user has JSON files
    if not check_user_has_json_files(user_id):
        print(f"No JSON files found for user {user_id}, skipping...")
        return
    
    # Output path for this user's parquet files
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{PROCESSED_BASE_PATH}likes/user_{user_id}_likes.parquet"
    
    try:    
        schema_l = define_schema_likes()
        df = spark.read \
            .schema(schema_l) \
            .json(f"{input_path}likes*.json")
        
        if df.count() == 0:
            print(f"No data found for user {user_id}")
            return
            
        print(f"Raw records for user {user_id}: {df.count()}")
        
        # Convert added_at to proper timestamps (UTC and Mexico timezone)
        df_cleaned = df.withColumn(
            "added_at_utc",
            to_timestamp(col("added_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ).withColumn(
            "added_at_mexico", 
            from_utc_timestamp(col("added_at_utc"), "America/Mexico_City")
        ).drop("added_at")
        
        # Handle null values and clean text fields
        df_cleaned = df_cleaned \
            .withColumn("track_name", trim(col("track_name"))) \
            .withColumn("user_id", lit(user_id)) \
            .fillna({
                "track_name": "Unknown Track",
                "artists_id": ["-1"], 
                "album_id": "-1",
                "track_popularity": 0,
                "explicit": False,
                "duration":0
            }) \
            .withColumn("processed_at", current_timestamp())
        
        # Reorder columns for better organization
        final_columns = [
            "user_id", "added_at_utc", "added_at_mexico", "track_id", "track_name", 
            "artists_id", "album_id",  "track_popularity", "explicit", "duration",
            "processed_at"
        ]
        
        df_final = df_cleaned.select(*final_columns)
        
        # Order by played_at_utc for better compression and queries
        df_final = df_final.orderBy("added_at_utc")
        
        print(f"Clean records for user {user_id}: {df_final.count()}")
        
        # Write to Parquet with compression
        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print(f"Successfully processed user {user_id} -> {output_path}")
        
        # Log data quality metrics
        total_records = df_final.count()
        duplicate_records = df_cleaned.count() - total_records
        null_tracks = df.filter(col("track_id").isNull()).count()
        
        print(f"Data Quality Report for {user_id}:")
        print(f"  - Total clean records: {total_records}")
        print(f"  - Duplicates removed: {duplicate_records}")
        print(f"  - Null track_ids found: {null_tracks}")
        print(f"  - Date range (UTC): {df_final.agg(min('added_at_utc'), max('added_at_utc')).collect()[0]}")
        print(f"  - Date range (Mexico): {df_final.agg(min('added_at_mexico'), max('added_at_mexico')).collect()[0]}")
        
    except Exception as e:
        print(f"Error processing user {user_id}: {str(e)}")
        raise

def creates_followed_data(user_id):
    """Creates all JSON files for a specific user"""
    print(f"Processing user: {user_id}")

    # Check if user has JSON files
    if not check_user_has_json_files(user_id):
        print(f"No JSON files found for user {user_id}, skipping...")
        return
    
    # Output path for this user's parquet files
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{PROCESSED_BASE_PATH}followed/user_{user_id}_followed.parquet"
    
    try:    
        schema_f = define_schema_followed()
        df = spark.read \
            .schema(schema_f) \
            .json(f"{input_path}followed*.json")
        
        if df.count() == 0:
            print(f"No data found for user {user_id}")
            return
            
        print(f"Raw records for user {user_id}: {df.count()}")
        
        # Handle null values and clean text fields
        df_cleaned = df_cleaned \
            .withColumn("user_id", lit(user_id)) \
            .withColumn("processed_at", current_timestamp())
        
        # Reorder columns for better organization
        final_columns = [
            "user_id", "artist_id", "processed_at"
        ]
        
        df_final = df_cleaned.select(*final_columns)
        
        print(f"Clean records for user {user_id}: {df_final.count()}")
        
        # Write to Parquet with compression
        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print(f"Successfully processed user {user_id} -> {output_path}")
        
        # Log data quality metrics
        total_records = df_final.count()
        
        print(f"Data Quality Report for {user_id}:")
        print(f"  - Total clean records: {total_records}")
        
    except Exception as e:
        print(f"Error processing user {user_id}: {str(e)}")
        raise

def creates_top_tracks_data(user_id):
    """Creates all parquet files for a specific user"""
    print(f"Processing user: {user_id}")

    # Check if user has JSON files
    if not check_user_has_json_files(user_id):
        print(f"No JSON files found for user {user_id}, skipping...")
        return
    
    # Output path for this user's parquet files
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{PROCESSED_BASE_PATH}top_tracks/user_{user_id}_top_tracks.parquet"
    
    try:    
        schema_t = define_schema_top_tracks()
        df = spark.read \
            .schema(schema_t) \
            .json(f"{input_path}top_*.json")
        
        if df.count() == 0:
            print(f"No data found for user {user_id}")
            return
            
        print(f"Raw records for user {user_id}: {df.count()}")
        
        
        # Handle null values and clean text fields
        df_cleaned = df \
            .withColumn("user_id", lit(user_id)) \
            .withColumn("track_name", trim(col("track_name"))) \
            .fillna({
                "track_name": "Unknown Track",
                "artists_id": ["-1"], 
                "album_id": "-1",
                "track_popularity": 0,
                "explicit": False,
                "duration":0
            }) \
            .withColumn("processed_at", current_timestamp())
        
        # Reorder columns for better organization
        final_columns = [
            "user_id", "ith_preference", "track_id", "track_name", 
            "artists_id", "album_id",  "track_popularity", "explicit", "duration",
            "processed_at"
        ]
        
        df_final = df_cleaned.select(*final_columns)
        
        # Order by played_at_utc for better compression and queries
        df_final = df_final.orderBy("ith_preference")
        
        print(f"Clean records for user {user_id}: {df_final.count()}")
        
        # Write to Parquet with compression
        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print(f"Successfully processed user {user_id} -> {output_path}")
        
        # Log data quality metrics
        total_records = df_final.count()
        duplicate_records = df_cleaned.count() - total_records
        null_tracks = df.filter(col("track_id").isNull()).count()
        
        print(f"Data Quality Report for {user_id}:")
        print(f"  - Total clean records: {total_records}")
        print(f"  - Duplicates removed: {duplicate_records}")
        print(f"  - Null track_ids found: {null_tracks}")
        
    except Exception as e:
        print(f"Error processing user {user_id}: {str(e)}")
        raise


def main():
    """Main execution function"""
    print("Starting Spotify history...")
    print(f"Input bucket: {INPUT_BUCKET}")
    print(f"Output bucket: {OUTPUT_BUCKET}")
    print(f"Target user: {USER_ID}")
    
    # Get list of users to process
    user_ids = get_user_directories()
    print(f"Found {len(user_ids)} users to process: {user_ids}")
    
    # Process each user
    processed_users = 0
    failed_users = []
    
    for user_id in user_ids:
        try:
            creates_likes_data(user_id)
            creates_followed_data(user_id)
            creates_top_tracks_data(user_id)
            processed_users += 1
        except Exception as e:
            print(f"Failed to process user {user_id}: {str(e)}")
            failed_users.append(user_id)
    
    # Final summary
    print("\n" + "="*50)
    print("JOB SUMMARY")
    print("="*50)
    print(f"Total users processed: {processed_users}/{len(user_ids)}")
    print(f"Successful: {processed_users}")
    print(f"Failed: {len(failed_users)}")
    if failed_users:
        print(f"Failed users: {failed_users}")
    print("="*50)

# Run the job
if __name__ == "__main__":
    main()
    job.commit()
