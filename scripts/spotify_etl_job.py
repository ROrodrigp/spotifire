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
PROCESSED_BASE_PATH = f"s3://{OUTPUT_BUCKET}/spotifire/processed/individual/"

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

def check_user_has_csv_files(user_id):
    """Check if user directory contains CSV files"""
    s3_client = boto3.client('s3')
    
    response = s3_client.list_objects_v2(
        Bucket=INPUT_BUCKET,
        Prefix=f'spotifire/raw/{user_id}/',
        MaxKeys=1
    )
    
    # Check if any objects exist and at least one is a CSV
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            return True
    return False

def define_schema():
    """Define the schema for the CSV files"""
    return StructType([
        StructField("played_at", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("album_name", StringType(), True),
        StructField("track_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("explicit", BooleanType(), True)
    ])

def process_user_data(user_id):
    """Process all CSV files for a specific user"""
    print(f"Processing user: {user_id}")
    
    # Check if user has CSV files
    if not check_user_has_csv_files(user_id):
        print(f"No CSV files found for user {user_id}, skipping...")
        return
    
    # Input path for this user's CSV files
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{PROCESSED_BASE_PATH}user_{user_id}.parquet"
    
    try:
        # Read all CSV files for this user
        schema = define_schema()
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(f"{input_path}*.csv")
        
        if df.count() == 0:
            print(f"No data found for user {user_id}")
            return
            
        print(f"Raw records for user {user_id}: {df.count()}")
        
        # Data cleaning and transformations
        df_cleaned = df \
            .filter(col("played_at").isNotNull()) \
            .filter(col("track_id").isNotNull()) \
            .filter(col("artist_id").isNotNull())
        
        # Convert played_at to proper timestamps (UTC and Mexico timezone)
        df_cleaned = df_cleaned.withColumn(
            "played_at_utc",
            to_timestamp(col("played_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ).withColumn(
            "played_at_mexico", 
            from_utc_timestamp(col("played_at_utc"), "America/Mexico_City")
        ).drop("played_at")
        
        # Handle null values and clean text fields
        df_cleaned = df_cleaned \
            .withColumn("track_name", trim(col("track_name"))) \
            .withColumn("artist_name", trim(col("artist_name"))) \
            .withColumn("album_name", trim(col("album_name"))) \
            .fillna({
                "track_name": "Unknown Track",
                "artist_name": "Unknown Artist", 
                "album_name": "Unknown Album",
                "duration_ms": 0,
                "popularity": 0,
                "explicit": False
            })
        
        # Remove duplicates based on track_id and played_at_utc
        df_deduplicated = df_cleaned.dropDuplicates(["track_id", "played_at_utc"])
        
        # Add derived fields based on Mexico timezone for behavioral analysis
        df_transformed = df_deduplicated \
            .withColumn("user_id", lit(user_id)) \
            .withColumn("play_hour", hour(col("played_at_mexico"))) \
            .withColumn("play_day_of_week", dayofweek(col("played_at_mexico"))) \
            .withColumn("play_month", month(col("played_at_mexico"))) \
            .withColumn("play_year", year(col("played_at_mexico"))) \
            .withColumn("duration_minutes", round(col("duration_ms") / 60000.0, 2)) \
            .withColumn("processed_at", current_timestamp())
        
        # Add season based on month (Mexico timezone)
        df_transformed = df_transformed.withColumn(
            "season",
            when((col("play_month").between(3, 5)), "Spring")
            .when((col("play_month").between(6, 8)), "Summer")
            .when((col("play_month").between(9, 11)), "Fall")
            .otherwise("Winter")
        )
        
        # Reorder columns for better organization
        final_columns = [
            "user_id", "played_at_utc", "played_at_mexico", "track_id", "track_name", 
            "artist_id", "artist_name", "album_id", "album_name",
            "duration_ms", "duration_minutes", "popularity", "explicit",
            "play_hour", "play_day_of_week", "play_month", "play_year", "season",
            "processed_at"
        ]
        
        df_final = df_transformed.select(*final_columns)
        
        # Order by played_at_utc for better compression and queries
        df_final = df_final.orderBy("played_at_utc")
        
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
        print(f"  - Date range (UTC): {df_final.agg(min('played_at_utc'), max('played_at_utc')).collect()[0]}")
        print(f"  - Date range (Mexico): {df_final.agg(min('played_at_mexico'), max('played_at_mexico')).collect()[0]}")
        
    except Exception as e:
        print(f"Error processing user {user_id}: {str(e)}")
        raise

def main():
    """Main execution function"""
    print("Starting Spotify data transformation job...")
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
            process_user_data(user_id)
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