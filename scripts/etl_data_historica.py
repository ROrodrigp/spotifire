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

# Updated S3 output paths to match table configurations
OUTPUT_PATHS = {
    'likes': f"s3://{OUTPUT_BUCKET}/spotifire/processed/likes/",
    'followed_artists': f"s3://{OUTPUT_BUCKET}/spotifire/processed/followed_artists/",
    'top_tracks': f"s3://{OUTPUT_BUCKET}/spotifire/processed/top_tracks/"
}

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

def check_specific_json_files(user_id):
    """Check which specific JSON files exist for the user"""
    s3_client = boto3.client('s3')
    
    response = s3_client.list_objects_v2(
        Bucket=INPUT_BUCKET,
        Prefix=f'spotifire/raw/{user_id}/'
    )
    
    files = {
        'likes': [],
        'followed': [],
        'top_tracks': []
    }
    
    for obj in response.get('Contents', []):
        key = obj['Key']
        filename = key.split('/')[-1]
        
        if filename.endswith('.json'):
            if 'likes' in filename:
                files['likes'].append(filename)
            elif 'followed' in filename:
                files['followed'].append(filename)
            elif 'top_' in filename:
                files['top_tracks'].append(filename)
    
    return files

def define_schema_likes():
    """Define the schema for the JSON files 'likes' """
    return StructType([
        StructField("track_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("artists_id", ArrayType(StringType()), True),
        StructField("explicit", BooleanType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_popularity", IntegerType(), True),
        StructField("added_at", StringType(), True)
    ])

def define_schema_followed():
    """Define the schema for the JSON files 'followed artists' """
    return StructType([
        StructField("artists_ids", ArrayType(StringType()), True)  # Fixed: artists_ids (with 's')
    ])

def define_schema_top_tracks():
    """Define the schema for the JSON files 'top_tracks' """
    return StructType([
        StructField("ith_preference", IntegerType(), True),
        StructField("track_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("artists_id", ArrayType(StringType()), True),
        StructField("explicit", BooleanType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_popularity", IntegerType(), True)
    ])


def creates_likes_data(user_id):
    """Creates likes data for a specific user"""
    print(f"Processing likes data for user: {user_id}")

    # Check which specific files exist
    files = check_specific_json_files(user_id)
    likes_files = files['likes']
    
    if not likes_files:
        print(f"No likes JSON files found for user {user_id}, skipping...")
        return
    
    print(f"Found likes files for user {user_id}: {likes_files}")
    
    # Input and output paths
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{OUTPUT_PATHS['likes']}user_{user_id}.parquet"
    
    try:    
        schema_l = define_schema_likes()
        
        # Process each likes file explicitly to track what we're reading
        all_dfs = []
        total_records_per_file = {}
        
        for likes_file in likes_files:
            file_path = f"{input_path}{likes_file}"
            print(f"Reading file: {file_path}")
            
            df_file = spark.read \
                .option("multiline", "true") \
                .schema(schema_l) \
                .json(file_path)
            
            file_count = df_file.count()
            total_records_per_file[likes_file] = file_count
            print(f"  - Records in {likes_file}: {file_count}")
            
            if file_count > 0:
                # Add source file info for tracking
                df_file = df_file.withColumn("source_file", lit(likes_file))
                all_dfs.append(df_file)
        
        if not all_dfs:
            print(f"No valid likes data found for user {user_id}")
            return
        
        # Combine all DataFrames
        if len(all_dfs) == 1:
            df = all_dfs[0]
        else:
            df = all_dfs[0]
            for df_additional in all_dfs[1:]:
                df = df.union(df_additional)
        
        print(f"Total combined likes records for user {user_id}: {df.count()}")
        print(f"Records breakdown: {total_records_per_file}")
        
        # Convert added_at to proper timestamps (UTC and Mexico timezone)
        df_cleaned = df.selectExpr("*", "timestamp(added_at) as added_at_utc").drop("added_at")
        df_cleaned = df_cleaned.selectExpr("*","from_utc_timestamp(added_at_utc, 'America/Mexico_City') as added_at_mexico")
        
        # Handle null values and clean text fields
        df_cleaned = df_cleaned \
            .withColumn("track_name", trim(col("track_name"))) \
            .withColumn("user_id", lit(user_id)) \
            .fillna({
                "track_name": "Unknown Track",
                "album_id": "-1",
                "track_popularity": 0,
                "explicit": False,
                "duration_ms": 0
            }) \
            .withColumn("processed_at", current_timestamp())
        
        # Handle null arrays
        df_cleaned = df_cleaned.withColumn(
            "artists_id",
            when(col("artists_id").isNull(), array(lit("-1"))).otherwise(col("artists_id"))
        )
        
        # Remove duplicates based on track_id and added_at_utc to handle potential duplicates from multiple files
        df_deduplicated = df_cleaned.dropDuplicates(["track_id", "added_at_utc"])
        
        duplicates_removed = df_cleaned.count() - df_deduplicated.count()
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate records")
        
        # Reorder columns for better organization (remove source_file from final output)
        final_columns = [
            "user_id", "added_at_utc", "added_at_mexico", "track_id", "track_name", 
            "artists_id", "album_id", "track_popularity", "explicit", "duration_ms",
            "processed_at"
        ]
        
        df_final = df_deduplicated.select(*final_columns)
        
        # Order by added_at_utc for better compression and queries
        df_final = df_final.orderBy("added_at_utc")
        
        print(f"Final clean likes records for user {user_id}: {df_final.count()}")
        
        # Write to Parquet with compression
        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print(f"Successfully processed likes for user {user_id} -> {output_path}")
        
        # Log data quality metrics
        total_records = df_final.count()
        null_tracks = df.filter(col("track_id").isNull()).count()
        
        print(f"Likes Data Quality Report for {user_id}:")
        print(f"  - Total clean records: {total_records}")
        print(f"  - Duplicates removed: {duplicates_removed}")
        print(f"  - Null track_ids found: {null_tracks}")
        print(f"  - Source files processed: {list(total_records_per_file.keys())}")
        if total_records > 0:
            date_range = df_final.agg(min('added_at_utc'), max('added_at_utc')).collect()[0]
            print(f"  - Date range (UTC): {date_range}")
        
    except Exception as e:
        print(f"Error processing likes for user {user_id}: {str(e)}")
        raise

def creates_followed_data(user_id):
    """Creates followed artists data for a specific user"""
    print(f"Processing followed artists data for user: {user_id}")

    # Check which specific files exist
    files = check_specific_json_files(user_id)
    followed_files = files['followed']
    
    if not followed_files:
        print(f"No followed artists JSON files found for user {user_id}, skipping...")
        return
    
    print(f"Found followed artists files for user {user_id}: {followed_files}")
    
    # Input and output paths
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{OUTPUT_PATHS['followed_artists']}user_{user_id}.parquet"
    
    try:    
        schema_f = define_schema_followed()
        
        # Process each followed file explicitly
        all_dfs = []
        total_records_per_file = {}
        
        for followed_file in followed_files:
            file_path = f"{input_path}{followed_file}"
            print(f"Reading file: {file_path}")
            
            df_file = spark.read \
                .option("multiline", "true") \
                .schema(schema_f) \
                .json(file_path)
            
            df_file = df_file.selectExpr("explode(artists_ids) as artist_id")

            file_count = df_file.count()
            total_records_per_file[followed_file] = file_count
            print(f"  - Records in {followed_file}: {file_count}")
            
            if file_count > 0:
                df_file = df_file.withColumn("source_file", lit(followed_file))
                all_dfs.append(df_file)
        
        if not all_dfs:
            print(f"No valid followed artists data found for user {user_id}")
            return
        
        # Combine all DataFrames
        if len(all_dfs) == 1:
            df = all_dfs[0]
        else:
            df = all_dfs[0]
            for df_additional in all_dfs[1:]:
                df = df.union(df_additional)
        
        print(f"Total combined followed artists records for user {user_id}: {df.count()}")
        print(f"Records breakdown: {total_records_per_file}")
        
        # Handle null values and add metadata
        df_cleaned = df \
            .withColumn("user_id", lit(user_id)) \
            .fillna({
                "artist_id": "-1"
            }) \
            .withColumn("processed_at", current_timestamp())
        
        # Remove duplicates based on artist_id
        df_deduplicated = df_cleaned.dropDuplicates(["artist_id"])
        
        duplicates_removed = df_cleaned.count() - df_deduplicated.count()
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate followed artists")
        
        # Reorder columns for better organization
        final_columns = [
            "user_id", "artist_id", "processed_at"
        ]
        
        df_final = df_deduplicated.select(*final_columns)
        
        print(f"Final clean followed artists records for user {user_id}: {df_final.count()}")
        
        # Write to Parquet with compression
        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print(f"Successfully processed followed artists for user {user_id} -> {output_path}")
        
        # Log data quality metrics
        total_records = df_final.count()
        
        print(f"Followed Artists Data Quality Report for {user_id}:")
        print(f"  - Total clean records: {total_records}")
        print(f"  - Duplicates removed: {duplicates_removed}")
        print(f"  - Source files processed: {list(total_records_per_file.keys())}")
        
    except Exception as e:
        print(f"Error processing followed artists for user {user_id}: {str(e)}")
        raise

def creates_top_tracks_data(user_id):
    """Creates top tracks data for a specific user"""
    print(f"Processing top tracks data for user: {user_id}")

    # Check which specific files exist
    files = check_specific_json_files(user_id)
    top_tracks_files = files['top_tracks']
    
    if not top_tracks_files:
        print(f"No top tracks JSON files found for user {user_id}, skipping...")
        return
    
    print(f"Found top tracks files for user {user_id}: {top_tracks_files}")
    
    # Input and output paths
    input_path = f"{RAW_BASE_PATH}{user_id}/"
    output_path = f"{OUTPUT_PATHS['top_tracks']}user_{user_id}.parquet"
    
    try:    
        schema_t = define_schema_top_tracks()
        
        # Process each top tracks file explicitly
        all_dfs = []
        total_records_per_file = {}
        
        for top_tracks_file in top_tracks_files:
            file_path = f"{input_path}{top_tracks_file}"
            print(f"Reading file: {file_path}")
            
            df_file = spark.read \
                .option("multiline", "true") \
                .schema(schema_t) \
                .json(file_path)
            
            file_count = df_file.count()
            total_records_per_file[top_tracks_file] = file_count
            print(f"  - Records in {top_tracks_file}: {file_count}")
            
            if file_count > 0:
                df_file = df_file.withColumn("source_file", lit(top_tracks_file))
                all_dfs.append(df_file)
        
        if not all_dfs:
            print(f"No valid top tracks data found for user {user_id}")
            return
        
        # Combine all DataFrames
        if len(all_dfs) == 1:
            df = all_dfs[0]
        else:
            df = all_dfs[0]
            for df_additional in all_dfs[1:]:
                df = df.union(df_additional)
        
        print(f"Total combined top tracks records for user {user_id}: {df.count()}")
        print(f"Records breakdown: {total_records_per_file}")
        
        # Handle null values and clean text fields
        df_cleaned = df \
            .withColumn("user_id", lit(user_id)) \
            .withColumn("track_name", trim(col("track_name"))) \
            .fillna({
                "track_name": "Unknown Track",
                "album_id": "-1",
                "track_popularity": 0,
                "explicit": False,
                "duration_ms": 0
            }) \
            .withColumn("processed_at", current_timestamp())
        
        # Handle null arrays
        df_cleaned = df_cleaned.withColumn(
            "artists_id",
            when(col("artists_id").isNull(), array(lit("-1"))).otherwise(col("artists_id"))
        )
        
        # Remove duplicates based on track_id and ith_preference
        df_deduplicated = df_cleaned.dropDuplicates(["track_id", "ith_preference"])
        
        duplicates_removed = df_cleaned.count() - df_deduplicated.count()
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate top tracks")
        
        # Reorder columns for better organization
        final_columns = [
            "user_id", "ith_preference", "track_id", "track_name", 
            "artists_id", "album_id", "track_popularity", "explicit", "duration_ms",
            "processed_at"
        ]
        
        df_final = df_deduplicated.select(*final_columns)
        
        # Order by ith_preference for better compression and queries
        df_final = df_final.orderBy("ith_preference")
        
        print(f"Final clean top tracks records for user {user_id}: {df_final.count()}")
        
        # Write to Parquet with compression
        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print(f"Successfully processed top tracks for user {user_id} -> {output_path}")
        
        # Log data quality metrics
        total_records = df_final.count()
        null_tracks = df.filter(col("track_id").isNull()).count()
        
        print(f"Top Tracks Data Quality Report for {user_id}:")
        print(f"  - Total clean records: {total_records}")
        print(f"  - Duplicates removed: {duplicates_removed}")
        print(f"  - Null track_ids found: {null_tracks}")
        print(f"  - Source files processed: {list(total_records_per_file.keys())}")
        
    except Exception as e:
        print(f"Error processing top tracks for user {user_id}: {str(e)}")
        raise


def main():
    """Main execution function"""
    print("Starting Spotify JSON data processing...")
    print(f"Input bucket: {INPUT_BUCKET}")
    print(f"Output bucket: {OUTPUT_BUCKET}")
    print(f"Target user: {USER_ID}")
    print("Output paths:")
    for data_type, path in OUTPUT_PATHS.items():
        print(f"  - {data_type}: {path}")
    
    # Get list of users to process
    user_ids = get_user_directories()
    print(f"Found {len(user_ids)} users to process: {user_ids}")
    
    # Process each user
    processed_users = 0
    failed_users = []
    
    for user_id in user_ids:
        try:
            print(f"\n{'='*60}")
            print(f"Processing user: {user_id}")
            print(f"{'='*60}")
            
            # Check what files exist for this user first
            files = check_specific_json_files(user_id)
            print(f"Available files for user {user_id}:")
            for file_type, file_list in files.items():
                print(f"  - {file_type}: {file_list}")
            
            creates_likes_data(user_id)
            creates_followed_data(user_id)
            creates_top_tracks_data(user_id)
            processed_users += 1
            
            print(f"✅ Successfully completed processing for user {user_id}")
            
        except Exception as e:
            print(f"❌ Failed to process user {user_id}: {str(e)}")
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
    
    print("\nOutput locations:")
    for data_type, path in OUTPUT_PATHS.items():
        print(f"  - {data_type}: {path}")
    
    print("\nData is now ready for Athena queries!")
    print("="*50)

# Run the job
if __name__ == "__main__":
    main()
    job.commit()