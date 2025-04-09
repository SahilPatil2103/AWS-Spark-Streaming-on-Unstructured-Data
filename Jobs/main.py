from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, regexp_replace, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, ArrayType

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'config')))
from config import configuration
from udf_utils import (
    split_job_postings, extract_file_name, extract_position, extract_classcode,
    extract_start_date, extract_end_date, extract_req, extract_notes,
    extract_duties, extract_selection, extract_experience_length,
    extract_job_type, extract_education_length, extract_school_type,
    extract_application_location, extract_salary_start, extract_salary_end
)

def define_udfs():
    return {
        'split_jobs_udf': udf(split_job_postings, ArrayType(StringType())),
        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_classcode_udf': udf(extract_classcode, StringType()),
        'extract_salary_start_udf': udf(extract_salary_start, DoubleType()),
        'extract_salary_end_udf': udf(extract_salary_end, DoubleType()),
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_end_date_udf': udf(extract_end_date, DateType()),
        'extract_req_udf': udf(extract_req, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_selection_udf': udf(extract_selection, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_job_type_udf': udf(extract_job_type, StringType()),
        'extract_education_length_udf': udf(extract_education_length, StringType()),
        'extract_school_type_udf': udf(extract_school_type, StringType()),
        'extract_application_location_udf': udf(extract_application_location, StringType()),
    }

if __name__ == "__main__":
    spark = (SparkSession.builder.appName('JobPostingProcessor')
              .config('spark.jars.packages',
                     'org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469')
              .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
              .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_Key'))
              .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_Key'))
              .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
              .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                      'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
              .config("spark.security.manager.enabled", "true")  # Ensure security manager is enabled
              .config("spark.hadoop.security.authorization", "true")  # Ensure authorization is enabled
              .config("spark.hadoop.security.auth_to_local", "RULE:[1:$1]")  # Add this line for user mapping
              .config("spark.driver.memory", "2g")  # Allocate 2 GB for the driver
              .config("spark.executor.memory", "2g")  # Allocate 2 GB for each executor
              .getOrCreate())

    text_input_dir = 'file:///c:/Users/sahil/OneDrive/Desktop/aws-spark-unstructured-project/input/input_text/'
    csv_input_dir = 'file:///c:/Users/sahil/OneDrive/Desktop/aws-spark-unstructured-project/input/input_csv/'
    image_input_dir = 'file:///c:/Users/sahil/OneDrive/Desktop/aws-spark-unstructured-project/input/input_image/'
    json_input_dir = 'file:///c:/Users/sahil/OneDrive/Desktop/aws-spark-unstructured-project/input/input_json/'
    pdf_input_dir = 'file:///c:/Users/sahil/OneDrive/Desktop/aws-spark-unstructured-project/input/input_pdf/'
    video_input_dir = 'file:///c:/Users/sahil/OneDrive/Desktop/aws-spark-unstructured-project/input/input_video/'

    data_schema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('classcode', StringType(), True),
        StructField('salary_start', DoubleType(), True),
        StructField('salary_end', DoubleType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('req', StringType(), True),
        StructField('notes', StringType(), True),
        StructField('duties', StringType(), True),
        StructField('selection', StringType(), True),
        StructField('experience_length', StringType(), True),
        StructField('job_type', StringType(), True),
        StructField('education_length', StringType(), True),
        StructField('school_type', StringType(), True),
        StructField('application_location', StringType(), True),
    ])

    udfs = define_udfs()

    # Pipeline
    raw_df = (spark.readStream
              .format('text')
              .option('wholetext', 'true')
              .load(text_input_dir))

    json_df = spark.readStream.format("json").schema(data_schema).option("multiLine", "true").load(json_input_dir)

    processed_df = (raw_df
        .withColumn('file_name', regexp_replace(udfs['extract_file_name_udf']('value'), '\r', ''))
        .withColumn('job_text', explode(udfs['split_jobs_udf']('value')))
        .withColumn('position', regexp_replace(udfs['extract_position_udf']('job_text'), '\r', ''))
        .withColumn('classcode', regexp_replace(udfs['extract_classcode_udf']('job_text'), '\r', ''))
        .withColumn('salary_start', udfs['extract_salary_start_udf']('job_text'))
        .withColumn('salary_end', udfs['extract_salary_end_udf']('job_text'))
        .withColumn('start_date', udfs['extract_start_date_udf']('job_text'))
        .withColumn('end_date', udfs['extract_end_date_udf']('job_text'))
        .withColumn('req', regexp_replace(udfs['extract_req_udf']('job_text'), '\r', ''))
        .withColumn('notes', regexp_replace(udfs['extract_notes_udf']('job_text'), '\r', ''))
        .withColumn('duties', regexp_replace(udfs['extract_duties_udf']('job_text'), '\r', ''))
        .withColumn('selection', regexp_replace(udfs['extract_selection_udf']('job_text'), '\r', ''))
        .withColumn('experience_length', udfs['extract_experience_length_udf']('job_text'))
        .withColumn('job_type', regexp_replace(udfs['extract_job_type_udf']('job_text'), '\r', ''))
        .withColumn('education_length', udfs['extract_education_length_udf']('job_text'))
        .withColumn('school_type', regexp_replace(udfs['extract_school_type_udf']('job_text'), '\r', ''))
        .withColumn('application_location', regexp_replace(udfs['extract_application_location_udf']('job_text'), '\r', ''))
                    )

    processed_df = processed_df.select('file_name', 'position', 'classcode', 'salary_start', 'salary_end',
                     'start_date', 'end_date', 'req', 'notes', 'duties', 'selection',
                        'experience_length', 'job_type', 'education_length', 'school_type',
                        'application_location')

    json_df = json_df.select('file_name', 'position', 'classcode', 'salary_start', 'salary_end',
                     'start_date', 'end_date', 'req', 'notes', 'duties', 'selection',
                        'experience_length', 'job_type', 'education_length', 'school_type',
                        'application_location')

    union_dataframe = processed_df.union(json_df)

    query = (union_dataframe.writeStream
             .outputMode('append')
             .format('console')
             .option('truncate', False)
             .start())

    query.awaitTermination()
