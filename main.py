from pyspark.sql import SparkSession
from config.config import configuration
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType



#setting up the environment


if __name__ == "__main__":
    spark = (SparkSession.builder.appName('smart_city')
            .config('spark.jars.packages',
                     'prg.apache.hadoop:hadoop-aws:3.3.1',
                     'com.amazonaws:aws-java-sdk:1.11.469')
            .config('spark.hadoop.fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))
            .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .getOrCreate()
            )
    

#inputting data
    text_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input'
    json_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_json'
    csv_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_csv'
    pdf_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_pdf'
    video_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_video'
    img_input_dir = 'file://C:\Users\Atindra\Desktop\Smart_city\input\input_image'

#creating data schema
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

    