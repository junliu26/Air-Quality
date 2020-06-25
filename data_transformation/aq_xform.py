from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import boto3
import logging
from datetime import datetime
from statistics import mean


def remove_prefix(text, prefix):

    if text.startswith(prefix):
        return text[len(prefix):]

    return text


def create_logger(name, path):

    # create logger
    lgr = logging.getLogger(name)
    lgr.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create a file handler and set level to debug
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)

    # create a formatter and set the formatter for the handler.
    frmt = logging.Formatter('%(asctime)s, %(name)s, %(levelname)s, %(message)s')
    ch.setFormatter(frmt)
    fh.setFormatter(frmt)

    # add the Handler to the logger
    lgr.addHandler(ch)
    lgr.addHandler(fh)

    return lgr


def airq_xform_daily(spark, lgr, s3_path_in, s3_path_out, aq_category):

    # Definition of csv datasets
    file_in = s3_path_in + aq_category + "/*.csv"
    file_out = s3_path_out + aq_category + ".parquet"

    # User defined pySpark functions
    udf_concat = F.udf(lambda cols: "".join([x if x is not None else "*" for x in cols]), T.StringType())
    udf_mean = F.udf(mean, T.FloatType())

    # Time start
    t_start = datetime.now()

    # Raw air quality data from spark, select the columns needed for later use
    df_airq = spark.read.format("csv").option("header", "true").load(file_in) \
        .select(F.col('State Code').alias('STATE_CODE'), F.col('County Code').alias('COUNTY_CODE'),
                F.col('State Name').alias('STATE_LONG'), F.col('County Name').alias('COUNTY'),
                F.col('Site Num').alias('SITE_NUM'),
                F.col('Latitude').cast('float').alias('LATITUDE'),
                F.col('Longitude').cast('float').alias('LONGITUDE'),
                F.col('Date Local').alias('RECORD_DATE'),
                F.col('Parameter Code').alias('PCODE'), F.col('Parameter Name').alias('PNAME'),
                F.col('Arithmetic Mean').cast('float').alias('MEAN'),
                F.col('1st Max Value').cast('float').alias('MAX'))

    # Calculate FIPS code
    df_airq = df_airq.withColumn("STCOUNTYFP", udf_concat(F.array("STATE_CODE", "COUNTY_CODE")))\

    # Get record count before reduce
    df_airq_count = df_airq.count()

    # Data transfomraiton on aggregation of multiple measurement on a same parameter
    # across the county on a same day
    df_airq_xform = df_airq.groupby("PCODE", "RECORD_DATE", "STCOUNTYFP").agg(F.first("STATE_CODE").alias("STATE_CODE"),
        F.first("COUNTY_CODE").alias("COUNTY_CODE"),
        F.first("STATE_LONG").alias("STATE_LONG"),
        F.first("COUNTY").alias("COUNTY"),
        F.countDistinct("SITE_NUM").alias("SITE_COUNT"),
        udf_mean(F.collect_set("LATITUDE")).alias("LATITUDE"),
        udf_mean(F.collect_set("LONGITUDE")).alias("LONGITUDE"),
        F.avg("MEAN").alias("MEAN"),
        F.max("MAX").alias("MAX"),
        F.count("*").alias("REC_COUNT"))

    # Get record count after reduce
    df_airq_xform_count = df_airq_xform.count()

    # Save result to parquet
    df_airq_xform.write.parquet(file_out)

    # Time end
    t_end = datetime.now()
    duration = t_end - t_start
    duration_in_s = duration.total_seconds()

    message = "Transformation of " + aq_category + " completed, reduced dataset from " + str(df_airq_count) + " to " \
              + str(df_airq_xform_count) + " in " + str(duration_in_s) + " seconds."
    lgr.info(message)

    return


def main():

    # Definition of path
    path_in = "input/AQ_DATA_UNZIP/"
    path_out = "output/AQ_DATA/"
    bucket_name = "dataset-air-quality"
    s3_path_in = "s3a://" + bucket_name + "/" + path_in
    s3_path_out = "s3n://" + bucket_name + "/" + path_out # EMR does not support s3a on writing yet
    s3_in_list = []

    lgr = create_logger('aq_xform_event', 'log/aq_xform_event.log')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    spark = SparkSession \
        .builder \
        .appName("AirQualityTransformation") \
        .getOrCreate()

    for obj in bucket.objects.filter(Prefix=path_in):
        str_obj = remove_prefix(obj.key, path_in)
        category = str_obj.split('/', 1)[0]
        if category != '' and category not in s3_in_list:
            s3_in_list.append(category)

    for aq_category in s3_in_list:
        if "daily" in aq_category:
            airq_xform_daily(spark, lgr, s3_path_in, s3_path_out, aq_category)
        elif "hourly" in aq_category:
            pass

    spark.stop()

    return


if __name__ == "__main__":
    main()
