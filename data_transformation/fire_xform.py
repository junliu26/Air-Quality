from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import boto3
import logging
import requests
import datetime


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


def string_to_county(input):
    if input == None:
        county = "NA"
    else:
        wordlist = input.split(' ', 3)
        if len(wordlist) <= 1:
            county = wordlist[0]
        elif wordlist[1] in ["County", "Parish", "City", "Municipio"]:
            county = wordlist[0]
        else:
            county = wordlist[0] + ' ' + wordlist[1]
    return county


def get_county(x, y):
    if x == None or y == None:
        county = "NA"
    else:
        try:
            # Get data from fcc census API
            response = requests.get(
                "https://geo.fcc.gov/api/census/block/find?latitude={}&longitude={}&showall=false&format=json"
                .format(x, y))
            dict_response = response.json()
            county = dict_response['County']['name']
        except:
            county = 'ERROR'
    return county


def to_date(year, days):
    if year == None or days == None:
        date = "NA"
    else:
        temp = datetime.datetime(year, 1, 1) + datetime.timedelta(days - 1)
        date = temp.date().isoformat()
    return date


def fire_data_cleansing(spark, lgr, s3_path_file_fire, s3_path_file_FIPS, s3_path_file_OUT):

    # User defined pySpark functions
    udf_to_date = F.udf(lambda x, y: to_date(x, y))
    udf_county = F.udf(lambda x, y: get_county(x,y))
    udf_stc = F.udf(lambda x: string_to_county(x))

    # Time start
    t_start = datetime.datetime.now()

    # FIPS dataframe to join with wild fire data frame in order to get FIPS
    df_FIPS = spark.read.format("csv").option("header", "true").load(s3_path_file_FIPS)\
    .select(F.col('STATE'),
    udf_stc(F.col('COUNTYNAME')).alias('COUNTY'),
    F.col('COUNTYNAME'),F.col('STCOUNTYFP')).distinct()

    # Raw fire data from spark, select the columns and calculate fire start date needed for later use
    df_fire_raw = spark.read.format("csv").option("header", "true").load(s3_path_file_fire)\
    .select(F.col('FOD_ID'),F.col('FIRE_NAME'),F.col('FIRE_YEAR'),F.col('DISCOVERY_DOY'),
    udf_to_date(F.col('FIRE_YEAR').cast('int'), F.col('DISCOVERY_DOY').cast('int')).alias('START_DATE'),
    F.col('STAT_CAUSE_DESCR'),F.col('FIRE_SIZE'),F.col('FIRE_SIZE_CLASS'),
    F.col('LATITUDE'), F.col('LONGITUDE'),F.col('STATE'),
    F.col('FIPS_NAME').alias('COUNTY'))

    # Only select fires with class above 'C'
    df_fire_raw = df_fire_raw.filter('FIRE_SIZE_CLASS NOT IN (\'A\', \'B\', \'C\')')
    #df_fire_raw = df_fire_raw.filter('FIRE_SIZE_CLASS NOT IN (\'A\', \'B\', \'C\', \'D\', \'E\', \'F\')')
    df_fire_raw_count = df_fire_raw.count()

    # Split the dataset into two, one with County name, one without
    df_fire_1 = df_fire_raw.filter('COUNTY IS NOT NULL')
    df_fire_1_count = df_fire_1.count()

    df_fire_NULL = df_fire_raw.filter('COUNTY IS NULL')
    df_fire_NULL_count = df_fire_NULL.count()

    message = "Total number of record with valid county is " + str(df_fire_1_count) + " in " + str(df_fire_raw_count)\
              + " total records."
    lgr.info(message)

    df_fire_2 = df_fire_NULL.select(F.col('FOD_ID'), F.col('FIRE_NAME'), F.col('FIRE_YEAR'), F.col('DISCOVERY_DOY'),
    F.col('START_DATE'), F.col('STAT_CAUSE_DESCR'), F.col('FIRE_SIZE'), F.col('FIRE_SIZE_CLASS'),
    F.col('LATITUDE'), F.col('LONGITUDE'),F.col('STATE'),
    udf_county(F.col('LATITUDE').cast('float'), F.col('LONGITUDE').cast('float')).alias('COUNTY'))

    df_fire_2 = df_fire_2.filter('COUNTY <> \'NA\'')
    df_fire_2_count = df_fire_2.count()

    message = "Fetched county information for " + str(df_fire_2_count) + " out of " \
              + str(df_fire_NULL_count) + " missing records."
    lgr.info(message)

    df_fire_combined = df_fire_1.union(df_fire_2)

    df_fire = df_fire_combined.select(F.col('FOD_ID'), F.col('FIRE_NAME'), F.col('FIRE_YEAR'), F.col('DISCOVERY_DOY'),
    F.col('START_DATE'), F.col('STAT_CAUSE_DESCR'), F.col('FIRE_SIZE'), F.col('FIRE_SIZE_CLASS'),
    F.col('LATITUDE'), F.col('LONGITUDE'),F.col('STATE'),
    udf_stc(F.col('COUNTY')).alias('COUNTY'))

    # Join df_fire with FIPS table, and rearrange column sequence
    df_fire_FIPS = df_fire.join(df_FIPS, on=['STATE','COUNTY'], how="inner").drop('COUNTY')
    cols = list(df_fire_FIPS.columns)
    cols = cols[1:] + [cols[0]]
    df_fire_FIPS = df_fire_FIPS[cols]

    # Save result to parquet
    df_fire_FIPS.write.parquet(s3_path_file_OUT)

    # Time end
    t_end = datetime.datetime.now()
    duration = t_end - t_start
    duration_in_s = duration.total_seconds()

    message = "Data cleansing completed in " + str(duration_in_s) + " seconds."
    lgr.info(message)

    return


def main():

    # Definition of path
    path_in = "input/WildFire/"
    path_out = "output/WildFire/"
    bucket_name = "dataset-air-quality"
    s3_path_in = "s3a://" + bucket_name + "/" + path_in
    s3_path_out = "s3n://" + bucket_name + "/" + path_out # EMR does not support s3a on writing yet

    file_fire = "wildfire_data.csv"
    file_FIPS = "ZIP-COUNTY-FIPS_2017-06.csv"
    file_OUT = "fire_cleansed.parquet"

    # Definition of csv datasets
    s3_path_file_fire = s3_path_in + file_fire
    s3_path_file_FIPS = s3_path_in + file_FIPS
    s3_path_file_OUT = s3_path_out + file_OUT

    lgr = create_logger('fire_xform_event', 'log/fire_xform_event.log')

    spark = SparkSession \
        .builder \
        .appName("FireTransformation") \
        .getOrCreate()

    fire_data_cleansing(spark, lgr, s3_path_file_fire, s3_path_file_FIPS, s3_path_file_OUT)

    spark.stop()

    return


if __name__ == "__main__":
    main()
