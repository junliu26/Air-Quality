import boto3
import logging

from io import BytesIO
from zipfile import ZipFile


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


def main():

    path_in = 'input/AQ_DATA/'
    path_out = 'input/AQ_DATA_UNZIP/'
    bucket_name = 'dataset-air-quality'

    aq_category = {
        'daily_44201': 'o3_daily_summary',
        'daily_42401': 'so2_daily_summary',
        'daily_42101': 'co_daily_summary',
        'daily_42602': 'no2_daily_summary',
        'daily_88101': 'pm25_frm_daily_summary',
        'daily_88502': 'pm25_nonfrm_daily_summary',
        'daily_81102': 'pm10_mass_daily_summary',
        'daily_SPEC': 'pm25_speciation_daily_summary',
        'daily_PM10SPEC': 'pm10_speciation_daily_summary',
        'daily_TEMP': 'wind_daily_summary',
        'daily_WIND': 'temperature_daily_summary',
        'daily_PRESS': 'pressure_daily_summary',
        'daily_RH_DP': 'rh_and_dp_daily_summary',
        'daily_HAPS': 'haps_daily_summary',
        'daily_VOCS': 'voc_daily_summary',
        'daily_NONOxNOy': 'nonoxnoy_daily_summary',
        'daily_LEAD': 'lead_daily_summary',
        'hourly_44201': 'o3_hourly_summary',
        'hourly_42401': 'so2_hourly_summary',
        'hourly_42101': 'co_hourly_summary',
        'hourly_42602': 'no2_hourly_summary',
        'hourly_88101': 'pm25_frm_hourly_summary',
        'hourly_88502': 'pm25_nonfrm_hourly_summary',
        'hourly_81102': 'pm10_mass_hourly_summary',
        'hourly_SPEC': 'pm25_speciation_hourly_summary',
        'hourly_PM10SPEC': 'pm10_speciation_hourly_summary',
        'hourly_TEMP': 'wind_hourly_summary',
        'hourly_WIND': 'temperature_hourly_summary',
        'hourly_PRESS': 'pressure_hourly_summary',
        'hourly_RH_DP': 'rh_and_dp_hourly_summary',
        'hourly_HAPS': 'haps_hourly_summary',
        'hourly_VOCS': 'voc_hourly_summary',
        'hourly_NONOxNOy': 'nonoxnoy_hourly_summary',
        'hourly_LEAD': 'lead_hourly_summary'
    }

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    lgr = create_logger('unzip_event', 'log/unzip_event.log')

    for prefix, category in aq_category.items():

        s3_in_list = []

        for obj in bucket.objects.filter(Prefix = path_in + prefix):
            s3_in_list.append(remove_prefix(obj.key, path_in))

        for file in s3_in_list:

            obj_zip = bucket.Object(path_in+file)
            content_zip = obj_zip.get()['Body'].read()
            zipfile = ZipFile(BytesIO(content_zip))

            # what file already in the folder in the s3 bucket
            s3_out_list = []
            path_out_category = path_out + category + '/'
            for obj in bucket.objects.filter(Prefix=path_out_category):
                if obj.key != path_out_category:
                    s3_out_list.append(remove_prefix(obj.key, path_out_category))

            for subfile in zipfile.namelist():
                if subfile not in s3_out_list:
                    content_unzip = zipfile.read(subfile)
                    obj = s3.Object(bucket_name, path_out_category + subfile)
                    obj.put(Body=content_unzip)
                    message = "Unzip of file " + file + " to " + path_out + category + '/' + subfile + " completed"
                else:
                    message = path_out + category + '/' + subfile + " existed"
                lgr.info(message)

    return


if __name__ == "__main__":
    main()
