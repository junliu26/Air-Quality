import boto3
import logging
import requests
import csv


def remove_prefix(text, prefix):

    if text.startswith(prefix):
        return text[len(prefix):]

    return text


def create_logger(name, path):

    # create logger that can both show on console and save to log file
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

    s3 = boto3.resource('s3')

    lgr = create_logger('save_aq_event', 'log/save_aq_event.log')

    listurl = 'https://aqs.epa.gov/aqsweb/airdata/file_list.csv'
    baseurl = 'https://aqs.epa.gov/aqsweb/airdata/'

    filelist = requests.get(listurl)
    decoded_content = filelist.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')

    # get file list and remove header
    my_list = list(cr)
    file_list = [i[0] for i in my_list]
    file_list = file_list [1:]

    # what file already in the folder in the s3 bucket
    s3_list = []
    prefix = 'input/AQ_DATA/'
    bucket_name = 'dataset-air-quality'
    bucket = s3.Bucket(bucket_name)
    for object in bucket.objects.filter(Prefix = prefix):
        if object.key != prefix:
            s3_list.append(remove_prefix(object.key, prefix))

    # filter out the files that have not been downloaded
    file_list = list(filter(lambda i: i not in s3_list, file_list))

    for file in file_list:
        myfile = requests.get(baseurl+file)
        content = myfile.content
        object = s3.Object(bucket_name, prefix + file)
        object.put(Body=content)
        lgr.info("Download of file " + file + " to s3 completed")

    return


if __name__ == "__main__":
    main()
