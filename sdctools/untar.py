import tarfile
import tempfile
import csv
import boto3
import datetime
import random
import botocore.exceptions

from dateutil import parser


# entry point - Lambda should call this method
def unbundle_pon(src_bucket_name, pon_key, dest_bucket_name, dest_prefix):
    # determine compression type as this impacts level of nesting in file
    file_ext = pon_key.split('.')[-1]

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(src_bucket_name)

    # single .TAR file can just be downloaded and processed
    if file_ext.upper() == 'TAR':
        temp_s3 = tempfile.SpooledTemporaryFile()
        bucket.download_fileobj(pon_key, temp_s3)
        temp_s3.seek(0)
        temp_tar = tarfile.open(name=None, mode='r', fileobj=temp_s3)
        __untar_pon(temp_tar, dest_bucket_name, dest_prefix)

    elif file_ext.upper() == 'GZ':
        __extract_tars(pon_key)
    else:
        raise ValueError('Invalid PON File Type')

    return file_ext


# returns a filename for extracted PON data that does not already exist in S3
#   file_type is the PON file type
#                       e.g. 'ponOltUtilTxOntHistory'
#   bucket_name is the bucket extracted PON data is sent to
#   prefix (optional) is the S3 folder in bucket_name (include the last / here)
#                       e.g. 'sdc_pom_extracted/'
def get_filename(file_type, bucket_name, prefix=''):

    # build string and check if key already exists
    s3 = boto3.resource('s3')

    while True:
        new_name = file_type \
                   + '_' \
                   + datetime.datetime.now().strftime('%Y%m%d-%H%M') \
                   + '_' \
                   + str(random.randint(1, 999999)) \
                   + '.csv'
        try:
            s3.Object(bucket_name, prefix + new_name).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                break
            else:
                # Something else has gone wrong.
                raise
        else:
            # already exists
            new_name = new_name

    return prefix + new_name


# basic wrapper to upload to S3 and capture common errors
def upload_to_s3(bucket_name, key_name, file_contents):

    s3 = boto3.resource('s3')
    try:
        s3 = boto3.resource('s3')
        r = s3.Bucket(bucket_name).put_object(Key=key_name, Body=file_contents)
        return r
    except botocore.exceptions.InvalidMaxRetryAttemptsError:
        print('Max Retry fail')
        raise


# extracts multiple TAR files from a high level TAR.GZ
def __extract_tars(file_path):
    return 'aardvark'


# takes a single .tar file and returns new fileobj(s) with normalised CSV data
def __untar_pon(pon_tarfile, dest_bucket_name, dest_prefix):
    # make sure the prefix ends in forward slash
    if not dest_prefix.endswith('/'):
        dest_prefix = dest_prefix + '/'

    for info in pon_tarfile:
        if info.isreg() and info.name.split('.')[-1].upper() == 'CSV':
            file_type = None
            timestamp = None
            object_type = ''
            ne_name = ''
            ne_type = ''
            headers = []
            raw_data = []
            data_row = []
            data_rows = []

            if info.name == 'iSAM_ponOltUtilTxOntHistoryData.csv':
                file_type = 'ponOltUtilTxOntHistory'
            elif info.name == 'iSAM_ponOltUtilRxOntHistoryData.csv':
                file_type = 'ponOltUtilRxOntHistory'
            elif info.name == 'iSAM_ponOltUtilHistoryData.csv':
                file_type = 'ponOltUtilHistory'
            elif info.name == 'iSAM_ontOltUtilBulkHistoryData.csv':
                file_type = 'ontOltUtilBulkHistory'
            elif info.name == 'iSAM_ng2ChannelPairOltUtilTxOntHistoryData.csv':
                file_type = 'ng2CpOltTxOntHistory'
            elif info.name == 'iSAM_ng2ChannelPairOltUtilRxOntHistoryData.csv':
                file_type = 'ng2CpOltRxOntHistory'
            elif info.name == 'iSAM_ng2ChannelPairOltUtilHistoryData.csv':
                file_type = 'ng2CpOltUtilHistory'
            elif info.name == 'iSAM_ng2OntOltUtilBulkHistoryData.csv':
                file_type = 'ng2OntOltUtilBulkHistory'

            if file_type is not  None:
                for line in pon_tarfile.extractfile(info.name):
                    csv_line = csv.reader([line.decode('utf-8')], quotechar='"')

                    # get the raw values
                    for r in csv_line:
                        try:
                            # get the file level variables and headers
                            if r[0] == 'Time stamp':
                                timestamp = r[1]
                            elif r[0] == 'Object Type':
                                object_type = r[1]
                            elif r[0] == 'NE Name':
                                ne_name = r[1]
                            elif r[0] == 'NE Type/Release':
                                ne_type = r[1]
                            elif r[0] == 'Object ID':
                                headers = ['file_type', 'time_stamp', 'objecttype', 'nename', 'ne_type'
                                    , r[0]
                                    , r[1] + '_1', r[1] + '_2', r[1] + '_3'
                                    , r[2] + '_1', r[2] + '_2', r[2] + '_3']
                            else:  # assuming these are data rows
                                raw_data = [(file_type, timestamp, object_type, ne_name, ne_type, r[0], r[1], r[2])]
                                split_1 = raw_data[0][6].strip('{}').split()
                                split_2 = raw_data[0][7].strip('{}').split()

                                # data_row = [(file_type, timestamp, object_type, ne_name, ne_type, r[0]
                                #              , split_1[0].strip(','), split_1[1].strip(','), split_1[2].strip(',')
                                #              , split_2[0].strip(','), split_2[1].strip(','), split_2[2].strip(','))]
                                # data_rows.append(data_row)

                                # normalise the arrays by splitting each row into three rows
                                for i in range (0,3):
                                    data_row = [(file_type, timestamp, object_type, ne_name, ne_type, r[0]
                                                 , split_1[i].strip(','), split_2[i].strip(','))]
                                    data_rows.append(data_row)

                        except IndexError:
                            pass

                with tempfile.SpooledTemporaryFile(mode='wb+') as temp_outfile:
                    # no headers, if required add: temp.write((','.join(map(str, headers)) + '\n').encode('utf-8'))
                    for row in data_rows:
                        # build a comma separated string & remove {} from array fields
                        delimited_row = (','.join(map(str, row)).strip('()') + '\n').encode('utf-8')
                        # replace single quotes from list obj with double for CSV output
                        a = delimited_row.decode().replace(" '",'"').replace("'",'"').encode('utf-8')
                        temp_outfile.write(a)

                    temp_outfile.seek(0)
                    outfile_key = get_filename(file_type=file_type
                                               , bucket_name=dest_bucket_name
                                               , prefix=dest_prefix)

                    upload_to_s3(bucket_name=dest_bucket_name
                                 , key_name=outfile_key
                                 , file_contents=temp_outfile)

    return True


# log processing details
# accepts key word args:
# log_application:          either s3 or dynamodb (mandatory)
#
def log_pon(**kwargs):
    # set file output options
    for arg in kwargs:
        if arg == 'log_application':
            log_application = kwargs[arg]


