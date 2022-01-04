"""
This simple utility reads the data from S3 and does checkpoint the information about last read location
"""
import datetime

import botocore.exceptions
import pandas as pd
import boto3
import logging

import pytest

dynamodb_boto3_client = boto3.client("dynamodb")
import logging

logger = logging.getLogger("root")


class BookMarks(object):
    valid_file_formats = ["csv", "parquet", "json", "xml"]
    """
    The constructor initializes the utility with S3 location information and table to store bookmark info.
    """

    def __init__(self,
                 s3_bucket_name: str,
                 s3_location: str,
                 format_of_data: str,
                 job_name: str,
                 dynamo_db_table_for_bookmark_storage: str = "bookmark_table"):
        self.s3_bucket_name = s3_bucket_name
        self.s3_location = s3_location
        self.format_of_the_data = format_of_data
        self.job_name = job_name
        self.dynamo_db_table_for_bookmark_storage = dynamo_db_table_for_bookmark_storage

    @property
    def s3_bucket_name(self):
        return self.__s3_bucket_name

    @s3_bucket_name.setter
    def s3_bucket_name(self, value):
        self.__s3_bucket_name = value

    @property
    def s3_location(self):
        return self.__s3_location

    @s3_location.setter
    def s3_location(self, value):
        self.__s3_location = value

    @property
    def format_of_the_data(self):
        return self.__format_of_the_data

    @format_of_the_data.setter
    def format_of_the_data(self, value):
        if value not in self.valid_file_formats:
            raise Exception("File format is not valid. Format should be one of csv,parquet,json and xml")

        self.__format_of_the_data = value

    @property
    def dynamo_db_table_for_bookmark_storage(self):
        return self.__dynamo_db_table_for_bookmark_storage

    @dynamo_db_table_for_bookmark_storage.setter
    def dynamo_db_table_for_bookmark_storage(self, value):
        try:
            table_schema = dynamodb_boto3_client.describe_table(
                TableName=value)['Table']['KeySchema']
            partition_key = [key['AttributeName'] for key in table_schema if key['KeyType'] == 'HASH'][0]
            sort_key = [key['AttributeName'] for key in table_schema if key['KeyType'] == 'RANGE'][0]
            if partition_key != "job_name":
                raise Exception("Partition key name is incorrect. It should be job_name")

            if sort_key != "bookmark_timestamp":
                raise Exception("Sort key name is not incorrect. It should be bookmark_timestamp")

        except dynamodb_boto3_client.exceptions.ResourceNotFoundException:
            print("Table not found and new table is being created")
            dynamodb_boto3_client.create_table(
                TableName=value,
                KeySchema=[
                    {
                        'AttributeName': 'job_name',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'bookmark_timestamp',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'job_name',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'bookmark_timestamp',
                        'AttributeType': 'N'
                    }

                ],
                BillingMode='PAY_PER_REQUEST'
            )
        self.__dynamo_db_table_for_bookmark_storage = value

    @property
    def job_name(self):
        return self.__job_name

    @job_name.setter
    def job_name(self, value):
        self.__job_name = value

    """
    process s3 file location information
    """

    def process_s3_location_information(self, s3_list_object_response):
        files = sorted((f for f in s3_list_object_response['Contents'] if f['Key'].endswith(".csv")),
                       key=lambda file_name: file_name['LastModified'], reverse=True)

        latest_timestamp = files[0]['LastModified'].timestamp()
        list_of_files_to_process = [f"s3://{self.s3_bucket_name}/{file['Key']}" for file in files]
        return latest_timestamp, list_of_files_to_process

    """
    This method registers the information about the last read timestamp in DynamoDB table
    """

    def register_bookmark(self, latest_timestamp):
        dynamodb_boto3_client.put_item(
            TableName=self.dynamo_db_table_for_bookmark_storage,
            Item={
                'job_name': {'S': self.job_name},
                'bookmark_timestamp': {'N': latest_timestamp},
                'data_load_timestamp': {'N': datetime.datetime.now().strftime('%s')}
            }
        )

    """
    get latest timestamp information from DynamoDB table
    """

    def get_latest_timestamp_from_db(self):
        result = dynamodb_boto3_client.query(
            TableName='bookmark_table',
            KeyConditionExpression="#job_name = :job_name",
            ExpressionAttributeNames={
                '#job_name': 'job_name'
            },
            ExpressionAttributeValues={
                ':job_name': {
                    'S': self.job_name
                }
            },
            ScanIndexForward=False,
            Limit=1,
        )

        if len(result['Items']) < 1:
            return 0
        else:
            return result['Items'][0]['bookmark_timestamp']['N']

    """
    This method reads the data from S3
    """

    def load_data_from_s3(self):
        s3 = boto3.client('s3')

        existing_timestamp = self.get_latest_timestamp_from_db()
        print("--------------->>>>>>>")
        print(f"existing timestamp {existing_timestamp}")
        print("--------------->>>>>>>")

        response = s3.list_objects(
            Bucket=self.s3_bucket_name,
            Prefix=self.s3_location,
        )

        latest_timestamp, files_to_process = self.process_s3_location_information(response)

        dataframes_to_union = []

        for filename in files_to_process:
            print(f"loading the filename: {filename}")
            df = pd.read_csv(filename, encoding='cp1252')
            dataframes_to_union.append(df)

        final_dataframe_with_latest_data = pd.concat(dataframes_to_union, axis=0, ignore_index=True)
        self.register_bookmark(latest_timestamp)




def test_register_bookmark():
    bm = BookMarks(
        "test-bucket-for-datalab",
        "folder-for-bookmark",
        "csv",
        "job_123"
        "bookmark_table")

    bm.register_bookmark("99999999999")

def test_get_latest_timestamp():
    bm = BookMarks(
        "test-bucket-for-datalab",
        "folder-for-bookmark",
        "csv",
        "job_123"
        "bookmark_table")

    assert bm.get_latest_timestamp_from_db() == "99999999999"

def test_validation():
    with pytest.raises(Exception) as ex:
        BookMarks(
            "test-bucket-for-datalab",
            "folder-for-bookmark-test",
            "abcd",
            "job_123"
            "bookmark_table")


def test_s3_load_exception():
    with pytest.raises(botocore.exceptions.ClientError):
        bm = BookMarks(
            "bucket-12345",
            "folder-for-bookmark-test",
            "csv",
            "job_123"
            "bookmark_table")
