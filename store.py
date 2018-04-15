from os.path import expanduser
import json
import logging
import random
from termcolor import colored
import boto3
from session import Item

class Store(Item):
    def __init__(self, id='venom', profile_name='default', region_name='eu-west-1', tags=()):
        self.id = id
        self.profile_name = profile_name
        self.region_name = region_name
        self.name = self.id+'-bucket'
        self.tags = list(tags)
        self.object_tags = self.tags + [{
                'Key': 'Id',
                'Value': self.id
            },{
                'Key': 'Name',
                'Value': self.name
            }]
        self.aws = boto3.session.Session(profile_name=self.profile_name, region_name=self.region_name)
        self.s3 = self.aws.resource('s3')
        self.bucket = None
        # Load lazy fields
        self.load()

    def load(self, force=False):
        if force:
            self.bucket = None
        if not self.bucket:
            if self.s3.Bucket(self.name).creation_date:
                self.bucket = self.s3.Bucket(self.name)
                logging.info("{} found".format(self.name))
            else:
                self.bucket = None
                logging.info("{} not found".format(self.name))
        return self

    def create(self):
        # Create bucket
        logging.info("Create the {} S3 bucket".format(self.name))
        if not self.bucket:
            self.bucket = self.s3.create_bucket(Bucket=self.name, CreateBucketConfiguration={'LocationConstraint': self.region_name})
            self.bucket.wait_until_exists()
            self.bucket.Tagging().put(Tagging={'TagSet': self.object_tags})
            logging.info(self.bucket.objects)
            logging.info(colored("{} S3 bucket created".format(self.name), 'green'))
        return self

    def terminate(self):
        # Terminate  bucket
        if self.bucket:
            logging.info("Terminate the {} S3 bucket".format(self.name))
            self.bucket.objects.all().delete()
            self.bucket.delete()
            self.bucket.wait_until_not_exists()
            logging.info(colored("{} S3 bucket terminated".format(self.name), 'red'))
        return self

    def __freeze__(self):
        result = super().__freeze__()
        result.update({'id':self.id, 'profile_name':self.profile_name, 'region_name':self.region_name, 'tags':self.tags})
        return result

    @staticmethod
    def __unfreeze__(obj):
        return Store(id=obj['id'], profile_name=obj['profile_name'], region_name=obj['region_name'], tags=obj['tags'])
