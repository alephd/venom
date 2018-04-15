from os.path import expanduser
import json
import logging
import random
from termcolor import colored
import boto3
from session import Item

class Cluster(Item):
    # SSH key
    ssh_key_path = expanduser('~/.ssh/id_rsa.pub')
    image_name = 'Deep Learning AMI (Ubuntu) Version 7.0'
    instance_user = 'ubuntu'#'ec2-user, ubuntu'
    # instance_type in  ['p3.2xlarge', 'p3.8xlarge', 'p3.16xlarge', 'p2.xlarge', 'p2.8xlarge', 'p2.16xlarge', 'm5.large']
    def __init__(self, id='venom', size=3, profile_name='default', region_name='eu-west-1', subnet_id=None, image_id=None, instance_type='p2.xlarge', instance_role='EMR_EC2_DefaultRole', tags=(), ip_mask='0.0.0.0/0'):
        self.id = id
        self.size = size
        self.profile_name = profile_name
        self.region_name = region_name
        self.subnet_id = subnet_id
        self.image_id = image_id
        self.instance_type = instance_type
        self.instance_role = instance_role
        self.tags = list(tags)
        self.object_tags = self.tags + [{'Key': 'Id','Value': self.id}]
        self.ip_mask = ip_mask
        self.aws = boto3.session.Session(profile_name=self.profile_name, region_name=self.region_name)
        self.ec2 = self.aws.resource('ec2')
        self.subnet = None
        self.vpc = None
        self.image = None
        self.key_name = self.id+'-key'
        self.key = None
        self.security_group_name = self.id+'-sg'
        self.security_group = None
        self.instance_name = self.id+'-instance'
        self.instance_tags = self.object_tags + [{
            'Key': 'Name',
            'Value': self.instance_name
        }]
        self.instances = None
        # Load lazy fields
        self.load()

    def load(self, force=False):
        if force:
            self.subnet = None
            self.vpc = None
            self.image = None
            self.key = None
            self.security_group = None
            self.instances = None
        # Load key-pair
        if not self.key:
            try:
                self.key = random.choice(list(self.ec2.key_pairs.filter(Filters=[{'Name':'key-name', 'Values':[self.key_name]}])))
                logging.info("{} found".format(self.key_name))
            except Exception as e:
                self.key = None
                logging.info(e)
        # Load security group
        if not self.security_group:
            try:
                self.security_group = random.choice(list(self.ec2.security_groups.filter(Filters=[{'Name':'group-name', 'Values':[self.security_group_name]}])))
                logging.info("{} found".format(self.security_group_name))
            except Exception as e:
                self.security_group = None
                logging.info(e)
        # Load instances
        if not self.instances:
            if self.security_group:
                self.instances = list(self.ec2.instances.filter(
                    Filters=[{'Name':'tag:Name', 'Values':[self.instance_name]},
                        {'Name':'instance-state-name', 'Values':['pending', 'running', 'stopping', 'stopped']},
                        {'Name':'instance.group-id', 'Values':[self.security_group.group_id]}]
                ))
            else:
                self.instances = []
            self.hosts = [instance.public_dns_name or instance.private_dns_name for instance in self.instances]
            logging.info("{} instances found".format(self.hosts))
        # Load subnet
        if not self.subnet:
            if self.subnet_id:
                self.subnet = self.ec2.Subnet(self.subnet_id)
            elif self.security_group:
                self.subnet = random.choice(list(self.ec2.subnets.filter(
                    Filters=[{'Name':'vpc-id', 'Values':[self.security_group.vpc_id]}]
                )))
                self.subnet_id = self.subnet.id
                logging.info("{} found".format(self.subnet_id))
            else:
                self.subnet = random.choice(list(self.ec2.subnets.all()))
                self.subnet_id = self.subnet.id
        # Load VPC
        if not self.vpc:
            self.vpc = self.subnet.vpc
        # Load disk image
        if not self.image:
            if self.image_id:
                self.image = self.ec2.Image(self.image_id)
            else:
                self.image = random.choice(list(self.ec2.images.filter(
                    Filters=[{'Name':'name', 'Values':[self.image_name]}]
                )))
                self.image_id = self.image.id
                logging.info("{} found".format(self.image.name))
        return self

    def create(self):
        logging.info("Create the cluster")
        # Upload key pair if needed
        if not self.key:
            with open(self.ssh_key_path, 'r') as key_file:
                public_key = key_file.read()
            self.key = self.ec2.import_key_pair(KeyName=self.key_name, PublicKeyMaterial=public_key)
            logging.info(colored("{} created".format(self.key), 'green'))
        # Create security group if needed
        if not self.security_group:
            self.security_group = self.ec2.create_security_group(Description='Venom cluster security group', GroupName=self.security_group_name, VpcId=self.vpc.id)
            self.security_group.create_tags(Tags=self.object_tags)
            all_in_group = {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535,
                'UserIdGroupPairs': [{
                        'Description': 'Current security group',
                        'GroupId': self.security_group.group_id
                    }]
            }
            ssh_all = {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22,
                'IpRanges': [{
                        'Description': 'All',
                        'CidrIp': self.ip_mask
                    }]
            }
            services_all = {'IpProtocol': 'tcp', 'FromPort': 1024, 'ToPort': 65535,
                'IpRanges': [{
                        'Description': 'All',
                        'CidrIp': self.ip_mask
                    }]
            }
            icmp_all = {'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1,
                'IpRanges': [{
                        'Description': 'All',
                        'CidrIp': self.ip_mask
                    }]
            }
            self.security_group.authorize_ingress(IpPermissions=[all_in_group, ssh_all, services_all, icmp_all])
            logging.info(colored("{} created".format(self.security_group.group_name), 'green'))
        # Create instances if needed
        logging.info("Instances already running: {}".format(self.instances))
        if self.size>len(self.instances):
            self.instances += self.ec2.create_instances(ImageId=self.image.id, InstanceType=self.instance_type,
                KeyName=self.key_name, MinCount=self.size-len(self.instances), MaxCount=self.size-len(self.instances),
                SecurityGroupIds=[self.security_group.group_id], SubnetId=self.subnet.id,
                IamInstanceProfile={'Name': self.instance_role},
                TagSpecifications=[{'ResourceType': 'instance', 'Tags': self.instance_tags}]
            )
            logging.info("Creating instances")
            for instance in self.instances:
                instance.wait_until_running()
                instance.reload() # Reload lazyly loaded fields, for potential public_dns_name updates
                logging.info(colored("{} created".format(instance.id), 'green'))
            # Get the host list
            self.hosts = [instance.public_dns_name or instance.private_dns_name for instance in self.instances]
        return self

    def start(self):
        logging.info("Start the cluster")
        # Destroy instances
        if self.instances:
            for instance in self.instances:
                instance.start()
            for instance in self.instances:
                instance.wait_until_running()
                instance.reload() # Reload lazyly loaded fields, for potential public_dns_name updates
                logging.info(colored("{} started".format(instance.id), 'green'))
            # Get the host list
            self.hosts = [instance.public_dns_name or instance.private_dns_name for instance in self.instances]
        return self

    def stop(self):
        logging.info("Stop the cluster")
        # Destroy instances
        if self.instances:
            for instance in self.instances:
                instance.stop()
            for instance in self.instances:
                instance.wait_until_stopped()
                instance.reload() # Reload lazyly loaded fields, for potential public_dns_name updates
                logging.info(colored("{} stopped".format(instance.id), 'red'))
            # Get the host list
            self.hosts = [instance.public_dns_name or instance.private_dns_name for instance in self.instances]
        return self

    def terminate(self):
        logging.info("Destroy the cluster")
        # Destroy instances
        if self.instances:
            for instance in self.instances:
                instance.terminate()
            for instance in self.instances:
                instance.wait_until_terminated()
                instance.reload() # Reload lazyly loaded fields, for potential public_dns_name updates
                logging.info(colored("{} terminated".format(instance.id), 'red'))
            # Get the host list
            self.hosts = [instance.public_dns_name or instance.private_dns_name for instance in self.instances]
        # Destroy secirity group
        if self.security_group:
            self.security_group.delete()
            self.security_group = None
            logging.info(colored("{} destroyed".format(self.security_group_name), 'red'))
        # Destroy key pair
        if self.key:
            self.key.delete()
            self.key = None
            logging.info(colored("{} destroyed".format(self.key_name), 'red'))
        return self

    def __freeze__(self):
        result = super().__freeze__()
        result.update({'id':self.id, 'size':self.size, 'profile_name':self.profile_name, 'region_name':self.region_name,
            'subnet_id':self.subnet_id, 'image_id':self.image_id, 'instance_type':self.instance_type, 'instance_role':self.instance_role,
            'tags':self.tags, 'ip_mask':self.ip_mask})
        return result

    @staticmethod
    def __unfreeze__(obj):
        return Cluster(id=obj['id'], size=obj['size'], profile_name=obj['profile_name'], region_name=obj['region_name'],
        subnet_id=obj['subnet_id'], image_id=obj['image_id'], instance_type=obj['instance_type'], instance_role=obj['instance_role'],
        tags=obj['tags'], ip_mask=obj['ip_mask'])
