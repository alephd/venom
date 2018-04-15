import os
import os.path
import tempfile
import uuid
import logging
import subprocess
import signal
from termcolor import colored
from fabric.api import env, output, settings, parallel, serial, roles, execute, sudo, run, local, put, get
#from fabric.contrib.files import append
from session import Session
from store import Store
from cluster import Cluster
from utilities import wait_for_ssh, wait_for_apt, apt_install, wait_for_file, daemon, write, append

ID = 'ng-{}'.format(uuid.uuid1().hex[:4])

class Stack(object):
    def __init__(self, id=ID):
        self.id = id
        with Session() as session:
            # Get stack
            if self.id in session:
                self.stack = session[self.id]
            # Get Store
            if 'store' in self.stack:
                self.store = self.stack['store']
            # Get Cluster
            if 'cluster' in self.stack:
                self.cluster = self.stack['cluster']

    def create(self):
        return self

    def terminate(self):
        return self

    def setup(self):
        logging.info("Setup the cluster for IPyParallel")
        # Set logging level
        output.stdout = False
        # Setup env
        env.use_ssh_config = True
        env.user = self.cluster.instance_user
        env.hosts = self.cluster.hosts
        env.roledefs = {
            'master': [env.hosts[0]],
            'workers': env.hosts
        }
        env.parallel = True
        env.disable_known_hosts = True # To avoid host checking
        wait_for_ssh()
        execute(self.all)
        execute(self.master)
        # Setup workers using master config
        execute(self.workers)
        logging.info(colored("You can now connect to http://{}:8888".format(env.hosts[0]), 'yellow'))
        logging.info(colored("Test ipyparallel (for tensorflow_p36 env) with", 'yellow'))
        logging.info(colored("""
import ipyparallel as ipp
rc = ipp.Client()
rc.ids
""", 'white'))
        logging.info(colored("then", 'yellow'))
        logging.info(colored("""
v = rc[:]
def ls(i):
    import socket
    return (i, socket.gethostname())

r = v.map(ls, range(100))
for l in r.get():
    print(l)
""", 'white'))
        return self

    def all(self):
        wait_for_apt()
        # Install daemon utils
        apt_install('daemon')
        # Install ipyparallel for tensorflow_p36
        with settings(prompts={'Proceed ([y]/n)? ': 'y'}):
            run('conda install ipyparallel')
            run('conda install -n tensorflow_p36 ipyparallel')
            #run('source activate tensorflow_p27; conda install ipyparallel')

    @roles('master')
    def master(self):
        # Install s3contents to read notebooks from S3
        # run('pip install s3contents')
        run('pip install https://github.com/danielfrg/s3contents/archive/master.zip')
        write('/home/ubuntu/.jupyter/jupyter_notebook_config.py', '''
from s3contents import S3ContentsManager
c = get_config()
# Use existing config
c.NotebookApp.kernel_spec_manager_class = "environment_kernels.EnvironmentKernelSpecManager"
c.NotebookApp.iopub_data_rate_limit = 10000000000
# Tell Jupyter to use S3ContentsManager for all storage.
c.NotebookApp.contents_manager_class = S3ContentsManager
c.S3ContentsManager.bucket = "{bucket}"
c.S3ContentsManager.sse = "aws:kms"
'''.format(bucket=self.store.name))
        # Run ipcontroller
        daemon('ipcontroller', 'ipcontroller --ip="*"')
        local('mkdir -p ~/.ipython/profile_default/security/')
        wait_for_file('/home/ubuntu/.ipython/profile_default/security/ipcontroller-client.json')
        wait_for_file('/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json')
        get('/home/ubuntu/.ipython/profile_default/security/ipcontroller-client.json', '~/.ipython/profile_default/security/ipcontroller-client.json')
        get('/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json', '~/.ipython/profile_default/security/ipcontroller-engine.json')
        daemon('notebook', 'jupyter notebook --ip="*" --NotebookApp.token=""')
        sudo('ipcluster nbextension enable')

    @roles('workers')
    def workers(self):
        run('mkdir -p /home/ubuntu/.ipython/profile_default/security/')
        put('~/.ipython/profile_default/security/ipcontroller-client.json', '/home/ubuntu/.ipython/profile_default/security/ipcontroller-client.json')
        put('~/.ipython/profile_default/security/ipcontroller-engine.json', '/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json')
        daemon('ipengine', 'ipengine --file=/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json --ip="*"')


OATH_EU_WEST_1_AMI = 'ami-55d6882c'
OATH_INSTANCE_ROLE = 'EMR_EC2_DefaultRole'
OATH_NETWORK = '10.0.0.0/8'
OATH_TAGS = [{
        'Key': 'Stack',
        'Value': 'alephd'
    },{
        'Key': 'App',
        'Value': 'test'
    },{
        'Key': 'Stage',
        'Value': 'dev'
    },{
        'Key': 'Owner',
        'Value': 'alephd-randd@teamaol.com'
    },{
        'Key': 'OrbProjectId',
        'Value': '67921890'
    }]

# An Oath cluster
class Oath(Stack):
    def __init__(self, id=ID, size=4, instance_type='p2.xlarge'):
        self.id = id
        with Session() as session:
            # Get stack
            if self.id in session:
                self.stack = session[self.id]
            else:
                self.stack = {}
                session[self.id] = self.stack
            # Get Store
            if 'store' in self.stack:
                self.store = self.stack['store']
            else:
                self.store = Store(id=self.id, profile_name='federate', tags=OATH_TAGS)
                self.stack['store'] = self.store
            # Get Cluster
            if 'cluster' in self.stack:
                self.cluster = self.stack['cluster']
            else:
                self.cluster = Cluster(id=self.id, size=size, profile_name='federate', region_name='eu-west-1', image_id=OATH_EU_WEST_1_AMI,
                    subnet_id='subnet-ea60c68e', instance_type=instance_type, instance_role=OATH_INSTANCE_ROLE, tags=OATH_TAGS, ip_mask=OATH_NETWORK)
                self.stack['cluster'] = self.cluster

    def create(self):
        self.store.create()
        self.cluster.create()
        return self

    def terminate(self):
        # We clean the bucket
        # self.store.bucket.Object('.s3keep')
        # We may want to delete bucket
        # self.store.terminate()
        self.cluster.terminate()
        with Session() as session:
            del session[self.id]
            self.stack = None
            # self.store = None
            self.cluster = None
        return self

HOME_INSTANCE_ROLE = 'EMR_EC2_DefaultRole'
# A home cluster
class Home(Stack):
    def __init__(self, id=ID, size=4, instance_type='m5.large'):
        self.id = id
        with Session() as session:
            # Get stack
            if self.id in session:
                self.stack = session[self.id]
            else:
                self.stack = {}
                session[self.id] = self.stack
            # Get Store
            if 'store' in self.stack:
                self.store = self.stack['store']
            else:
                self.store = Store(id=self.id)
                self.stack['store'] = self.store
            # Get Cluster
            if 'cluster' in self.stack:
                self.cluster = self.stack['cluster']
            else:
                self.cluster = Cluster(id=self.id, size=size, region_name='eu-west-1', instance_type=instance_type, instance_role=HOME_INSTANCE_ROLE)
                self.stack['cluster'] = self.cluster

    def create(self):
        self.store.create()
        self.cluster.create()
        return self

    def terminate(self):
        # We clean the bucket
        # self.store.bucket.Object('.s3keep')
        # We may want to delete bucket
        # self.store.terminate()
        self.cluster.terminate()
        with Session() as session:
            del session[self.id]
            self.stack = None
            # self.store = None
            self.cluster = None
        return self

# A home cluster
class Local(Stack):
    def __init__(self, id=ID, size=4, profile_name='default', tags=()):
        self.id = id
        self.size = size
        self.profile_name = profile_name
        self.tags = tags
        # self.path = os.path.join(tempfile.gettempdir(), self.id)
        self.path = os.path.join('/tmp', self.id)
        self.subprocesses = []
        with Session() as session:
            # Get stack
            if self.id in session:
                self.stack = session[self.id]
            else:
                self.stack = {}
                session[self.id] = self.stack
            # Get Store
            if 'store' in self.stack:
                self.store = self.stack['store']
            else:
                self.store = Store(id=self.id, profile_name=self.profile_name, tags=self.tags)
                self.stack['store'] = self.store

    def create(self):
        self.store.create()
        return self

    def terminate(self):
        with Session() as session:
            del session[self.id]
            self.stack = None
        return self

    def setup(self):
        logging.info("Setup the cluster for IPyParallel")
        try:
            os.mkdir(self.path)
        except FileExistsError as e:
            logging.info("{} already exists".format(self.path))
        subprocess.run('pip3 install ipyparallel', shell=True)
        subprocess.run('pip3 install https://github.com/danielfrg/s3contents/archive/master.zip', shell=True)
        self.subprocesses.append(subprocess.Popen('ipcontroller &', shell=True))
        subprocess.run('''echo 'from s3contents import S3ContentsManager
c = get_config()
# Set working dir
c.NotebookApp.notebook_dir = "{path}"
# Tell Jupyter to use S3ContentsManager for all storage.
c.NotebookApp.contents_manager_class = S3ContentsManager
c.S3ContentsManager.bucket = "{bucket}"' > {path}/jupyter_notebook_config.py
'''.format(bucket=self.store.name, path=self.path), shell=True)
        for host in range(self.size):
            self.subprocesses.append(subprocess.Popen('cd {path}; ipengine &'.format(path=self.path), shell=True))
        self.subprocesses.append(subprocess.Popen('jupyter notebook --ip="*" --NotebookApp.token="" --config {path}/jupyter_notebook_config.py'.format(path=self.path), shell=True))
        logging.info(colored("You can now connect to http://localhost:8888", 'yellow'))
        logging.info(colored("Test ipyparallel (for tensorflow_p36 env) with", 'yellow'))
        logging.info(colored("""
import ipyparallel as ipp
rc = ipp.Client()
rc.ids
""", 'white'))
        logging.info(colored("then", 'yellow'))
        logging.info(colored("""
v = rc[:]
def ls(i):
    import socket
    return (i, socket.gethostname())

r = v.map(ls, range(100))
for l in r.get():
    print(l)
""", 'white'))
        return self

    @roles('master')
    def master(self):
        # Run ipcontroller
        daemon('ipcontroller', 'ipcontroller --ip="*"')
        local('mkdir -p ~/.ipython/profile_default/security/')
        wait_for_file('/home/ubuntu/.ipython/profile_default/security/ipcontroller-client.json')
        wait_for_file('/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json')
        get('/home/ubuntu/.ipython/profile_default/security/ipcontroller-client.json', '~/.ipython/profile_default/security/ipcontroller-client.json')
        get('/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json', '~/.ipython/profile_default/security/ipcontroller-engine.json')
        daemon('notebook', 'jupyter notebook --ip="*" --NotebookApp.token=""')
        sudo('ipcluster nbextension enable')

    @roles('workers')
    def workers(self):
        run('mkdir -p /home/ubuntu/.ipython/profile_default/security/')
        put('~/.ipython/profile_default/security/ipcontroller-client.json', '/home/ubuntu/.ipython/profile_default/security/ipcontroller-client.json')
        put('~/.ipython/profile_default/security/ipcontroller-engine.json', '/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json')
        daemon('ipengine', 'ipengine --file=/home/ubuntu/.ipython/profile_default/security/ipcontroller-engine.json --ip="*"')
