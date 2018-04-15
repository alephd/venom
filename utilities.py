import logging
import socket
import time
from fabric.api import env, settings, parallel, serial, roles, execute, sudo, run, local, put, get
#from fabric.contrib.files import append

def wait_for(action, interval=5, message="Waiting"):
    while True:
        logging.info(message)
        try:
            execute(action)
            return
        except SystemExit as e:
            logging.info(e)
            logging.info("Retry")
            time.sleep(interval)

def wait_for_ssh():
    wait_for(lambda : run('echo "ssh responding"'), message="Waiting for SSH")

def wait(action, interval=5, message="Waiting", run=run):
    run('while ! {}; do sleep {}; echo "{}"; done'.format(action, interval, message))

def wait_for_apt():
    wait('apt update', message="Waiting for APT", run=sudo)

def apt_install(package):
    wait('apt install {package}'.format(package=package), message="Waiting for APT", run=sudo)

def wait_for_file(path, roles=roles()):
    wait('cat {}'.format(path), message="Waiting for a file")

def daemon(name, cmd, options='--inherit --respawn'):
    run('''
if (daemon --name="{name}" --running)
then
    daemon --name="{name}" --restart
else
    daemon --name="{name}" --stdout={name}.out --stderr={name}.err --chdir=/home/ubuntu/ {options} -- {cmd}
fi
'''.format(name=name, cmd=cmd, options=options))

def write(path, text, run=run):
    run("echo '{text}' > {path}".format(path=path, text=text))

def append(path, text, run=run):
    run("echo '{text}' >> {path}".format(path=path, text=text))
