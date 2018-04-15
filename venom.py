import os
import time
import signal
import logging
from stack import Oath, Home, Local, OATH_TAGS

'''
activate VPN
kinit
awsfed --account=aolp-dev --browser_sso=True
'''


# Run the main program
if __name__ == '__main__':
    #os.setpgrp()
    pgrp = os.getpgrp()
    print(pgrp)
    logging.basicConfig(level=logging.INFO)
    # stk = Oath(size=5, id='test-ng')
    stk = Local(size=5, id='test-local')
    stk.create()
    try:
        stk.setup()
        while True:
            time.sleep(1)
    finally:
        stk.terminate()
        os.killpg(pgrp, signal.SIGKILL)
