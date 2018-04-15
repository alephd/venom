import os
import json
import logging
from termcolor import colored

class Item(object):
    # A session Item should implement freeze and unfreeze primitives
    def __freeze__(self):
        return {
            '__module__':self.__class__.__module__,
            '__class__':self.__class__.__name__
        }
    @staticmethod
    def __unfreeze__(obj):
        if '__module__' in obj and '__class__' in obj:
            try:
                return getattr(__import__(obj['__module__']), obj['__class__']).__unfreeze__(obj)
            except Exception as e:
                logging.warning(colored(e, 'yellow'))
        return obj

class Session(Item):
    def __init__(self, path=None):
        self.path = path or 'session.json'
        self.data = {}
        if not os.path.exists(self.path):
            with open(self.path, 'w') as file:
                Session.dump(self.data, file)

    def __enter__(self):
        logging.info("Openning local session at {}".format(self.path))
        with open(self.path, 'r') as file:
            self.data = Session.load(file)
        return self

    def __exit__(self, type, value, traceback):
        logging.info("Closing {}".format(self.path))
        with open(self.path, 'w') as file:
            Session.dump(self.data, file)
    # Delegated methods
    __contains__ = lambda self, key: self.data.__contains__(key)
    __getitem__ = lambda self, key: self.data.__getitem__(key)
    __setitem__ = lambda self, key, value: self.data.__setitem__(key, value)
    __delitem__ = lambda self, key: self.data.__delitem__(key)
    # Json methods
    @staticmethod
    def load(file):
        return json.load(file, object_hook=Item.__unfreeze__)
    @staticmethod
    def dump(obj, file):
        return json.dump(obj, file, default=lambda obj: obj.__freeze__(),
        sort_keys=True, indent=2, separators=(',', ': '))
