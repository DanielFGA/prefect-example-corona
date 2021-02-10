import random
from time import sleep


class StringConverter(dict):
    def __contains__(self, item):
        return True
    def __getitem__(self, item):
        return str
    def get(self, default=None):
        return str


def remove_dot(x):
    return x.replace('.', '')


def sleep_random():
    sleep(random.randint(1, 10))