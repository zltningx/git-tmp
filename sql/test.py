import pickle

class DataDict(object):
    def __init__(self, table_name):
        self.tableName = table_name
        self.dk = dict()
        self._key_value = dict()
        self._dk_dict = list()

    def __str__(self):
        return self.tableName

    @property
    def key_value(self):
        return self._key_value

    @property
    def dk_dict(self):
        return self._dk_dict

    def add_dk(self):
        pass


class IndexDict(object):
    def __init__(self):
        self._attribute_list_index = dict()

    @property
    def attribute_list_index(self):
        return self._attribute_list_index


def pickle_load(f_name):
    with open(f_name, 'rb') as file:
        return pickle.load(file)

a = pickle_load('i')
print(a.attribute_list_index)