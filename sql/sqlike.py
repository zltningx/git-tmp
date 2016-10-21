import pickle
import os
import re
from collections import OrderedDict
from queue import Queue
import bisect


def combine_list(list_a, list_b):
    new_list = list()
    if not list_a:
        return list_b
    elif not list_b:
        return list_a
    for a_value in list_a:
        for b_value in list_b:
            if a_value not in new_list:
                new_list.append(a_value)
            if b_value not in new_list:
                new_list.append(b_value)
    return new_list

class DataDict(object):
    def __init__(self, table_name):
        self.tableName = table_name
        self._key_value = OrderedDict()
        self._dk_dict = list()
        self.index_list = list()
        self._index_name = None

    def __str__(self):
        return self.tableName

    @property
    def key_value(self):
        return self._key_value

    @property
    def dk_dict(self):
        return self._dk_dict

    @property
    def index_name(self):
        return self._index_name

    def get_keys(self):
        return self._key_value.keys()

    def update_key(self, my_key, value):
        self._key_value.update({my_key: value})

    def del_key(self, my_key):
        del self._key_value[my_key]

    def clean_data(self):
        self._dk_dict = list()

    def set_index_name(self, index_name):
        self._index_name = index_name


class IndexDict(object):
    def __init__(self):
        self._attribute_list_index = dict()

    @property
    def attribute_list_index(self):
        return self._attribute_list_index

    def get_keys(self):
        return self._attribute_list_index.keys()


class SqlManager(object):
    def __init__(self):
        pass

    def execute(self, string):
        command = string.split()
        if command[0] == "create" and command[1] == 'table':
            v = re.match(r'create\s+table\s+(\w+)', string)
            self.create(string)
        elif command[0] == "insert" and command[1] == 'into':
            v = re.match(r'insert\s+into\s+(\w+)\s+\((.*)\)\s+values\s*\((.*)\)', string)
            try:
                self.insert(v.group(1), v.group(2), v.group(3))
            except:
                try:
                    v = re.match(r'insert\s+into\s+(\w+)\s+values\s*\((.*)\)', string)
                    self.insert(v.group(1), None, v.group(2))
                except Exception as e:
                    print(e)
                    print("Command Error")

        elif command[0] == "delete" and command[1] == 'from':
            if 'where' in string:
                v = re.match(r'delete\s+from\s+(\w+)\s+where\s+(.*\S)', string)
            else:
                v = re.match(r'delete\s+from\s+(\w+)', string)
            self.delete(string)

        elif command[0] == "update" and command[2] == 'set':
            if 'where' in string:
                v = re.match(r'update\s+(\w+)\s+set\s+(\w+)\s+=\s+(\w+)\s+where\s+(.*\S)', string)
            else:
                v = re.match(r'update\s+(\w+)\s+set\s+(\w+)\s+=\s+(\w+)', string)
            self.update(string)

        elif command[0] == "select":
            if 'where' in string:
                v = re.match(r'select\s+(.*\S)\s+from\s+(.*\S)\s+where\s*(.*\S)', string)
                if ',' in v.group(2):
                    self.select_multi(v.group(1), v.group(2), v.group(3))
                else:
                    self.select_single(v.group(1), v.group(2), v.group(3))
            else:
                v = re.match(r'select\s+(.*\S)\s+from\s+(.*\S)', string)
                if ',' in v.group(2):
                    self.select_multi(v.group(1), v.group(2))
                else:
                    self.select_single(v.group(1), v.group(2))

        elif command[0] == "alter" and command[1] == 'table' and command[3] == 'add':
            v = re.match(r'alter\s+table\s+(\w+)\s+add\s+(.*\S)', string)
            self.alter_add(string)
        elif command[0] == "alter" and command[1] == 'table' and command[3] == 'drop':
            v = re.match(r'alter\s+table\s+(\w+)\s+drop\s+(.*\S)', string)
            self.alter_drop(string)
        elif command[0] == 'drop':
            v = re.match(r'drop\s+table\s+(\w+)', string)
            self.drop(string)
        elif command[0] == 'create' and command[1] == 'index':
            v = re.match(r'create\s+index\s+(\w+)\s+on\s+(.*\S)\s+\((.*)\)', string)
            self.create_index(v.group(1), v.group(2), v.group(3))
        else:
            print("Error Command")

    def create_index(self, index_name, table_name, attribute_list):
        goahead = False
        if index_name == table_name:
            print("index name could not be table name!")
            return
        for f_name in os.listdir('.'):
            if f_name == table_name:
                goahead = True
            if f_name == index_name:
                print("index has been exist!")
                return
        if not goahead:
            print("table doesn't exist")
            return
        index_dump = IndexDict()
        attribute_list = attribute_list.split(',')
        for attribute in attribute_list:
            attribute = attribute.strip()
            attr_index_list = self.attribute_index(attribute, table_name)
            if attr_index_list == -1:
                print("operate shutdown")
                return
            index_dump.attribute_list_index[attribute] = attr_index_list
        self.pickle_dump(index_name, index_dump)
        data_dict = self.pickle_load(table_name)
        # 索引已存在情况没有处理
        data_dict.set_index_name(index_name)
        self.pickle_dump(table_name, data_dict)

    def attribute_index(self, attribute, table_name):
        data_dict = self.pickle_load(table_name)
        if attribute not in data_dict.get_keys():
            print("{} not table attribute".format(attribute))
            return -1

        all_dict = dict()
        for index, item in enumerate(data_dict.dk_dict):
            if attribute in item.keys():
                all_dict[item[attribute]] = item

        return [[k, all_dict[k]] for k in sorted(all_dict.keys())]

    def select_single(self, attr_list, table_name, condition_list=None):
        goahead = False
        for f_name in os.listdir('.'):
            if f_name == table_name:
                goahead = True
        if not goahead:
            print("table doesn't exist")
            return

        data_dict = self.pickle_load(table_name)

        if condition_list is not None:
            result_list = self.deal_condition(condition_list, data_dict)
            print(result_list)
            if attr_list == "*":
                pass
            else:
                pass
        elif attr_list == '*':
            for item in data_dict.dk_dict:
                for key, value in item.items():
                    print("{} : {}".format(key, value), end=" ")
                print()
        else:
            for item in data_dict.dk_dict:
                for key, value in item.items():
                    if key in attr_list:
                        print("{} : {}".format(key, value), end=" ")
                print()

    def select_multi(self, attr_list, table_name_list, condition_list=None):
        goahead = False
        table_dict = dict()
        table_name_list = table_name_list.split(',')
        for table_name in table_name_list:
            for f_name in os.listdir('.'):
                if f_name == table_name.strip():
                    goahead = True
            if not goahead:
                print("table {} doesn't exist".format(table_name.strip()))
                return
            table_dict[table_name.strip()] = self.pickle_load(table_name.strip())

        if condition_list is not None:
            pass
        elif attr_list == '*':
            for table_name in table_name_list:
                print("Table---->{}<-----".format(table_name.strip()))
                for item in table_dict[table_name.strip()].dk_dict:
                    for key, value in item.items():
                        print("{} : {}".format(key, value), end=" ")
                    print()
        else:
            for table_name in table_name_list:
                print("Table---->{}<-----".format(table_name.strip()))
                for item in table_dict[table_name.strip()].dk_dict:
                    for key, value in item.items():
                        if key in attr_list:
                            print("{} : {}".format(key, value), end=" ")
                    print()

    def deal_condition(self, condition_list, data_dict):
        table_list = list()
        if 'or' in condition_list:
            or_condition_list = condition_list.split('or')
            for or_condition in or_condition_list:
                if 'and' in or_condition:
                    tmp_list = list()
                    and_condition_list = or_condition.split('and')
                    for i, and_condition in enumerate(and_condition_list):
                        line_list = self.isValued(and_condition, data_dict)
                        if line_list is None or not line_list:
                            tmp_list = list()
                            break
                        if i == 0:
                            tmp_list = line_list
                        else:
                            tmp_list = [value for value in tmp_list if value in line_list]
                        if not tmp_list:
                            break
                    table_list = combine_list(table_list, tmp_list)
                else:
                    line_list = self.isValued(or_condition, data_dict)
                    if line_list is None:
                        continue
                    table_list = combine_list(table_list, line_list)
        elif 'and' in condition_list:
            and_condition_list = condition_list.split('and')
            for i, and_condition in enumerate(and_condition_list):
                line_list = self.isValued(and_condition, data_dict)
                if line_list is None or not line_list:
                    return list()
                if i == 0:
                    table_list = line_list
                else:
                    table_list = [value for value in table_list if value in line_list]
                if not table_list:
                    return table_list
        else:
            table_list = self.isValued(condition_list, data_dict)
        return table_list

    def isValued2(self, condition, table_dict):
        v = re.match(r'\s*(\w+)\.(\w+)\s*(=|!=|>|<|>=|<=)\s*(\w+)\.(\w+|\d+)',
                     condition)
        try:
            if v.group(1) not in table_dict.get_keys():
                pass
            else:
                pass

    def isValued(self, condition, data_dict):
        v = re.match(r'\s*(\w+)\s*(=|!=|>|<|>=|<=)\s*(\w+|\d+)', condition)
        try:
            if v.group(1) not in data_dict.get_keys():
                print("key not in table plz checkout it")
                return None
            else:
                key = v.group(1)
                operator = v.group(2)
                value = v.group(3)
                line_list = list()
                if operator == '=':
                    for item in data_dict.dk_dict:
                        if str(item[key]) == value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '!=':
                    for item in data_dict.dk_dict:
                        if str(item[key]) != value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '>':
                    for item in data_dict.dk_dict:
                        if str(item[key]) > value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '<':
                    for item in data_dict.dk_dict:
                        if str(item[key]) < value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '>=':
                    for item in data_dict.dk_dict:
                        if str(item[key]) >= value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '<=':
                    for item in data_dict.dk_dict:
                        if str(item[key]) <= value:
                            line_list.append((data_dict.tableName, item))
                return line_list
        except Exception as e:
            print(e)
            return None

    def pickle_load(self, f_name):
        with open(f_name, 'rb') as file:
            return pickle.load(file)

    def pickle_dump(self, f_name, obj):
        with open(f_name, 'wb') as file:
            pickle.dump(obj, file)

    def create(self, string):
        name = string.split()[2]
        try:
            command = re.findall(r'{(.+?)}', string)[0]
        except:
            print("Command Error")
            return
        command = command.split(',')
        for i in os.listdir('.'):
            if i == name:
                print("Table Exist")
                return
        date = DataDict(name)
        for attr in command:
            date.key_value[attr.split()[0]] = " ".join(attr.split()[1:])
        self.pickle_dump(name, date)

    def insert(self, table_name, attribute_list, values_list):
        goahead = False
        for i in os.listdir('.'):
            if i == table_name:
                goahead = True
        if not goahead:
            print("Table Doesn't Exist")
            return

        data_dict = self.pickle_load(table_name)
        values = values_list.split(',')
        data_keys = data_dict.get_keys()
        dk = OrderedDict()  # one line
        if attribute_list is None:
            # all done
            if len(values) != len(data_keys):
                print("Given not matching")
                return
            for i, key in enumerate(data_keys):
                # primary key check
                if 'primary key' in data_dict.key_value[key]:
                    if 'int' in data_dict.key_value[key]:
                        for item in data_dict.dk_dict:
                            if item[key] == int(values[i].strip()):
                                print("primary key has existed")
                                return
                    else:
                        for item in data_dict.dk_dict:
                            if item[key] == values[i].strip():
                                print("primary key has existed")
                                return
                # int key check
                if 'int' in data_dict.key_value[key]:
                    if not values[i].strip().isdigit():
                        print("int attribute get char input")
                        return
                    dk[key] = int(values[i].strip())
                # char key
                else:
                    dk[key] = values[i].strip()
                # TODO: 插入改变索引
                # 若有索引
            if data_dict.index_name is not None:
                indexObj = self.pickle_load(data_dict.index_name)
                index_keys = indexObj.get_keys()
                for key in data_dict.get_keys():
                    if key in index_keys:
                        index_list = indexObj.attribute_list_index[key]
                        if dk[key] >= index_list[len(index_list) - 1][0]:
                            index_list.insert(len(index_list),
                                                [dk[key], dk])
                        else:
                            for i, item in enumerate(index_list):
                                if dk[key] <= item[0]:
                                    index_list.insert(i, [dk[key], dk])
                                    break
                self.pickle_dump(data_dict.index_name, indexObj)
        else:
            attribute_list = attribute_list.split(',')
            if len(attribute_list) != len(values):
                print("attribute {} but values is {}".format(
                    len(attribute_list),
                    len(values))
                )
                return
            for i, key in enumerate(attribute_list):
                key = key.strip()
                # primary key check
                if 'primary key' in data_dict.key_value[key]:
                    if 'int' in data_dict.key_value[key]:
                        for item in data_dict.dk_dict:
                            if item[key] == int(values[i].strip()):
                                print("primary key has existed")
                                return
                    else:
                        for item in data_dict.dk_dict:
                            if item[key] == values[i].strip():
                                print("primary key has existed")
                                return
                # int key check
                elif 'int' in data_dict.key_value[key]:
                    if not values[i].isdigit():
                        print("int attribute get char input")
                        return
                    dk[key] = int(values[i].strip())
                # char key
                else:
                    dk[key] = values[i].strip()
                    # TODO: 插入改变索引
                    # 若有索引
                if data_dict.index_name is not None:
                    indexObj = self.pickle_load(data_dict.index_name)
                    index_keys = indexObj.get_keys()
                    for key in attribute_list:
                        key = key.strip()
                        if key in index_keys:
                            index_list = indexObj.attribute_list_index[key]
                            if dk[key] >= index_list[len(index_list) - 1][0]:
                                index_list.insert(len(index_list),
                                                    [dk[key], dk])
                            else:
                                for i, item in enumerate(index_list):
                                    if dk[key] <= item[0]:
                                        index_list.insert(i, [dk[key], dk])
                                        break
                    self.pickle_dump(data_dict.index_name, indexObj)
        data_dict.dk_dict.append(dk)
        self.pickle_dump(table_name, data_dict)

    def delete(self, string):
        name = string.split()[2]
        goahead = False
        part_delete = False
        for i in os.listdir('.'):
            if i == name:
                goahead = True

        if not goahead:
            print("Table Doesn't Exist")
            return

        if len(string.split()) > 3:
            if string.split()[3] == "where":
                part_delete = True
        data_dict = self.pickle_load(name)

        if part_delete:
            if not len(string.split()) > 4:
                print("Command error")
            tmp = string.split()[4:]
            del_key_val = "".join(tmp)
            if ',' in del_key_val:
                pass
            else:
                del_key_val = del_key_val.split('=')
                if len(del_key_val) != 2:
                    print("command error")
                    return
                if del_key_val[0] in data_dict.get_keys():
                    for item in data_dict.dk_dict:
                        if item.get(del_key_val[0]) == del_key_val[1]:
                            data_dict.dk_dict.remove(item)
                else:
                    print("key doesn't exsit")
                    return
        else:
            # delete all data
            data_dict.clean_data()
        self.pickle_dump(name, data_dict)

    def update(self, string):
        # update a set key = value
        # update a set key = value where key = value
        name = string.split()[1]
        goahead = False
        for i in os.listdir('.'):
            if i == name:
                goahead = True

        if not goahead:
            print("Table Doesn't Exist")
            return

        data_dict = self.pickle_load(name)

        command = string.split()[3:]

        if "where" in command:
            tmp = " ".join(command)
            tmp = tmp.split("where")
            f_key = tmp[0].split('=')
            l_key = tmp[1].split('=')
            if len(f_key) != 2:
                print("command error")
            if f_key[0].strip() not in data_dict.get_keys():
                print("Error Key or Value you given")
                return
            if l_key[0].strip() not in data_dict.get_keys():
                print("Error Key or Value you given")
                return
            for item in data_dict.dk_dict:
                if item[l_key[0].strip()] == l_key[1].strip():
                    item[f_key[0].strip()] = f_key[1].strip()
        else:
            if '=' in command and len(command) == 3:
                if command[0] in data_dict.get_keys():
                    for item in data_dict.dk_dict:
                        item[command[0]] = command[2]
                else:
                    print("Error Key or Value you given")
                    return
            else:
                print("Command error")
                return
        self.pickle_dump(name, data_dict)

    def alter_add(self, string):
        # alter table a add key char/int primary key
        if len(string.split()) < 6:
            print("Command error")
            return
        name = string.split()[2]
        attr = string.split()[4]
        other = string.split()[5:]
        value = " ".join(other)
        goahead = False
        for i in os.listdir('.'):
            if i == name:
                goahead = True

        if not goahead:
            print("table doesn't exsit")
            return

        data_dict = self.pickle_load(name)

        if attr in data_dict.get_keys():
            print("attribute has been used")
            return

        data_dict.update_key(attr, value)

        self.pickle_dump(name, data_dict)

    def alter_drop(self, string):
        # alter table a drop key char/int primary key
        if len(string.split()) < 6:
            print("Command error")
            return
        name = string.split()[2]
        attr = string.split()[4]
        other = string.split()[5:]
        goahead = False
        for i in os.listdir('.'):
            if i == name:
                goahead = True

        if not goahead:
            print("table doesn't exsit")
            return

        data_dict = self.pickle_load(name)

        if attr not in data_dict.get_keys():
            print("attribute not in table")
            return

        data_dict.del_key(attr)

        for item in data_dict.dk_dict:
            try:
                del item[attr]
            except:
                pass

        self.pickle_dump(name, data_dict)

    def drop(self, string):
        name = string.split()[2].strip()
        try:
            os.remove(name)
        except Exception as e:
            raise e

if __name__ == '__main__':
    print("SQL PLUS")
    command = input('>>>')
    executor = SqlManager()
    while command != 'exit':
        try:
            executor.execute(command)
        except Exception as e:
            print(e)
        command = input('>>>')