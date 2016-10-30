import pickle
import os
import re
from collections import OrderedDict
from time import time
# config name: sql_user_config


class UserControl(object):
    def __init__(self):
        self._user_password = {'root': 'admin'}
        self._user_table = {'root': []}

    # when create user not create table
    def add_user(self, user, password, table=list()):
        self._user_password[user] = password
        self._user_table[user] = table
        # if table:
        #     self._user_table[user] = [table]
        # else:
        #     self._user_table[user] = table

    # when create table
    def add_table(self, user, table):
        self._user_table[user].append(table)

    @property
    def user_password(self):
        return self._user_password

    @property
    def user_table(self):
        return self._user_table


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
        self._user = None

    def __str__(self):
        return self.tableName

    @property
    def username(self):
        return self._user

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
    def __init__(self, user):
        self.user_now = user
        self.userObject = pickle_load('sql_user_config')

    def __del__(self):
        if self.user_now == 'root':
            pickle_dump('sql_user_config', self.userObject)

    def check_table(self, tableName):
        try:
            if self.userObject.user_table[self.user_now]:
                if tableName in self.userObject.user_table[self.user_now]:
                    return True
                else:
                    return False
        except Exception as e:
            print(e)

    def execute(self, string):
        command = string.split()
        if command[0] == "create" and command[1] == 'table':
            v = re.match(r'create\s+table\s+(\w+)', string)
            self.create(string)
        elif command[0] == "insert" and command[1] == 'into':
            v = re.match(
                r'insert\s+into\s+(\w+)\s+\((.*)\)\s+values\s*\((.*)\)'
                , string)
            try:
                self.insert(v.group(1), v.group(2), v.group(3))
            except:
                try:
                    v = re.match(r'insert\s+into\s+(\w+)\s+values\s*\((.*)\)'
                                 , string)
                    self.insert(v.group(1), None, v.group(2))
                except Exception as e:
                    print(e)
                    print("Command Error")

        elif command[0] == "delete" and command[1] == 'from':
            if 'where' in string:
                v = re.match(r'delete\s+from\s+(\w+)\s+where\s+(.*\S)'
                             , string)
                self.delete(v.group(1), v.group(2))
            else:
                v = re.match(r'delete\s+from\s+(\w+)', string)
                self.delete(v.group(1))

        elif command[0] == "update" and command[2] == 'set':
            if 'where' in string:
                v = re.match(
                    r'update\s+(\w+)\s+set\s+(\w+)\s+=\s+(\w+)\s+where\s+(.*\S)'
                    , string)
                self.update(v.group(1), v.group(2), v.group(3), v.group(4))
            else:
                v = re.match(r'update\s+(\w+)\s+set\s+(\w+)\s+=\s+(\w+)'
                             , string)
                self.update(v.group(1), v.group(2), v.group(3))

        elif command[0] == "select":
            if 'where' in command:
                v = re.match(r'select\s+(.*\S)\s+from\s+(.*\S)\s+where\s*(.*\S)'
                             , string)
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

        elif command[0] == "alter" and command[1] == 'table' \
                and command[3] == 'add':
            v = re.match(r'alter\s+table\s+(\w+)\s+add\s+(.*\S)', string)
            self.alter_add(string)
        elif command[0] == "alter" and command[1] == 'table' \
                and command[3] == 'drop':
            v = re.match(r'alter\s+table\s+(\w+)\s+drop\s+(.*\S)', string)
            self.alter_drop(string)
        elif command[0] == 'drop':
            v = re.match(r'drop\s+table\s+(\w+)', string)
            try:
                self.drop(v.group(1))
            except:
                print("command error")
                return
        elif command[0] == 'create' and command[1] == 'index':
            v = re.match(r'create\s+index\s+(\w+)\s+on\s+(.*\S)\s+\((.*)\)'
                         , string)
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
            result_list = self.deal_condition(condition_list, table_dict, True)
            print(result_list)
            if attr_list == "*":
                pass
            else:
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

    def deal_condition(self, condition_list, data_dict, multi_type=False):
        table_list = list()
        if 'or' in condition_list:
            or_condition_list = condition_list.split('or')
            for or_condition in or_condition_list:
                if 'and' in or_condition:
                    tmp_list = list()
                    and_condition_list = or_condition.split('and')
                    for i, and_condition in enumerate(and_condition_list):
                        if multi_type:
                            line_list = self.isValued2(and_condition, data_dict)
                        else:
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
                    if multi_type:
                        line_list = self.isValued2(or_condition, data_dict)
                    else:
                        line_list = self.isValued(or_condition, data_dict)
                    if line_list is None:
                        continue
                    table_list = combine_list(table_list, line_list)
        elif 'and' in condition_list:
            and_condition_list = condition_list.split('and')
            for i, and_condition in enumerate(and_condition_list):
                if multi_type:
                    line_list = self.isValued2(and_condition, data_dict)
                else:
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
            if multi_type:
                table_list = self.isValued2(condition_list, data_dict)
            else:
                table_list = self.isValued(condition_list, data_dict)
        return table_list

    def index_check_return(self, table):
        if table.index_name is None:
            return False
        index = self.pickle_load(table.index_name)
        return index.attribute_list_index

    def isValued2(self, condition, table_dict):
        v = re.match(r'\s*(\w+)\.(\w+)\s*(=|!=|>|<|>=|<=)\s*(\w+)\.(\w+|\d+)',
                     condition)
        try:
            f_table = v.group(1)
            s_table = v.group(4)
            f_key = v.group(2)
            s_key = v.group(5)
            operator = v.group(3)
            line_list = list()
            if (f_table not in table_dict.keys()
                or s_table not in table_dict.keys()):
                print("table name not matching")
                return None
            if (f_key not in table_dict[f_table].key_value.keys()
                or s_key not in table_dict[s_table].key_value.keys()):
                print("table not have attribute you given")
                return None
            if (table_dict[f_table].key_value[f_key]
                not in table_dict[s_table].key_value[s_key]
                or table_dict[s_table].key_value[s_key]
                not in table_dict[f_table].key_value[f_key]):
                print("attributes has different type")
                return None
            f_index_in = self.index_check_return(table_dict[f_table])
            s_index_in = self.index_check_return(table_dict[s_table])
            if f_index_in:
                if f_key not in f_index_in.keys():
                    f_index_in = False
            if s_index_in:
                if s_key not in s_index_in.keys():
                    s_index_in = False
            if operator == '=':
                for f_item in table_dict[f_table].dk_dict:
                    for s_item in table_dict[s_table].dk_dict:
                        if f_item[f_key] == s_item[s_key]:
                            line_list.append((table_dict[f_table].tableName,
                                              f_item))
                            line_list.append((table_dict[s_table].tableName,
                                              s_item))
            elif operator == '!=':
                pass
            elif operator == '>':
                for f_item in table_dict[f_table].dk_dict:
                    for s_item in table_dict[s_table].dk_dict:
                        if f_item[f_key] > s_item[s_key]:
                            line_list.append((table_dict[f_table].tableName,
                                              f_item))
                            break
            elif operator == '<':
                for f_item in table_dict[f_table].dk_dict:
                    for s_item in table_dict[s_table].dk_dict:
                        if f_item[f_key] < s_item[s_key]:
                            line_list.append((table_dict[f_table].tableName,
                                              f_item))
                            break
            elif operator == '>=':
                for f_item in table_dict[f_table].dk_dict:
                    for s_item in table_dict[s_table].dk_dict:
                        if f_item[f_key] >= s_item[s_key]:
                            line_list.append((table_dict[f_table].tableName,
                                              f_item))
                            break
            elif operator == '<=':
                for f_item in table_dict[f_table].dk_dict:
                    for s_item in table_dict[s_table].dk_dict:
                        if f_item[f_key] >= s_item[s_key]:
                            line_list.append((table_dict[f_table].tableName,
                                              f_item))
                            break
            return line_list
        except Exception as e:
            print(e)
            return None

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
                if value.isdigit():
                    value = int(value)
                line_list = list()
                index_in = self.index_check_return(data_dict)
                if index_in:
                    if key not in index_in.keys():
                        index_in = False
                if operator == '=':
                    if index_in:
                        datas = index_in[key]
                        for i, data in enumerate(datas):
                            if data[0] == value:
                                line_list.append((data_dict.tableName, data[1]))
                                if datas[i + 1] != value:
                                    break

                    else:
                        for item in data_dict.dk_dict:
                            if item[key] == value:
                                line_list.append((data_dict.tableName, item))
                elif operator == '!=':
                    for item in data_dict.dk_dict:
                        if item[key] != value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '>':
                    if index_in:
                        datas = index_in[key]
                        for i, data in enumerate(datas):
                            if data[0] > value:
                                line_list.append((data_dict.tableName, data[1]))
                                break
                    else:
                        for item in data_dict.dk_dict:
                            if item[key] > value:
                                line_list.append((data_dict.tableName, item))
                elif operator == '<':
                    for item in data_dict.dk_dict:
                        if item[key] < value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '>=':
                    for item in data_dict.dk_dict:
                        if item[key] >= value:
                            line_list.append((data_dict.tableName, item))
                elif operator == '<=':
                    for item in data_dict.dk_dict:
                        if item[key] <= value:
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
        is_user_table = self.check_table(table_name)
        if not is_user_table:
            print("You do not have permission to make changes to the table!")
            return
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
                if ('not null' in data_dict.key_value[key]
                    and values[i].strip() == 'null'):
                    print("{} key could not be null".format(key))
                    return
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
                if ('not null' in data_dict.key_value[key]
                    and values[i].strip() == 'null'):
                    print("{} key could not be null".format(key))
                    return
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

    def delete(self, table_name, condition_list=None):
        is_user_table = self.check_table(table_name)
        if not is_user_table:
            print("You do not have permission to make changes to the table!")
            return
        goahead = False
        for i in os.listdir('.'):
            if i == table_name:
                goahead = True
        if not goahead:
            print("Table Doesn't Exist")
            return

        data_dict = self.pickle_load(table_name)
        index_in = self.index_check_return(data_dict)

        if condition_list is not None:
            result_list = self.deal_condition(condition_list, data_dict)
            if result_list:
                if index_in:
                    for item in result_list:
                        data_dict.dk_dict.remove(item[1])
                        for key, value in index_in.items():
                            for i in value:
                                if i[1] == item[1]:
                                    value.remove(i)
                    index_dump = IndexDict()
                    index_dump._attribute_list_index = index_in
                    self.pickle_dump(data_dict.index_name, index_dump)
                else:
                    for item in result_list:
                        data_dict.dk_dict.pop(item[1])
        else:
            # delete all data
            data_dict.clean_data()
            if index_in:
                try:
                    os.remove(data_dict.index_name)
                    data_dict.index_name = None
                except Exception as e:
                    raise e
        self.pickle_dump(table_name, data_dict)

    def update(self, table_name, attribute, value, condition_list=None):
        # update a set key = value
        # update a set key = value where key = value
        is_user_table = self.check_table(table_name)
        if not is_user_table:
            print("You do not have permission to make changes to the table!")
            return
        goahead = False
        for i in os.listdir('.'):
            if i == table_name:
                goahead = True
        if not goahead:
            print("Table Doesn't Exist")
            return

        data_dict = self.pickle_load(table_name)
        index_in = self.index_check_return(data_dict)

        if attribute not in data_dict.get_keys():
            print("bad key you given!")
            return
        elif ('primary key' in data_dict.key_value[attribute]
              and value == 'null'):
            print("primary key couldn't be null type")
            return
        elif ('not null' in data_dict.key_value[attribute] and
              value == 'null'):
            print("{} has set not null, value couldn't null"
                  .format(attribute))
            return

        if condition_list is not None:
            result_list = self.deal_condition(condition_list, data_dict)
            if result_list:
                if index_in and attribute in index_in.keys():
                    for item in result_list:
                        for i, line in enumerate(data_dict.dk_dict):
                            if line == item[1]:
                                data_dict[i][attribute] = value
                        for key, value in index_in.items():
                            for i in value:
                                if i[1] == item[1]:
                                    # value.remove(i)
                                    pass
                    index_dump = IndexDict()
                    index_dump._attribute_list_index = index_in
                    self.pickle_dump(data_dict.index_name, index_dump)
                else:
                    for item in result_list:
                        for i, line in enumerate(data_dict.dk_dict):
                            if line == item[1]:
                                data_dict[i][attribute] = value
        else:
            if 'primary key' not in data_dict.key_value[attribute]:
                for item in data_dict.dk_dict:
                    item[attribute] = value
                # 索引
                if index_in and attribute in index_in.keys():
                    for sql_line in index_in[attribute]:
                        sql_line[0] = value
                    for key, value in index_in:
                        for line in value:
                            line[1][attribute] = value
            else:
                print("You couldn't update the same value of all  primary key")
                return
        self.pickle_dump(table_name, data_dict)

    # 未更改
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

    # 未更改
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

    # 未更改
    def drop(self, table_name):
        try:
            os.remove(table_name)
        except Exception as e:
            raise e


def pickle_load(f_name):
    with open(f_name, 'rb') as file:
        return pickle.load(file)


def pickle_dump(f_name, obj):
    with open(f_name, 'wb') as file:
        pickle.dump(obj, file)


def check():
    username = input("DB Username: ")
    password = input("Password: ")
    userObject = pickle_load('sql_user_config')
    if username not in userObject.user_password.keys():
        print("User not exist!")
        return False
    if userObject.user_password[username] != password:
        print("Wrong password!")
        return False
    else:
        print("Login success!")
        return username

if __name__ == '__main__':
    print("SQL PLUS")
    username = check()
    if username:
        command = input('>>>')
        executor = SqlManager(username)
        while command != 'exit':
            try:
                executor.execute(command)
            except Exception as e:
                print(e)
            command = input('>>>')