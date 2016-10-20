import os
import re


class Executor(object):
    def __init__(self):
        self.command = None

    def execute(self, string):
        command = string.split()
        if command[0] == "create" and command[1] == 'table':
            self.create(string)
        elif command[0] == "insert" and command[1] == 'into':
            self.insert(string)
        elif command[0] == "delete" and command[1] == 'from':
            self.delete(string)
        elif command[0] == "update" and command[2] == 'set':
            self.update(string)
        elif command[0] == "select" and command[2] == 'from':
            self.select(string)
        elif command[0] == "alter" and command[1] == 'table' and command[3] == 'add':
            self.alter_add(string)
        elif command[0] == "alter" and command[1] == 'table' and command[3] == 'drop':
            self.alter_drop(string)
        elif command[0] == 'drop':
            self.drop(string)
        else:
            print("Error Command")

    def create(self, string):
        name = string.split()[2]
        try:
            command = re.findall(r'{(.+?)}', string)[0]
        except:
            print("Command Error")
            return

        command = command.split(',')
        for i in os.listdir('.'):
            if i == name + '.db':
                print("Table Exist")
                return

        with open(name + '.db', 'w') as f1:
            for att in command:
                f1.write(att.strip() + '\n')
        with open(name + '.cvs', 'w') as f2:
            for att in command:
                f2.write(att.split()[0] + '\t')
            f2.write('\n')

    def insert(self, string):
        name = string.split()[2]
        try:
            command = re.findall(r'\((.+?)\)', string)[0]
        except:
            print("Command Error")
            return

        command = command.split(',')
        check_index = 0
        with open(name + '.db', 'r') as f1:
            with open(name + '.cvs', 'r') as f2:
                for line in f1:

                    # Check primary key
                    if "primary key" in line:
                        for data in f2:
                            key = data.split('\t')[check_index]
                            if key == command[check_index].strip():
                                print("Key already been use in table")
                                return

                    # Check Type
                    if line.split()[1] == 'char':
                        pass
                    if line.split()[1] == 'int':
                        if not command[check_index].isdigit():
                            print("int type not clearly")
                            return

                    check_index += 1
                if len(command) < check_index:
                    print("insert value too less")
                    return
                elif len(command) > check_index:
                    print("given too many value")
                    return

        with open(name + '.cvs', 'a') as f:
            for value in command:
                f.write(value.strip() + '\t')
            f.write('\n')

    def delete(self, string):
        name = string.split()[2]
        goahead = False
        part_delete = False
        for i in os.listdir('.'):
            if i == name + '.db':
                goahead = True

        if not goahead:
            print("Table Doesn't Exist")
            return

        if len(string.split()) > 3:
            if string.split()[3] == "where":
                part_delete = True

        if part_delete:
            tmp = string.split()
            command = str()
            for i, line in enumerate(tmp):
                if i > 3:
                    command += line
            if ',' in command:
                # TODO:
                pass
            else:
                command = command.split('=')
                lineno = 1
                lie = 0
                with open(name + '.cvs', 'r') as f:
                    att = f.readline().strip('\n').split('\t')
                    for i in att:
                        if i == command[0]:
                            break
                        lie += 1
                    for line in f:
                        if line.split()[lie] == command[1] and lineno != 0:
                            break
                        lineno += 1
                removeLine(name + '.cvs', lineno)
        else:
            with open(name + '.cvs', 'r') as f:
                head_line = f.readline()
            head_line.strip('\n')
            with open(name + '.cvs', 'w') as f2:
                f2.write(head_line)

    def update(self, string):
        name = string.split()[1]
        goahead = False
        for i in os.listdir('.'):
            if i == name + '.db':
                goahead = True

        if not goahead:
            print("table doesn't exsit")
            return

        command = string.split()[3:]
        if '=' in command[0]:
            attr = command[0].split('=')[0]
        else:
            attr = command[0]

        update_index = 0

        with open(name + '.db', 'r') as f:
            for line in f:
                if line.split()[0] == attr:
                    break
                update_index += 1

        value = string.split('where')[0].split('=')[1].strip()

        if 'where' in string:
            que = string.split('where')[1]
            que = que.split('=')
            index = 0
            lineno_list = list()
            with open(name + '.cvs', 'r') as f:
                lineno = 1
                head_line = f.readline()
                for i in head_line.strip('\n').split('\t'):
                    if i == que[0].strip():
                        break
                    index += 1
                for line in f:
                    if line.strip('\n').split('\t')[index] == que[1].strip() and lineno != 0:
                        lineno_list.append(lineno)
                    lineno += 1
            for i in lineno_list:
                moveLine(name + '.cvs', i, value, update_index)
        else:
            line_count = 0
            with open(name + '.cvs', 'r') as f:
                for i in f:
                    line_count += 1
            lineno = 1
            while lineno < line_count:
                moveLine(name + '.cvs', lineno, value, update_index)
                lineno += 1

    def select(self, string):
        name = string.split()[3]
        goahead = False
        for i in os.listdir('.'):
            if i == name + '.db':
                goahead = True

        if not goahead:
            print("table doesn't exsit")
            return

        if 'where' in string:
            que = string.split('where')[1]
            que = que.split('=')
            index = 0
            lineno_list = list()
            with open(name + '.cvs', 'r') as f:
                lineno = 1
                head_line = f.readline()
                print(head_line.strip('\n'))
                for i in head_line.strip('\n').split('\t'):
                    if i == que[0].strip():
                        break
                    index += 1
                for line in f:
                    if line.strip('\n').split('\t')[index] == que[1].strip() and lineno != 0:
                        lineno_list.append(lineno)
                    lineno += 1

            num = 0
            with open(name + '.cvs', 'r') as f:
                for line in f:
                    if num in lineno_list:
                        print(line.strip('\n'))
                    num += 1
        else:
            with open(name + '.cvs', 'r') as f2:
                for line in f2:
                    print(line.strip('\n'))

    def alter_add(self, string):
        name = string.split()[2]
        attr = string.split()[4]
        other = string.split()[5:]
        with open(name + '.cvs', 'r') as f:
            line = f.readline()
            if attr.strip() in line.split('\t'):
                print("Attr already been use")
                return

        fro = open(name + '.cvs', "r")

        current_line = 0
        while current_line < 0:
            fro.readline()
            current_line += 1

        seekpoint = fro.tell()
        frw = open(name + '.cvs', "r+")
        frw.seek(seekpoint, 0)

        # read the line we want to discard
        changeLine = fro.readline()
        changeLine = changeLine.strip('\n') + attr.strip() + '\t\n'
        frw.write(changeLine)

        # now move the rest of the lines in the file
        # one line back
        chars = fro.readline()
        while chars:
            frw.writelines(chars)
            chars = fro.readline()

        fro.close()
        frw.truncate()
        frw.close()

        with open(name + '.db', 'a') as f:
            f.write(attr + ' ')
            for i in other:
                f.write(i + ' ')
            f.write('\n')

    def alter_drop(self, string):
        name = string.split()[2]
        attr = string.split()[4]
        other = string.split()[5:]
        with open(name + '.cvs', 'r') as f:
            line = f.readline()
            if attr.strip() not in line.split('\t'):
                print("Attr doesn't exsit")
                return

        index_lineno = 0
        with open(name + '.db', 'r') as f:
            for line in f:
                if line.split()[0] == attr.strip():
                    break
                index_lineno += 1
        removeLine(name + '.db', index_lineno)

        line_count = 0
        with open(name + '.cvs', 'r') as f:
            for i in f:
                line_count += 1

        lineno = 0
        while lineno < line_count:
            deleteCow(name + '.cvs', lineno, index_lineno)
            lineno += 1

    def drop(self, string):
        command = string.split()[2].strip()
        try:
            os.remove(command + '.cvs')
            os.remove(command + '.db')
        except Exception as e:
            raise e


def removeLine(filename, lineno):
    fro = open(filename, "r")

    current_line = 0
    while current_line < lineno:
        fro.readline()
        current_line += 1

    seekpoint = fro.tell()
    frw = open(filename, "r+")
    frw.seek(seekpoint, 0)

    # read the line we want to discard
    fro.readline()

    # now move the rest of the lines in the file
    # one line back
    chars = fro.readline()
    while chars:
        frw.writelines(chars)
        chars = fro.readline()

    fro.close()
    frw.truncate()
    frw.close()


def moveLine(filename, lineno, value, index):
    fro = open(filename, "r")

    current_line = 0
    while current_line < lineno:
        fro.readline()
        current_line += 1

    seekpoint = fro.tell()
    frw = open(filename, "r+")
    frw.seek(seekpoint, 0)

    # read the line we want to discard
    changeLine = fro.readline()
    tmp = changeLine.strip('\n').split('\t')
    tmp[index] = value
    changeLine = ''
    for i in tmp:
        changeLine = changeLine + i +'\t'
    changeLine += '\n'
    frw.write(changeLine)

    # now move the rest of the lines in the file
    # one line back
    chars = fro.readline()
    while chars:
        frw.writelines(chars)
        chars = fro.readline()

    fro.close()
    frw.truncate()
    frw.close()


def deleteCow(filename, lineno, index):
    fro = open(filename, "r")

    current_line = 0
    while current_line < lineno:
        fro.readline()
        current_line += 1

    seekpoint = fro.tell()
    frw = open(filename, "r+")
    frw.seek(seekpoint, 0)

    # read the line we want to discard
    changeLine = fro.readline()
    tmp = changeLine.strip('\n').split('\t')
    tmp.pop(index)
    # tmp.pop()
    changeLine = ''
    for i in tmp:
        changeLine = changeLine + i + '\t'
    changeLine += '\n'
    frw.write(changeLine)

    # now move the rest of the lines in the file
    # one line back
    chars = fro.readline()
    while chars:
        frw.writelines(chars)
        chars = fro.readline()

    fro.close()
    frw.truncate()
    frw.close()

if __name__ == '__main__':
    print("SQL PLUS")
    command = input('>>>')
    executor = Executor()
    while command != 'exit':
        try:
            executor.execute(command)
        except Exception as e:
            print(e)
        command = input('>>>')