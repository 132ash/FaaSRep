class t1:
    def __init__(self, dict):
        self.write_set = dict['write_set']

    def add(self, key, value):
        self.write_set[key] = value

dic = {'write_set':{}}

t  = t1(dic)
t.add('a', 1)
print(dic)