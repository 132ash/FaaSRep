d1 = {'k1':{'f1':2}, 'k2':{'f2':2}}
d2 = {'k1':{'f1':3}, 'k3':{'f3':2}}

self_d = d1
d1.update(d2)
print(d1)