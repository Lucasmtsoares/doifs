b = ['bolo','torta','pao','doce']

v = ['doce', 'sal', 'bis', 'jujuba']

f = []
for i in b:
    for n in v:
        if i == n:
            f.append(n)
        else:
            f.append(i)
            
for t in f:
    print(t)