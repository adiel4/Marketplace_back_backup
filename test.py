a = '16'
print([int(i) for i in a.replace('[','').replace(']','').split(',')])