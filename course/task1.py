import sys
import logging
import time
from multiprocessing import Process
#logging.basicConfig(filename='test.log', level=logging.DEBUG, \
#format='%(asctime)s %(name)s %(levelname)s %(message)s', filemode='w')
#CTRL + J
#import pytest
#main = __main__
import sys # Python CLI 

print(__name__)

#import multiprocessing
#print(Process.cpu_count())

def sleepy_man(p):
  print(f'process {p} Starting to sleep')
  time.sleep(1)
  print(f'process {p} Done to sleep')

if __name__ == "__main__":
  tic = time.time()
  p1 = Process(target=sleepy_man, args=(1,))
  p2 = Process(target=sleepy_man, args=(2,))
  p1.start()
  p2.start()
  p1.join()
  p2.join()
  toc = time.time()
  print(f'{toc-tic}')
  print('Done in {:.4f} seconds'.format(toc-tic))
    
  


""""

def myfunc():
  x = 5/0
  return x

print(myfunc())
logging.debug("Debug message")


import yaml
with open("b.yaml", mode='r') as file:
  print(yaml.safe_load(file))



import pandas as pd
df = pd.read_json("https://api.coinbase.com/v2/currencies")
df = df["data"]
df.to_csv('coins.csv')
print(df)



import requests
import csv
#response = requests.get("http://www.google.com")
response = requests.get("https://api.coinbase.com/v2/currencies")
j = response.json()["data"]
#print(str(response.status_code), "\n", response.headers)
with open ("coins.csv", 'w', encoding='UTF-8') as f:
  writer = csv.DictWriter(f, fieldnames=j[0].keys())
  writer.writeheader()
  for i in j:
    writer.writerow(i)



import csv
with open("/Users/victor.basin/Downloads/list.csv", 'w') as f:
  reader = csv.DictReader(f)
  print(sum(int(i['scors']) for i in reader))
  li = ['john', 'cohen', 80]
  writer = csv.writer(f)
  writer.writerow(li)



import os
def merge(file1, file2):
  with open("merge.txt", 'w') as f_merge:
    with open(file1) as f1:
      f_merge.write(f1.read())
    with open(file2) as f2:
      f_merge.write(f2.read())

merge("a.txt", "b.txt") 

# def zip_merge(file1, file2):
#   with open("merge.txt", 'w') as f_merge:
#     z = zip(open(file1), open(file2))


import pathlib
desktop = pathlib.Path(r"/Users/victor.basin/")
print(desktop.rglob("*"))
#print('\n'.join([str(item)]))
print([item for item in desktop.glob("*")])


f = open("a.txt")
for line in f:
  #print the bigest string in the file
  print(max(f, key = len))
"""




# set1 = {1,2,3,4,5}
# set2 = {3,4,5,6,7,8}

# print(set1.intersection(set2))
# print(set1.difference(set2))
# print(set1.symmetric_difference(set2))

# from functools import reduce
# l1 = [-2,4,5,4,-1,0,7,3,-6]
# x1 = list(filter(lambda x : x>0, l1))
# x2 = list(map(lambda x : x**2, filter(lambda x : x%2!=0, l1)))
# x= reduce(lambda x,y:x*y , [3,4,5,6,8,])
# print(f"lambda help {x2}") #[25, 1, 49, 9]
# print(str(x))

# x = [1,2,3]
# y = [54,34,80]
# print(list(zip(x,y))) #[(1, 54), (2, 34), (3, 80)]

# def decorator (funk):
#     def inner():
#      print("a")
#     return inner

# def simple():
#   print("i am simple")

# simple = decorator(simple)
# simple()

# for n in range(2,10):
#   for x in range(2,n):
#     if n%x == 0:
#       print(f"{n}%{x}=0")
#       break
# else:
#     print(f"{n} is primary")



# def numbers():
#   x=1
#   yield x
#   x+=1
#   yield x

# x = numbers()
# print(next(x))
# print(next(x))

