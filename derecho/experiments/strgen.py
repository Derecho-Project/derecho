from random import choice
from string import hexdigits, punctuation
from os import makedirs

def mkstr(nodes, num, length):
  makedirs('RandomStrings', exist_ok = True)
  for i in range(nodes):
    with open(f"RandomStrings/node{i}.txt", 'w') as f:
      for j in range(num):
        x = len(str(num-1))
        f.write(str(j).zfill(x) + ' ' + ''.join([choice(hexdigits + punctuation) for _ in range(length-(x+1))]) + '\n')

def main():
  nodes = 0
  num = 0
  length = 0
  while nodes <= 0:
    try:
      nodes = int(input("Please enter the number of files to create.\n"))

    except ValueError:
        print("Invalid number.\n")

  while num <= 0:
    try:
      num = int(input("Please enter the number of strings in each file.\n"))

    except ValueError:
        print("Invalid number.\n")

  while length <= 0:
    try:
      length = int(input("Please enter the length of each string.\n"))

    except ValueError:
      print("Invalid length.\n")
      
  mkstr(nodes, num, length)

if __name__ == '__main__': main()