"""
a = input("Enter the sentence :")
b = 0
c = ''
d = a.split()
for i in d:
    if len(i) > b:
        c = i
        b = len(i)
print("Longest word in a String is : " + c)
print("Length of longest word is : " + str(b))
"""
