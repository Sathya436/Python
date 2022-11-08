""""
# Method 1
a = input("Enter String ")
changed = ""
for i in a:
    changed = i + changed
print(changed)
"""
"""
# Method 2
a = input("Enter the string ")
b = a[::-1]
print(b)
"""
"""
# Reverse each word in a sentence
a = input("Enter Sentence :")
b = ""
rev = ""
if len(a) == 1:
    print(a)
else:
    b = a.split()
    for i in b:
        rev = rev + " " + i[::-1] 
print(rev)
"""