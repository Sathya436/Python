"""
# Method 1:
s = input("Enter a string")
space = ''
for char in s:
    if char != ',':
        space = space+char
print(space)
"""
"""
# Method 2
s = input("Enter a string ")
Modified_string = s.replace(',', ' ')
print("Modified String"+Modified_string)
"""



