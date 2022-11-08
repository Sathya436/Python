a = input("Enter string : ")
vow = 0
con = 0
b = a.lower()
for i in b:
    if i == 'a' or i == 'e' or i == 'i' or i == 'o' or i == 'u':
        vow = vow + 1
    else:
        con = con + 1
print("No of vowels = " + str(vow))
print("No of consonants = " + str(con))
