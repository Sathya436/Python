from datetime import date, datetime

"""
a = date.today()
print(a.today())
print(a.day)
print(a.month)
print(a.year)
b = date(2000, 5, 29)
c = a - b
print(c)
"""
a = datetime.now()
print(a.today())
b = datetime(2000, 5, 29, 17, 43, 22)
c = a - b
print(c)
