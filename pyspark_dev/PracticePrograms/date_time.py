#Date and Time Functions
from datetime import datetime

#Current datetime
now = datetime.now()
print(now)

#Date
current_date = now.date()
print("Date: ", current_date)

#Time
current_time = now.time()
print("Time: ", current_time)

#Year, Month, Day
year = now.year
month = now.month
day = now.day

#Print
print("Year: ", year)
print("Month: ", month)
print("Day: ", day)