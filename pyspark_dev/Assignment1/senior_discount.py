
def check_eligibility_for_discount(age):
    if age > 60:
        return "Senior Citizen Discount"
    elif age < 25:
        return "Student discount"
    else:
        return "No Eligibility"


print("Age 75:", check_eligibility_for_discount(75))
print("Age 20:", check_eligibility_for_discount(20))