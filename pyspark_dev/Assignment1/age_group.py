
def age_group_classification(age):
    if age > 20:
        return "Adult"
    elif age > 13 and age < 19:
        return "Teenager"
    else:
        return "Child"

#Call the function
print(age_group_classification(15))




