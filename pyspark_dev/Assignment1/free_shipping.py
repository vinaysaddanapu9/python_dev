
#Check eligibility for free shipping or discount
def check_eligibility(amount):
    if amount > 150:
        return "Eligible for discount"
    elif amount > 100:
        return "Qualifies for free shipping"
    else:
        return "No Eligible"

print("Check Eligibility for 120: ", check_eligibility(120))
print("Check Eligibility for 170: ", check_eligibility(170))