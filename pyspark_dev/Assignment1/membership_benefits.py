
def check_eligibility_for_membership(amount):
    if amount > 200:
        return "Eligible for discount"
    else:
        return "Qualifies for membership benefits"


print("Amount 300:" ,check_eligibility_for_membership(300))
print("Amount 100:", check_eligibility_for_membership(100))