
def check_eligibility_for_voting_driving(age):
    if age >= 18:
        return "Eligible to vote"
    elif age >= 16:
        return "Eligible to drive"
    else:
        return "No Eligibility"


print(check_eligibility_for_voting_driving(20))
