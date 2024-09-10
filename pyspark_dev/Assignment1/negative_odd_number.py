
def check_negative_odd_number(num):
    if num < 0 and num%2 != 0:
        return True
    else:
        return False

print("Check Negative for Odd Number: ", check_negative_odd_number(-7))
