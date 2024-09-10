
def check_odd_number(num):
    if num%2 != 0 and num%3 != 0:
        return True
    else:
        return False

print("Check odd number and not divisible by 3: ", check_odd_number(27))