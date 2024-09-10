
def check_divisibility_5_and_7(num):
    if num%5 == 0 and num%7 == 0:
        return True
    else:
        return False

print("Check Divisibility by 5 and 7: ", check_divisibility_5_and_7(35))