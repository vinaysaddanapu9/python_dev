

def check_divisibility_4_or_6(num):
    if num%4 == 0 or num%6 == 0:
        return True
    else:
        return False


print("Check divisibility by 4 or 6: ", check_divisibility_4_or_6(18))
print("Check divisiblity by 4 or 6: ", check_divisibility_4_or_6(20))