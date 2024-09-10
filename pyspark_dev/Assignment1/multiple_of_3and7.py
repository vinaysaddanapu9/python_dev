
def check_multiple(num):
    if num%3 == 0 and num%7 == 0:
        return True
    else:
        return False


print("Multiple of 3 and 7: ",check_multiple(21))