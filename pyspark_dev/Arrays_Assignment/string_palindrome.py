#Check for the Palindrome

def reverse_string(str):
    reversed_string = ""
    for char in str[::-1]:
        reversed_string = reversed_string + char
    return reversed_string

str1= "dad"
rev_string = reverse_string(str1)

if rev_string == str1:
    print("It's a Palindrome")
else:
    print("It's not a palindrome")

