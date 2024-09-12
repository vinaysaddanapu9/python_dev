count = 0
def count_vowels(str):
    global count
    for char in str:
        if(char == "a" or char == "e" or char == "i"
        or char == "o" or char == "u"):
            count = count + 1
    return count

str = "quicka"
print(f"No of vowels for {str}: ", count_vowels(str))
