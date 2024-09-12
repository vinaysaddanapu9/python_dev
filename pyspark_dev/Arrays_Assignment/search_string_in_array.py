#Search for the string in an array
def search_string(str, search_str):
    str_array = str.split(" ")
    is_found = False
    for word in str_array:
        if search_str == word:
            is_found = True
    return is_found


str =  "The quick brown fox jumps over the lazy dog"
str1 = "brown"
if search_string(str, str1):
    print("Found:", str1)
else:
    print("Not Found")








