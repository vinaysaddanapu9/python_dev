
fruits_array = ["mango", "banana", "carrot", "apple"]
search_string = "mango"
result = ""

#1st Approach
if search_string in fruits_array:
    print("Found: "+ search_string)
else:
    print("Fruit not found")

#2nd Approach
found = False
for fruit in fruits_array:
    if fruit == search_string:
        found = True
        break

if found:
    print("Item Found")
else:
    print("Item not found")