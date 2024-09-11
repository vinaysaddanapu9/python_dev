#Maximum element in an array
numbers_double  = [1.0, 10.0, 70.0, 20.0, 50.0]

max_element = numbers_double[0]

for num in numbers_double:
    if num > max_element:
        max_element = num

print("Maximum element in array: ",max_element)