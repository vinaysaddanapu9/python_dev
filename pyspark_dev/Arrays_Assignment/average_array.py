
numbers_array = [1,2,3,4,5]
array_size = len(numbers_array)

array_sum = 0
#print(array_size)

for num in numbers_array:
    array_sum = array_sum + num

print("Average of elements in array: ", (array_sum/array_size))