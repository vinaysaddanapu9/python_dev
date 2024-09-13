#Take input the size and elements for array
size = int(input("Enter the size of an array:"))
array = [0] * size

for index, value in enumerate(array):
    array[index] = int(input("Enter the element"))


print("Elements of array are: ", array)
