
def remove_even_numbers(array_num):
    filtered_array = []
    for num in array_num:
        if num%2 != 0:
            filtered_array.append(num)
    return filtered_array

numbers_array = [2,4,7,8,9,10,11,15]
odd_array = remove_even_numbers(numbers_array)
print("Removed the even numbers in array:", odd_array)