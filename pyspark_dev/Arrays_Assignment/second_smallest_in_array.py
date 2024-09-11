from pyspark.sql.functions import second


def retrieve_second_smallest_in_array(num_array):
    numbers_array.sort()
    smallest = num_array[0]
    second_smallest = num_array[1]
    for num in num_array:
        if  num != smallest and num < second_smallest:
            second_smallest = num
    return second_smallest


numbers_array = [3,2,19,15,6,8,9,10]
second_smallest_num = retrieve_second_smallest_in_array(numbers_array)
print("Second smallest number in array: ",second_smallest_num)



