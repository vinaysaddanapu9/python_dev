#Print the sentence in the reverse order

str = "The quick brown fox jumps over the lazy dog"

str_array = str.split(" ")
reversed_sentence = ""

for word in str_array[::-1]:
    reversed_sentence = reversed_sentence +" "+word

print(reversed_sentence)

