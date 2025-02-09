import random
import string
from collections import Counter
import pickle

def generate_random_cusip():
    def calculate_check_digit(cusip):
        # Convert characters to values (A=10, B=11, ..., Z=35)
        def char_to_value(char):
            if char.isdigit():
                return int(char)
            else:
                return ord(char) - 55  # 'A' is 65 in ASCII, so 65 - 55 = 10

        # Convert CUSIP to a list of integers
        values = [char_to_value(char) for char in cusip]

        # Double the values at odd indices (0-based)
        for i in range(len(values)):
            if i % 2 == 1:
                values[i] *= 2

        # If doubling results in a number >= 10, subtract 9 from it
        values = [value - 9 if value >= 10 else value for value in values]

        # Sum all values
        total = sum(values)

        # The check digit is the number that, when added to the total, makes it a multiple of 10
        check_digit = (10 - (total % 10)) % 10

        return check_digit

    # Generate the first 8 characters randomly (alphanumeric)
    cusip_body = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

    # Calculate the check digit
    check_digit = calculate_check_digit(cusip_body)

    # Return the full CUSIP
    return cusip_body + str(check_digit)

def generate_multiple_cusips(n):
    cusips = [generate_random_cusip() for _ in range(n)]
    return cusips

# Generate and print a list of random CUSIPs (with possible duplicates)
num_cusips = 10000  # Adjust this number as needed
random_cusips = generate_multiple_cusips(num_cusips)

# Print the generated CUSIPs and their counts
cusip_counts = Counter(random_cusips)
for cusip, count in cusip_counts.items():
    print(f'{cusip}: {count}')





# Serialize the object to a file
with open('/Users/anujwalia/PycharmProjects/DataEngineering/data/cusips.pickle', 'wb') as f:
    pickle.dump(random_cusips, f)

# De-serialize the object from the file
with open('/Users/anujwalia/PycharmProjects/DataEngineering/data/cusips.pickle', 'rb') as f:
    loaded_object = pickle.load(f)

# Check that the de-serialized object is the same as the original object
print(random_cusips == loaded_object) # Output: True



