import random
import string


def generate_random_id(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
