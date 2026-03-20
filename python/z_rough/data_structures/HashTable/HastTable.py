"""Hash tables are one way and are deterministic (meaning we know the address so its very fast to retrieve)
Python has dictionary as a hash table

collision
if the address already has a value, you put the dict as a list of values

Linear probing
if the address is occupied, you check all subsequent addresses before inputting a value within it
"""

'''Building a Hash table'''


class HashTable:
    def __init__(self, size=7):
        self.data_map = [None] * size

    def print_table(self):
        for i, val in enumerate(self.data_map):
            print(i, ":", val)

    def __hash(self, key):
        my_hash = 0
        for letter in key:
            my_hash = (my_hash + ord(letter) * 23) % len(self.data_map)
        return my_hash

    def set_item(self, key, value):
        index = self.__hash(key)
        if self.data_map[index] == None:
            self.data_map[index] = []
        self.data_map[index].append([key, value])

    def get_item(self, key):
        index = self.__hash(key)
        if self.data_map[index] is not None:
            for i in range(len(self.data_map[index])):
                if self.data_map[index][i][0] == key:
                    return self.data_map[index][i][1]
        return None


ht = HashTable()
ht.set_item('bolts', 1400)
ht.set_item('washers', 50)
ht.set_item('lumber', 70)
ht.print_table()

print(ht.get_item('washers'))
