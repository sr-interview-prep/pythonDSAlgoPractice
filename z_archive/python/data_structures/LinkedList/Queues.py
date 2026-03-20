'queue is append and pop_first'
'enqueue, dequeue,print_queue'


class Node:
    def __init__(self, value):
        self.value = value
        self.next = None


class Queue:
    def __init__(self, value):
        new_node = Node(value)
        self.first = new_node
        self.last = new_node
        self.length = 1

    def enqueue(self, value):
        new_node = Node(value)
        if self.length == 0:
            self.first = new_node
            self.last = new_node
        else:
            self.last.next = new_node
            self.last = new_node
        self.length += 1
        return True

    def dequeue(self):
        if self.length == 0:
            return False
        temp = self.first
        if self.length == 1:
            self.first = None
            self.last = None
        else:
            after = temp.next
            temp.next = None
            self.first = after
        self.length -= 1
        return temp

    def print_queue(self):
        temp = self.first
        for _ in range(self.length):
            print(temp.value)
            temp = temp.next


q = Queue(4)
q.enqueue(5)
q.enqueue(6)
q.enqueue(7)
q.dequeue()
q.dequeue()
q.enqueue(9)
q.dequeue()
q.dequeue()
q.print_queue()
