import re
from tempfile import tempdir


class Node:
    def __init__(self, value):
        self.value=value
        self.next=None
class Queue:
    def __init__(self, value):
        new_node=Node(value)
        self.first=new_node
        self.last=new_node
        self.length=1
    def print_queue(self):
        temp =self.first
        while temp:
            print(temp.value)
            temp=temp.next
        print('length of the queue is :',self.length)
    def enqueue(self, value):
        new_node=Node(value)
        if self.length==0:
            self.first=new_node
            self.last=new_node
            return True
        temp = self.last
        temp.next=new_node
        self.last=new_node
        self.length+=1
        return True
    def dequeue(self):
        if self.length==0:
            return False
        temp=self.first
        if self.length==1:
            self.first=None
            self.last=None
            return temp
        else:
            self.first=temp.next
            temp.next=None
        self.length-=1
        return temp

q=Queue(4)
q.enqueue(5)
q.enqueue(6)
q.enqueue(7)
q.dequeue()
q.dequeue()
q.print_queue()


    