# Write a Double linked list with the below functions
'''
append, prepend, pop, pop_first, get, set_value, insert, remove, 
print_list, reverse
'''
class Node:
    def __init__(self, value):
        self.value=value
        self.next=None
        self.prev=None
class DoubleLinkedList:
    def __init__(self, value):
        new_node=Node(value)
        self.head=new_node
        self.tail=new_node
        self.length=1
    def append(self, value):
        new_node=Node(value)
        if self.length==0:
            self.head=new_node
            self.tail=new_node
        else:
            new_node.prev=self.tail
            self.tail.next=new_node
            self.tail=new_node
        self.length+=1
        return True
    def prepend(self, value):
        new_node=Node(value)
        if self.length==0:
            self.head=new_node
            self.tail=new_node
        else:
            new_node.next=self.head
            self.head.prev=new_node
            self.head=new_node
        self.length+=1
        return True
    def pop(self):
        if self.length==0:
            return False
        temp=self.tail
        if self.length==1:
            self.head=None
            self.tail=None
        else:
            before=temp.prev
            before.next=None
            temp.prev=None
            self.tail=before
        self.length-=1
        return temp
    def pop_first(self):
        if self.length==0:
            return False
        temp=self.head
        if self.length==1:
            self.head=None
            self.tail=None
        else:
            after=temp.next
            after.prev=None
            temp.next=None
            self.head=after
        self.length-=1            
        return temp
    def get(self, index):
        if index<0 or index>=self.length:
            return False
        else:
            temp=self.head
            for _ in range(index):
                temp=temp.next
            return temp
    def set_value(self, index, value):
        temp=self.get(index)
        if temp:
            temp.value=value
            return True
        return False
    def insert(self, index, value):
        if index<0 or index>=self.length:
            return False
        if index==0:
            return self.prepend(value)
        if index==self.length-1:
            return self.append(value)
        new_node=Node(value)
        before=self.get(index-1)
        after=before.next
        new_node.prev=before
        before.next=new_node
        new_node.next=after
        after.prev=new_node
        self.length+=1
        return True
    def remove(self,index):
        if index<0 or index>=self.length:
            return False
        if index==0:
            return self.pop_first()
        if index==self.length-1:
            return self.pop()
        temp=self.get(index)
        before=temp.prev
        after=temp.next
        before.next=after
        after.prev=before
        temp.next=None
        temp.prev=None
        self.length-=1
        return temp
    def print_list(self):
        temp=self.head
        for _ in range(self.length):
            print(temp.value)
            temp=temp.next
    def reverse(self):
        temp=self.head
        self.head=self.tail
        self.tail=temp

        before=None

        for _ in range(self.length):
            after=temp.next
            temp.next=before
            temp.prev=after
            before=temp
            temp=after


DLL=DoubleLinkedList(1)
DLL.append(3)
DLL.append(4)
DLL.prepend(5)
# DLL.remove(4)
DLL.print_list()
DLL.reverse()
print('after reversal')
DLL.print_list()





    


        




        


    