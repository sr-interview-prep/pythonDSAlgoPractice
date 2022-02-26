'''Works best in the case of small Array's (smaller than 10) where possibility of achieving linear time complexity is more'''

'''
Algorithm
1) Start with 2nd item compare it with 1st, if less then insert 2nd at 1st postion
2) 3rd item compare to 2nd, if less then compare it to first and insert the value before 1st or 2nd
'''

# 3 2 1
# 2 3 1 
# 2 1 3
# 1 2 3 
def insertion_sort(my_list):
    for i in range(1, len(my_list)):
        temp=my_list[i]
        j=i-1
        while temp<my_list[j] and j>-1:
            my_list[j+1]=my_list[j]
            my_list[j]=temp
            j-=1
    return my_list

print(insertion_sort([4,2,6,5,1,3]))
            
