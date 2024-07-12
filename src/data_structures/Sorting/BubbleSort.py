'''
Algorithm
1) Start with first element and compare it with the rest of the list and do swaps
2) By now we have the greatest element at the last position
3) repeat the operation from first element until last 2nd element and stop when the  end of the list is reached
'''
def bubble_sort(my_list):
    for i in range(len(my_list)-1, 0, -1):
        for j in range(i):# first time whole list, 2nd time n-1, n-2. just the first number
            if my_list[j]>my_list[j+1]:
                temp=my_list[j]
                my_list[j]=my_list[j+1]
                my_list[j+1]=temp
    return my_list

print(bubble_sort([2,4,5,1,3,6]))