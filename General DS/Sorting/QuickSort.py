'''
Algoritm
1) Take first element as the pivot
2) Check from 2nd element if that is greater than the pivot element, if yes inc the swap_index and then swap i and and swap_index value
3) At the end of the loop swap the swap_index and pivot_index value. This will ensure that all small elements are left of pivot and big elements are right of pivot
4) The call quick sort on the left elements, and right elements in a recursion with base case being left index should be greater than right index
'''
def swap(my_list, index1, index2):
    temp=my_list[index1]
    my_list[index1]=my_list[index2]
    my_list[index2]=temp
    return my_list
'''
[4, 6, 1, 7, 3, 2, 5]
[4, 1, 6, 7, 3, 2, 5]
[4, 1, 3, 7, 6, 2, 5]
[4, 1, 3, 2, 6, 7, 5]
[2, 1, 3, 4, 6, 7, 5] -- End of 1st pivot
'''



def pivot(my_list, pivot_index, end_index):
    swap_index=pivot_index
    for i in range(pivot_index+1, end_index+1):
        if my_list[i]<my_list[pivot_index]:
            swap_index+=1
            my_list=swap(my_list, swap_index,i)
    my_list=swap(my_list,pivot_index,swap_index)
    return swap_index

def quick_sort(my_list, left, right):
    if left<right:
        pivot_index=pivot(my_list, left, right)
        quick_sort(my_list, left,pivot_index-1)
        quick_sort(my_list,pivot_index+1, right)
    return my_list

my_list=[4,6,1,7,3,2,5]
print(quick_sort(my_list,0,len(my_list)-1))


