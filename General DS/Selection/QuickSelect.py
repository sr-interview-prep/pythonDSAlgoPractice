# Mainly used to find the k-th order statistic (not many, if many better go for sorting)



# If the pivot element is chosen as the median of the array, O(n) time complexity is 
# gauranteed but at the expense 
#of o(log n) space complexity

'''To make the above statement work, split the array into 5 element chunks sort them with insertion sort
whose time complexity is O(n). Later find the median of each arrays, sort them using insertion sort
and then find the median of medians as the pivot value'''
'''The combination of medians algo with quick select algo is called introselect algo'''

def median_index(my_list):
    # for the moment hardcoded to 2, but need an algo for it
    return 2

def swap(my_list, index1, index2):
    temp=my_list[index1]
    my_list[index1]=my_list[index2]
    my_list[index2]=temp
    return my_list


def pivot(my_list, pivot_index, end_index):
    swap_index=pivot_index
    for i in range(pivot_index+1, end_index+1):
        if my_list[i]<my_list[pivot_index]:
            swap_index+=1
            my_list=swap(my_list, swap_index,i)
    my_list=swap(my_list,pivot_index,swap_index)
    return swap_index

def quick_select_kth_greatest_value(my_list, left, right,k):
    if left<=right:
        pivot_index=median_index(my_list)
        swap(my_list,pivot_index,left)
        pivot_index=pivot(my_list, left, right)
        if k==pivot_index:
            return my_list[k]
        elif k<pivot_index:
            return quick_select_kth_greatest_value(my_list,left, pivot_index-1,k)
        elif k>pivot_index:
            return quick_select_kth_greatest_value(my_list, pivot_index+1,right,k)        
    # return pivot_index

my_list=[4,6,1,7,3,2,5]
def quick_selecter_helper(my_list, n):
    return quick_select_kth_greatest_value(my_list,0, len(my_list)-1,n-1)

print(quick_selecter_helper(my_list,4))


