# Mainly used to find the k-th order statistic (not many, if many better go for sorting)

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

def quick_select(my_list, left, right,k):
    if left<=right:
        pivot_index=pivot(my_list, left, right)
        # print(pivot_index)
        if k-1==pivot_index-1:
            return my_list[k]
        elif k-1<pivot_index-1:
            return quick_select(my_list,left, pivot_index-1,k)
        elif k-1>pivot_index-1:
            return quick_select(my_list, pivot_index+1,right,k)        
    # return pivot_index

my_list=[4,6,1,7,3,2,5]
def quick_selecter_helper(my_list, n):
    return quick_select(my_list,0, len(my_list)-1,n-1)

print(quick_selecter_helper(my_list,1))


