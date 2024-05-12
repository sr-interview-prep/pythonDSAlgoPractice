from data_structures.LinkedList.SingleLinkedList import LinkedList


def test_single_linked_list():
    sll = LinkedList(value=10)
    assert sll.get(index=0).value == 10
    sll.append(value=20)
    assert sll.get(index=1).value == 20
    sll.remove(index=0)
    assert sll.get(index=0).value == 20
    sll.prepend(value=30)
    assert sll.get(index=0).value == 30
    assert sll.pop().value == 20
    assert sll.get(index=0).value == 30
    assert sll.append(value=50)
    sll.reverse()
    assert sll.get(index=0).value == 50
    sll.insert(index=1, value=80)
    assert sll.get(index=1).value == 80
    assert sll.pop_first().value == 50
    assert sll.get(index=0).value == 80
    assert sll.set_value(index=0, value=90) is True
    assert sll.get(index=0).value == 90
