import random
from operator import gt, lt
import heapq
import copy
import inspect
import sys
from time import sleep
from pprint import pprint
import os
import binarytree
from termcolor import colored, COLORS


with open(os.path.realpath(__file__)) as f:
    LINES_FROM_THIS_FILE = f.readlines()


GREEN = "green"
RED = "red"
BLUE = "blue"
GREY = "grey"
CYAN = "cyan"

SWAP = "swap"
CONSIDER = "consider"

DEMARCATIONS = {
    SWAP: RED,
    CONSIDER: BLUE
}

DEMARCATION_MESSAGES = {
    SWAP: "A swap has occurred!",
    CONSIDER: "Considering the following items..."
}

MIN = "min"
MAX = "max"
HEAP_TYPES = {MIN: lt, MAX: gt}  # Min Heap  # Max Heap


def heapify(items, kind=MIN):
    end = len(items) - 1
    comparator = HEAP_TYPES.get(kind)  # default to max heap.

    # Start with arbitrary list, then heapify from the bottom up
    for index in range(end // 2, -1, -1):
        sift_down(items, index, compare=comparator)


def sift_down(heap, parent_ptr, end=None, compare=gt):
    """Sift down implementation.

    Args:
        heap: An array being treated as a heap structure.
        parent_ptr: the pointer to the root node from which we are descending.
        end: Optional argument. Define the pointer for the end of the heap.
        compare: the comparator function determining what kind of "sift" this is - gt for max heap; lt for min.

    Side Effects:
        Mutates the first argument, heap.
    """
    ptr_to_last = end if end is not None else len(heap) - 1
    left_ptr = 2 * parent_ptr + 1
    right_ptr = left_ptr + 1

    potential_swap_ptr = parent_ptr  # This pointer will change if the mini-heap is broken!

    # The two blocks below mean, "out of the parent and its two children, make sure the parent is in the position of
    # the largest"
    if left_ptr <= ptr_to_last:
        demarcate(heap, left_ptr, potential_swap_ptr, kind=CONSIDER)
        if compare(heap[left_ptr], heap[potential_swap_ptr]):
            potential_swap_ptr = left_ptr

    if right_ptr <= ptr_to_last:
        demarcate(heap, right_ptr, potential_swap_ptr, kind=CONSIDER)
        if compare(heap[right_ptr], heap[potential_swap_ptr]):
            potential_swap_ptr = right_ptr

    # capture these values to make the next part readable
    current_largest = heap[potential_swap_ptr]
    parent = heap[parent_ptr]

    if potential_swap_ptr != parent_ptr:
        heap[potential_swap_ptr] = parent
        heap[parent_ptr] = current_largest

        demarcate(heap, parent_ptr, potential_swap_ptr, kind=SWAP)

        sift_down(heap, potential_swap_ptr, end=ptr_to_last, compare=compare)


def heapsort(array):
    ptr_to_last = len(array) - 1

    heapify(array, kind=MAX)

    while ptr_to_last > 0:  # keep swapping first and last and sifting down.
        array[0], array[ptr_to_last] =  array[ptr_to_last], array[0]  # first and last are switched.
        ptr_to_last -= 1  # reduce heap size by one.
        sift_down(array, 0, end=ptr_to_last)  # restore heap property.



def demarcate(array, first, second, kind=SWAP):
    message = DEMARCATION_MESSAGES[kind]
    heap = demarcate_items_in_heap(array, first, second, kind)
    array = demarcate_items_in_array(array, first, second, kind)
    code = demarcate_code()

    full_output = f"{message}\n{heap}\n{array}\n\n{code}\n"
    sys.stdout.write(full_output)


def demarcate_items_in_array(array, first, second, kind=SWAP):
    which_color = DEMARCATIONS.get(kind)

    aligned_indices_as_strings = []

    demarcated = []

    # construct list with indices aligned with sheer brute force
    for index, number in enumerate(array):
        string_index = str(index)
        colored_index = colored(string_index, GREY, attrs=["dark"])
        string_number = str(number)
        string_num_len = len(string_number)
        index_num_len = len(string_index)
        buffer_width = len(max(string_index, string_number, key=len)) + 2

        if index == first or index == second:
            string_number = colored(string_number, which_color, attrs=["bold"])
            colored_index = colored(string_index, GREY, attrs=["dark", "underline"])

        aligned_indices_as_strings.append(colored_index + (" " * (buffer_width - index_num_len)))
        demarcated.append(string_number + (" " * (buffer_width - string_num_len)))

    return " ".join(demarcated) + "\n" + " ".join(aligned_indices_as_strings)


def demarcate_items_in_heap(heap, first_idx, second_idx, kind=SWAP):

    which_color = DEMARCATIONS.get(kind)

    tree = binarytree.build(heap)
    lines = binarytree._build_tree_string(tree, 0, index=True, delimiter="-")[0]
    tree_string = "\n" + "\n".join((line.rstrip() for line in lines))

    for index, number in enumerate(heap):  # Convert the rest as well.
        as_string = str(number)
        with_index = f"{index}-{as_string}"
        replacement = ("{:^" + str(len(with_index)) + "}").format(as_string)

        if index in (first_idx, second_idx):
            tree_string = tree_string.replace(
                with_index,
                colored(replacement, which_color, attrs=["bold", "blink"]))
        else:
            tree_string = tree_string.replace(with_index, replacement)

    return tree_string


def demarcate_code():
    """Serious shit!"""
    buffer = " . " * 4
    # We want to make sure this works in a debugger, so we get that info from the frames.
    frame_info = [fi for fi in inspect.stack() if fi.filename == __file__ and fi.function == "sift_down"][0]
    line_of_execution = frame_info.frame.f_lineno
    segments = LINES_FROM_THIS_FILE[line_of_execution - 5: line_of_execution + 5]
    segments[6] = colored(segments[6], CYAN, attrs=["bold"])

    return "\n{:_^75}\n".format(buffer + "<CODE>" + buffer) + "".join(segments) + "\n{:_^75}".format(buffer + "</CODE>" + buffer)



### Tests....

test_one = [random.randint(0, 1000) for _ in range(31)]
test_two = test_one.copy()

heapify(test_one, kind=MIN)

mine = binarytree.build(test_one)

print("\n{:-^100}\n".format("<*****HEAP*****>"))
print(colored(mine, GREEN if mine.is_min_heap else RED, attrs=["bold"]))

print("\n{:-^100}\n".format("<*****BINARY SEARCH TREE*****>"))
print("\nHeap sorting...\n")
heapsort(test_one)
print(test_one)
print(sorted(test_one))
mine = binarytree.build(test_one)
