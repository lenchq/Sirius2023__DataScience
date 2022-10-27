from typing import List


def list_chunks(input_list: List, size: int):
    return [input_list[n:n + size] for n in range(0, len(input_list), size)]
