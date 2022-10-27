import vkParser
import random
from typing import Callable


class ids_generator:

    def __init__(self, api_key: str):
        self.parser = vkParser.VkParser(api_key)

    def generate_ids(self,
                     min_range: int,
                     max_range: int,
                     count: int,
                     chunk_size: int = 1000,
                     update_status: Callable[[int, int], None] = None):
        ids = list()
        generated_count = 0
        while True:
            if generated_count >= count:
                break

            gen_ids = list()
            for i in range(chunk_size):
                id = random.randrange(min_range, max_range)
                gen_ids.append(id)

            # get valid users from generated user_ids
            data = self.parser.users(gen_ids)

            # extend by valid users
            ids.extend(data)
            generated_count += len(data)

            if update_status is not None:
                update_status(generated_count, count)
        update_status(count, count)
        return ids[:count]
