from enum import Enum
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, NamedTuple, Union, Callable, Optional
from dateutil import parser
from dateutil import relativedelta
import requests
import util


class Counters(NamedTuple):
    friends: int
    followers: int


class Relationship(Enum):
    undefined = 0
    not_married = 1
    has_a_friend = 2
    engaged = 3
    married = 4
    complicated = 5
    actively_looking = 6
    in_love = 7
    in_a_civil_marriage = 8


# noinspection PyShadowingBuiltins
class VkParser:
    IdType = Union[int, str]

    API_VER = 5.131
    API_ENDPOINT = "https://api.vk.com"
    API_USERS_GET = API_ENDPOINT + "/method/users.get"
    API_GROUPS_GET = API_ENDPOINT + "/method/groups.get"
    API_WALL_GET = API_ENDPOINT + "/method/wall.get"
    API_EXECUTE = API_ENDPOINT + "/method/execute"
    API_KEY = ""

    def __init__(self, api_key):
        self.API_KEY = api_key

    def users(self, ids: List[int]) -> List[int]:
        res = list()

        reqdata = {
            "v": self.API_VER,
            "access_token": self.API_KEY,
            "user_ids": ",".join(iter(map(lambda x: str(x), ids))),
            "fields": "followers_count,bdate,status"
        }
        data = self.get_request(self.API_USERS_GET, params=reqdata)

        for user in data["response"]:
            keys = user.keys()
            if "deactivated" in keys:
                continue
            if "is_closed" in keys and user["is_closed"]:
                continue
            if "followers_count" in keys \
                    and user["followers_count"] > 10 \
                    and "bdate" in keys \
                    and user["bdate"] != '' \
                    and "status" in keys \
                    and user["status"] != '':
                bdate = parser.parse(user["bdate"]).date()
                today = datetime.today()
                age = relativedelta.relativedelta(today, bdate).years
                if age > 10:
                    res.append(user["id"])

        return res

    def parse(self, ids: List[IdType],
              status_update: Optional[Callable[[int, int], None]] = None,
              chunk_size: int = 4) -> List[Dict[str, Any]]:
        total = len(ids)
        current = 0

        res = list()
        with open("userInfo.vkscript", 'r') as f:
            code = f.read()

        for curr_ids in util.list_chunks(ids, chunk_size):
            ids_count = len(curr_ids)
            gen_ids = ""

            for i in range(ids_count):
                gen_ids += f'"{curr_ids[i]}"'
                if ids_count + 1 != i:
                    gen_ids += ','

            cd = code.format(
                ids=gen_ids,
            )
            request_data = {
                "v": self.API_VER,
                "access_token": self.API_KEY,
                "code": cd
            }
            data = self.get_request(self.API_EXECUTE, params=request_data)

            for user in data["response"]:
                bdate = parser.parse(user["bdate"]).date()
                today = datetime.today()
                age = relativedelta.relativedelta(today, bdate).years
                posts = user["posts"]

                res.append({
                    "ID": user["id"],
                    "name": " ".join(iter([user["first_name"], user["last_name"]])),
                    "age": age,
                    "status": user["status"],
                    "groups": "\n".join(iter(user["groups"]["names"])),
                    "groups_links": "\n".join(iter(
                        self.resolve_group_links(user["groups"]["ids"])
                    )),
                    # "groups": self.resolve_groups(user["groups"], user["groups"]["count"]),
                    "friends": user["friends"],
                    "followers": user["followers"],
                    "likes": self.resolve_likes(posts),
                    "relationship": Relationship(user["relationship"]).name.replace('_', ' ')
                })
                current += 1
                if status_update is not None:
                    status_update(current, total)

        return res

    @staticmethod
    def resolve_groups(groups: Dict[str, List[str]], count: int) -> List[Dict[str, str]]:
        res = list()
        vk_group_url = "https://vk.com/"

        for i in range(count):
            id = groups["ids"][i]
            name = groups["names"][i]

            res.append(
                {
                    "name": name,
                    "url": vk_group_url + id,
                }
            )
        return res

    def resolve_counters(self, id: IdType) -> Counters:
        friends = followers = 0

        reqdata = {
            "v": self.API_VER,
            "access_token": self.API_KEY,
            "user_id": id,
            "fields": "counters",
            "extended": 1
        }
        data = self.get_request(self.API_USERS_GET, params=reqdata)
        data = data["response"][0]

        if "friends" in data["counters"]:
            friends = data["counters"]["friends"]
        if "followers" in data["counters"]:
            followers = data["counters"]["followers"]

        return Counters(friends, followers)

    @staticmethod
    def get_request(endpoint: str, params: Any) -> Dict[Any, Any]:
        try:
            req = requests.post(endpoint, params=params)
            data = req.json()
            if "error" in data.keys():
                error = data["error"]
                raise NameError(error["error_code"], error["error_msg"])
            else:
                # sleep(0.2)
                return data
        except NameError as err:
            print(err)
            raise

    @staticmethod
    def resolve_group_links(ids: List[int]) -> List[str]:
        res = list()
        vk_group_url = "https://vk.com/"
        for id in ids:
            res.append(vk_group_url + str(id))
        return res

    @staticmethod
    def resolve_likes(posts: Dict[str, Any]) -> float:
        def non_null(x: List[Optional[int]]) -> List[int]:
            res = list()
            for i in x:
                if i is None:
                    continue
                res.append(i)
            return res

        likes: List[int] = posts["likes"]
        if len(likes) == 0:
            return 0
        likes = non_null(likes)
        if len(likes) == 0:
            return 0

        return np.average(likes)
