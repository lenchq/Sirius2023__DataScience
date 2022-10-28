from dotenv import dotenv_values, find_dotenv, set_key
import asyncio
import ids_generator
import util
import vkParser
import pandas as pd

from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)


async def main():
    # init dotenv variables
    dotenv_file = find_dotenv()
    dotenv = dotenv_values(dotenv_file)
    save_xlsx = dotenv.get("SAVE_XLSX") == "True"
    api_key = dotenv.get("VK_USER_KEY")
    skip_gen = dotenv.get("ID_GEN_SKIP") == "True"
    save_ids = dotenv.get("SAVE_GENERATED_IDS") == "True"

    progress = Progress(
        TextColumn("[bold blue]{task.fields[name]}{task.fields[counter]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        TimeRemainingColumn(),
    )
    progress.start()

    def upd(counter: int, total_count: int) -> None:
        progress.update(task_id,
                        completed=counter,
                        counter=f"[{counter} / {total_count}]")

    # Any value here
    ids_to_gen = 10_000

    if skip_gen:
        ids_data = pd.read_csv("ids.csv")
        ids = ids_data.iloc[:, 1].tolist()
    else:
        task_id = progress.add_task("Generating", name="Generating ", total=ids_to_gen, counter=0, start=True)
        progress.start_task(task_id)

        ids = ids_generator.ids_generator(api_key) \
            .generate_ids(1_000_000, 600_000_000, ids_to_gen, chunk_size=1000, update_status=upd)
        if save_ids:
            ids_series = pd.Series(ids, dtype='int')
            ids_series.to_csv("ids.csv")
        progress.remove_task(task_id)

    vkp = vkParser.VkParser(api_key)

    task_id = progress.add_task("Parsing",
                                name="Parsing ",
                                total=len(ids),
                                counter=0,
                                start=True)
    progress.start_task(task_id)

    # users DataFrame columns
    columns = [
        "ID",
        "name",
        "age",
        "status",
        "groups",
        "groups_links",
        "friends",
        "followers",
        "likes",
        "relationship"
    ]
    # users DataFrame columns which type is supposed to be integer
    integer_columns = [
        "ID",
        "age",
        "friends",
        "followers"
    ]

    # generate empty result.csv file (with dataframe columns)
    # if none is present
    with open('result.csv', 'w+', encoding="utf-8") as f:
        if len(f.read()) < 3:
            f.write(",".join(iter(columns)))

    parsed_df = pd.read_csv("result.csv",
                            dtype="object",
                            index_col=0)

    pos = int(dotenv.get("PARSING_POS"))
    # any number bigger than 8 causes "Too Many API Calls" error by vk api
    # because parse 1 person = 3 api requests
    # parse 8 persons = 24 api requests
    # the limit is 25 api requests per api.execute
    chunk_size = 8
    chunks = util.list_chunks(ids, chunk_size)
    total = len(ids)
    new = pd.DataFrame(columns=columns, dtype="object")
    for current in chunks[pos:]:
        try:
            parsed_chunk = vkp.parse(current,
                                     chunk_size=chunk_size)
        except Exception as e:
            print(e)
            set_key(dotenv_file, "PARSING_POS", str(pos))
            break
        upd(pos * chunk_size, total)
        for user in parsed_chunk:
            user_series = pd.Series(user, index=None)
            new = new.append(user_series, ignore_index=True)
        if current == chunks[-1]:
            # if end of parsing
            set_key(dotenv_file, "PARSING_POS", "0")
        else:
            pos += 1

    # new[integer_columns] = new[integer_columns] \
    #     .applymap(int)

    # concat parsed ids by this session and already saved in results.csv file, if any
    result_df = pd.concat([parsed_df, new], ignore_index=True)

    result_df.to_csv("result.csv")

    if save_xlsx:
        result_df.to_excel("result-excel.xlsx", sheet_name="VkUsers", index=True)

    progress.stop()


if __name__ == '__main__':
    asyncio.run(main())
