from gss import resources
import srsly
import pickle
import os
import polars as pl
from functional import seq as sequence


def test_yaml():
    srsly.read_yaml(resources["codebook.yaml"])


def test_msg():
    srsly.read_msgpack("test.msg")


def test_pickle():
    with open("test.pickle", "rb") as f:
        pickle.load(f)


def test_arrows():
    pl.read_ipc("test.arrows")


def wrap_string(s: str) -> str:
    return s.encode("utf-8").decode(encoding="utf-8", errors="repace")


def convert_to_rows(item) -> dict:
    values = ["null = unknown"]
    values.extend(
        [
            wrap_string(f"{value} = {label}")
            for value, label in item["values"].items()
            if not isinstance(value, str)
        ]
    )
    if item["label"] == None:
        item["label"] = ""

    result = {
        "variable": wrap_string(item["name"]),
        "description": wrap_string(item["label"].encode(encoding="utf-8").decode(errors="replace")),
        "values": wrap_string("\n".join(values),)
    }
    return result


# def convert_to_rows(item) -> dict:
#     result = {
#         "variable": item["name"],
#         "description": item["label"],
#         "values": [{"value": None, "label": "unknown"}],
#     }
#     result["values"].extend(
#         [
#             {"value": value, "label": label}
#             for value, label in item["values"].items()
#             if not isinstance(value, str)
#         ]
#     )
#     return result


# def convert_to_rows(item) -> list[dict]:
#     result = [
#         {
#             "variable": item["name"],
#             "description": item["label"],
#             "value": None,
#             "label": "unknown",
#         }
#     ]
#     result.extend(
#         [
#             {
#                 "variable": item["name"],
#                 "description": item["label"],
#                 "value": value,
#                 "label": label,
#             }
#             for value, label in item["values"].items()
#             if not isinstance(value, str)
#         ]
#     )
#     return result


def convert_to_polars():
    rows = (
        sequence(srsly.read_msgpack("test.msg")["variables"])  # type: ignore
        .map(convert_to_rows)
        .to_list()  # type: ignore
    )
    df = pl.DataFrame(rows)
    df.write_ipc("gss/layouts/test.arrows")
    print(df)


if __name__ == "__main__":
    convert_to_polars()
    # test_yaml()
    # test_msg()
    # test_pickle()
    # test_arrows()
