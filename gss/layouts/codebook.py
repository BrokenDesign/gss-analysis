from gss import settings, resources
import polars as pl
import os
from tabulate import tabulate
from functools import partial

def find_any(terms: list[str], description: str) -> bool:
    return any(map(lambda term: term.casefold() in description, terms))


def search_description(terms: str | list[str]) -> str:
    if not isinstance(terms, list): 
        terms = [terms]
    
    find = partial(find_any, terms)
    found = (
        pl.read_ipc(resources["test.arrows"])
        .filter(pl.col("description").apply(find))
        .to_dicts()
    )
    table = tabulate(found, tablefmt="fancy_grid", headers="keys")
    with open("reports/search.txt", "w", encoding="utf-16") as f:
        f.write(table)
    print(table)
    return table


search_description(["sex"])
