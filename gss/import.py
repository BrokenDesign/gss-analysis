# type: ignore
import os
import pickle
import tempfile
from zipfile import ZipFile
import polars as pl
from polars import Expr

from gss import settings


def read_to_dict() -> tuple:
    archive = ZipFile(settings.gss.raw)
    temp = tempfile.NamedTemporaryFile(suffix=".dta", mode="wb", delete=False)
    temp.write(archive.read(settings.gss.archive.data))
    temp.close()

    data, meta = pyreadstat.read_dta(
        filename_path=temp.name,
        encoding="LATIN1",
        metadataonly=False,
        user_missing=True,
        output_format="dict",
    )

    os.remove(temp.name)
    return data, meta


def _safe_get(_dict, key):
    if not key in _dict:
        raise KeyError(key, _dict)
    else:
        return _dict.get(key, null)


def _safe_put(_dict, key, value) -> None:
    assert not key in _dict, "duplicate keys"
    _dict[key] = value


def _map_values(values: dict) -> tuple[dict, dict]:
    data_map = {}
    value_map = {}
    for k, v in values.items():
        if isinstance(k, int | float):
            _safe_put(data_map, k, int(k))
            _safe_put(value_map, int(k), v)
        else:
            _safe_put(data_map, k, -ord(k))
            _safe_put(value_map, -ord(k), v)
    return data_map, value_map


def split_metadata(meta) -> tuple[list[Expr], dict]:
    transforms = []
    config = []
    variable_value_labels = {
        variable: values for variable, values in meta.variable_value_labels.items()
    }
    for name, label in meta.column_names_to_labels.items():
        if name in variable_value_labels:
            data_map, value_map = _map_values(variable_value_labels[name])
            if data_map:
                transforms.append(pl.col(name).apply(lambda x: data_map[x]))
            config.append({"name": name, "label": label, "values": value_map})
        else:
            config.append({"name": name, "label": label, "values": None})
    return transforms, {"variables": config}


def write_layout(meta: dict) -> None:
    with open(settings.gss.layout, "wb") as f:
        pickle.dump(meta, f)


def process_data(data: dict, transforms: list[Expr]) -> None:
    df = pl.DataFrame(data, schema_overrides={col: pl.Utf8 for col in data})
    # df = df.with_columns(transforms)
    print(df)
    df.write_ipc(settings.gss.data)
    return df


if __name__ == "__main__":
    data, meta = read_to_dict()
    transforms, config = split_metadata(meta)
    write_layout(config)
    df = process_data(data, transforms)
    print(list(filter(lambda x: x.key not in df.columns, meta)))


# print({
#     "column_names": meta.column_names[0],
#     "column_labels": meta.column_labels[0],
#     "column_names_to_labels": list(meta.column_names_to_labels.items())[0],
#     "variable_value_labels": list(meta.variable_value_labels.items())[0],
#     "value_labels": list(meta.value_labels.items())[0],
#     "variable_to_label": list(meta.variable_to_label.items())[0],
# })
# return 0, 0
