import importlib.resources

from dynaconf import Dynaconf


def _resources(package: str, files=None):
    exclude = [".py", "__"]
    if files is None:
        files = {}

    for file in importlib.resources.files(package).iterdir():
        excluded = any((file.name.endswith(suffix) for suffix in exclude))
        if file.is_file() and not excluded:
            files[file.name] = str(file)
        elif not excluded:
            _resources(f"{package}.{file.name}", files=files)

    return files


resources: dict[str, str] = _resources("gss")

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=[
        resources["settings.toml"]
    ],
)
