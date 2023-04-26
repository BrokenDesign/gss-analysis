# type: ignore
from os.path import exists

from gss import settings


def download():
    from tqdm import tqdm
    import requests

    url = "http://download.thinkbroadband.com/10MB.zip"
    response = requests.get(url, stream=True)

    with open("10MB", "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)


def import_data():
    pass


def main() -> None: 
    with settings.execution.download as task: 
        if not task.execute: 
            print("skipping download: execute=false")
        elif exists(settings.gss.raw) and not task.overwrite):
            print("skipping download: exists=true, overwrite=false")
        else: 
            download(
                url=settings.gss.url,
                out=settings.gss.file,
                overwrite=settings.execution.overwrite,
            )


    with settings.execution.import_data as task: 
        if not task.execute: 
            print("skipping import: execute=false")
        elif exists(settings.gss.data and not task.overwrite:
            print("skipping import: exists=true, overwrite=false")
        else:
            import_data(
                layout=settings.gss.layout,
                out=settings.gss.processed,
                overwrite=settings.execution.overwrite,
            )


if __name__ == "__main__": 
    main() 