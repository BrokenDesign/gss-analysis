# type: ignore
import re
from dataclasses import dataclass
from gss import settings
from io import BytesIO
from typing import Any, Optional, Iterable
from PyPDF2 import PdfReader, PageObject
from zipfile import ZipFile
from more_itertools import peekable
import srsly


@dataclass
class Variable:
    name: str
    codebook_name: str
    section: int
    label: str
    description: str
    categories: Optional[dict]

    def format(self, value: Any) -> str:
        if self.categories is None:
            raise ValueError("Categories not set")
        elif value not in self.categories:
            raise ValueError("Value not in categories")
        else:
            return self.categories.get(value, value)


class CodebookReader:
    pdf: PdfReader
    _variables: list[Variable]

    def __init__(self, pdf: PdfReader):
        self.pdf = pdf
        self._variables = []

    @property
    def variables(self):
        if not self._variables:
            self._variables = self._read_variables()
            return self._variables
        else:
            return self._variables

    def _read_variables(self) -> list[Variable]:
        variables = []
        for page in self.pdf.pages[42:43]:
            print("******")
            print(type(page))
            variables.extend(self._parse_variable_page(page))
        return variables

    def _parse_variable_page(self, page: PageObject) -> Iterable[Variable]:
        contents = page.extract_text()
        pattern = r"(?P<name>[A-Z0-9]+)\s(?P<section>\d+)"
        matches = peekable(re.finditer(pattern, contents))

        for match in matches:
            try:
                label = contents[match.end() : matches.peek().start()]
            except StopIteration:
                label = contents[match.end() :]
            finally:
                label = label.replace("\n", " ")  # type: ignore

            yield Variable(
                name=match.group("name"),
                codebook_name=match.group("name"),
                section=int(match.group("section")),
                label=label,
                description="",
                categories=None,
            )

    def save(self, out: Optional[str] = None) -> None:
        if out is None:
            out = settings.gss.codebook  # type: ignore
        srsly.write_json(out, self.variables)  # type: ignore


if __name__ == "__main__":
    archive = ZipFile(settings.gss.raw)  # type: ignore
    pdf = PdfReader(
        BytesIO(archive.open(settings.gss.archive.codebook).read())  # type: ignore
    )
    codebook = CodebookReader(pdf)
    codebook.save()
