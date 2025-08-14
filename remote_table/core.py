import os
import io
import requests
import pandas as pd
import yaml
from bs4 import BeautifulSoup
from lxml import etree
from odf.opendocument import load as load_ods
from odf.table import Table, TableRow, TableCell
from openpyxl import load_workbook

class RemoteTable:
    def __init__(self, source, **kwargs):
        self.source = source
        self.kwargs = kwargs
        self.data = self._load()

    def _load(self):
        if self.source.startswith('http://') or self.source.startswith('https://'):
            content = requests.get(self.source).content
            ext = self.source.split('.')[-1].lower()
        else:
            with open(self.source, 'rb') as f:
                content = f.read()
            ext = self.source.split('.')[-1].lower()
        if ext in ['csv', 'tsv']:
            sep = '\t' if ext == 'tsv' else ','
            return pd.read_csv(io.BytesIO(content), sep=sep)
        elif ext == 'json':
            return pd.read_json(io.BytesIO(content))
        elif ext == 'xlsx':
            return pd.read_excel(io.BytesIO(content), engine='openpyxl')
        elif ext == 'xls':
            return pd.read_excel(io.BytesIO(content))
        elif ext == 'ods':
            return self._read_ods(content)
        elif ext == 'yml' or ext == 'yaml':
            return pd.DataFrame(yaml.safe_load(content))
        elif ext == 'xml':
            return self._read_xml(content)
        elif ext == 'html':
            return self._read_html(content)
        else:
            raise ValueError(f'Unsupported file extension: {ext}')

    def _read_ods(self, content):
        ods = load_ods(io.BytesIO(content))
        tables = ods.spreadsheet.getElementsByType(Table)
        rows = []
        for table in tables:
            for row in table.getElementsByType(TableRow):
                cells = [cell.plaintext() for cell in row.getElementsByType(TableCell)]
                rows.append(cells)
        return pd.DataFrame(rows)

    def _read_xml(self, content):
        tree = etree.fromstring(content)
        rows = []
        for row in tree.findall('.//row'):
            rows.append([cell.text for cell in row])
        return pd.DataFrame(rows)

    def _read_html(self, content):
        soup = BeautifulSoup(content, 'lxml')
        table = soup.find('table')
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text() for td in tr.find_all(['td', 'th'])]
            rows.append(cells)
        return pd.DataFrame(rows)

    def __iter__(self):
        return self.data.itertuples(index=False, name=None)

    def to_dataframe(self):
        return self.data
