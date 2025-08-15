import os
import io
import requests
import pandas as pd
import json
import importlib
import importlib.util


def _lazy_import(module_name: str, package_hint: str | None = None):
    """Import a module on demand and raise a helpful error if missing.

    package_hint is the pip package name to suggest to the user (e.g. 'pyyaml').
    """
    try:
        return importlib.import_module(module_name)
    except ImportError as e:
        hint = package_hint or module_name
        raise ImportError(
            f"Missing optional dependency '{module_name}'. Install it with e.g. `pip install {hint}` to use this feature."
        ) from e

class RemoteTable:
    def __init__(self, source, **kwargs):
        self.source = source
        # store user options
        self.kwargs = kwargs
        # normalize some common option names
        # headers: False | 'first_row' (default) | list of names
        if 'headers' not in self.kwargs:
            self.kwargs.setdefault('headers', 'first_row')
        self.data = self._load()

    def _load(self):
        if self.source.startswith('http://') or self.source.startswith('https://'):
            content = requests.get(self.source).content
            ext = self.source.split('.')[-1].lower()
        else:
            with open(self.source, 'rb') as f:
                content = f.read()
            ext = self.source.split('.')[-1].lower()
        df = None
        if ext in ['csv', 'tsv']:
            # CSV/TSV options
            sep = '\t' if ext == 'tsv' else ','
            sep = self.kwargs.get('delimiter', sep)
            read_csv_kwargs = {
                'sep': sep,
            }
            if self.kwargs.get('quote_char'):
                read_csv_kwargs['quotechar'] = self.kwargs['quote_char']
            if self.kwargs.get('skip'):
                read_csv_kwargs['skiprows'] = self.kwargs['skip']
            if self.kwargs.get('encoding'):
                read_csv_kwargs['encoding'] = self.kwargs['encoding']
            headers = self.kwargs.get('headers')
            if headers is False:
                read_csv_kwargs['header'] = None
            elif isinstance(headers, (list, tuple)):
                read_csv_kwargs['header'] = None
                read_csv_kwargs['names'] = list(headers)
            # BytesIO works for pandas read_csv
            df = pd.read_csv(io.BytesIO(content), **read_csv_kwargs)
        elif ext == 'json':
            # support root_node option to select nested JSON arrays
            text = content.decode(self.kwargs.get('encoding', 'utf-8')) if isinstance(content, (bytes, bytearray)) else content
            obj = json.loads(text)
            root = self.kwargs.get('root_node')
            if root:
                for part in root.split('.'):
                    obj = obj.get(part, {})
            df = pd.DataFrame(obj)
        elif ext == 'xlsx':
            # ensure openpyxl is available for .xlsx
            _lazy_import('openpyxl', 'openpyxl')
            excel_kwargs = {}
            if 'sheet' in self.kwargs:
                excel_kwargs['sheet_name'] = self.kwargs['sheet']
            df = pd.read_excel(io.BytesIO(content), engine='openpyxl', **excel_kwargs)
        elif ext == 'xls':
            excel_kwargs = {}
            if 'sheet' in self.kwargs:
                excel_kwargs['sheet_name'] = self.kwargs['sheet']
            df = pd.read_excel(io.BytesIO(content), **excel_kwargs)
        elif ext == 'ods':
            df = self._read_ods(content)
        elif ext == 'yml' or ext == 'yaml':
            # yaml parsing is optional
            yaml = _lazy_import('yaml', 'pyyaml')
            text = content.decode(self.kwargs.get('encoding', 'utf-8')) if isinstance(content, (bytes, bytearray)) else content
            obj = yaml.safe_load(text)
            root = self.kwargs.get('root_node')
            if root:
                for part in root.split('.'):
                    obj = obj.get(part, {})
            df = pd.DataFrame(obj)
        elif ext == 'xml':
            df = self._read_xml(content)
        elif ext == 'html':
            df = self._read_html(content)
        else:
            raise ValueError(f'Unsupported file extension: {ext}')

        # Post-process headers and cleaning
        headers_opt = self.kwargs.get('headers')
        if headers_opt == 'first_row':
            df = self._ensure_headers_from_first_row(df)
        # Clean header names (normalize whitespace, fill blanks, unique)
        df = self._clean_headers(df)
        return df

    def _ensure_headers_from_first_row(self, df: pd.DataFrame) -> pd.DataFrame:
        # If the DataFrame columns look like positional integers (no headers),
        # and the first row holds header names, promote the first row to header.
        if df is None or df.shape[0] == 0:
            return df
        # Heuristic: if columns are numeric range or '0','1',..., treat first row as header
        cols = list(df.columns)
        numeric_cols = all(isinstance(c, (int,)) or (isinstance(c, str) and c.isdigit()) for c in cols)
        if numeric_cols:
            new_header = df.iloc[0].astype(str).tolist()
            df = df.iloc[1:].reset_index(drop=True)
            df.columns = new_header
        return df

    def _clean_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            return df
        cols = list(df.columns)
        new = []
        seen = set()
        untitled_count = 0
        for i, c in enumerate(cols):
            name = '' if c is None else str(c)
            # normalize whitespace
            name = ' '.join(name.split())
            name = name.strip()
            if name == '' or name.lower().startswith('unnamed'):
                untitled_count += 1
                name = f'untitled_{untitled_count}'
            base = name
            suffix = 1
            while name in seen:
                name = f"{base}_{suffix}"
                suffix += 1
            seen.add(name)
            new.append(name)
        df.columns = new
        return df

    def _read_ods(self, content):
        # ODS support is optional (odfpy)
        odf_module = _lazy_import('odf.opendocument', 'odfpy')
        table_module = _lazy_import('odf.table', 'odfpy')
        load_ods = odf_module.load
        Table = table_module.Table
        TableRow = table_module.TableRow
        TableCell = table_module.TableCell
        ods = load_ods(io.BytesIO(content))
        tables = ods.spreadsheet.getElementsByType(Table)
        rows = []
        for table in tables:
            for row in table.getElementsByType(TableRow):
                cells = [cell.plaintext() for cell in row.getElementsByType(TableCell)]
                rows.append(cells)
        return pd.DataFrame(rows)

    def _read_xml(self, content):
        # XML parsing via lxml (optional)
        lxml = _lazy_import('lxml.etree', 'lxml')
        etree = lxml
        tree = etree.fromstring(content)
        rows = []
        for row in tree.findall('.//row'):
            rows.append([cell.text for cell in row])
        return pd.DataFrame(rows)

    def _read_html(self, content):
        # HTML parsing via BeautifulSoup (optional)
        bs4 = _lazy_import('bs4', 'beautifulsoup4')
        BeautifulSoup = bs4.BeautifulSoup
        soup = BeautifulSoup(content, 'lxml')
        # allow selecting rows/columns by CSS if provided
        row_selector = self.kwargs.get('row_css')
        col_selector = self.kwargs.get('column_css')
        table = soup.find('table') if not self.kwargs.get('table_index') else soup.find_all('table')[self.kwargs.get('table_index')]
        rows = []
        if row_selector:
            tr_elements = table.select(row_selector)
        else:
            tr_elements = table.find_all('tr')
        for tr in tr_elements:
            if col_selector:
                cells = [c.get_text() for c in tr.select(col_selector)]
            else:
                cells = [td.get_text() for td in tr.find_all(['td', 'th'])]
            rows.append(cells)
        return pd.DataFrame(rows)

    def __iter__(self):
        # allow iterating as dict rows when requested
        # Return an iterator in both cases. Avoid using `yield` in one branch
        # and `return` in the other because that makes the function a
        # generator and breaks the non-generator branch.
        if self.kwargs.get('as_dict'):
            return iter(self.data.to_dict('records'))
        return self.data.itertuples(index=False, name=None)

    def to_dataframe(self):
        return self.data
