"""Read, combine, and query metrics from Documentation sites CSVs."""


import collections
import csv
import io

from types import SimpleNamespace


def csv_to_rows_of_strings(csv_string=None, filehandle=None, path=None):
    """Read a path/csv_string/file obj and spit out rows of string lists.

    Specify ONE of csv_string, filehandle, or path (first source found in
    that order wins if multiple are provided). Make sure to follow the csv
    module instructions (open with newline='') and ensure the encoding is correct
    if you provide a filehandle. Paths to files assume a utf-8 encoded file.
    CSVs are opened with default csv module settings.

    :param csv_string: str, A string containing the contents of a CSV file.
    :param filehandle: An open file object (in string mode) to a CSV file.
    :param path: str, Path on disk to a CSV file.
    """

    # Dump whatever data source into a BytesIO object,
    # then read it with the CSV reader
    data = io.StringIO()
    if csv_string is not None:
        data.write(csv_string)
    elif filehandle is not None:
        data.write(filehandle.read())
    elif path is not None:
        with open(path, encoding='utf8', newline='') as csvfile:
            data.write(csvfile.read())
    else:
        raise Exception("Must provide a source for data!")
    # Put seek position at 0 (like an unread file)
    data.seek(0)

    rows = []
    reader = csv.reader(data)
    for row in reader:
        rows.append(row)
    return rows


class RowColumnView:
    """Lightweight row index or column-name indexable lists of cell values.

    Headers are separated/removed from data rows.

    Supports:
        - for row in mydata:
              # Do something with the row
        - mydata.headers()
        - len(mydata)  # Only counts data rows (not headers)
        - Index on rows or columns:
              mydata[51]  # Row at index 51
              mydata['Date']  # Date column
        - Get cells from rows or columns by column name
              mydata[51][mydata.col_index('Date')]
              mydata['Date'][51]
        - "ColumnName" in mydata  # Check if sheet has header/column name
        - Lazy load rows/columns with rowsi(), columni(), columnsi()
    """

    def __init__(self, rows_of_strings):
        if len(rows_of_strings) == 0:
            raise Exception('Empty CSV with no headers!')

        if len(rows_of_strings) == 1:
            # Has headers but no data (empty data rows)
            self._rows = []
        else:
            self._rows = rows_of_strings[1:]
        self._headers = rows_of_strings[0]

    def __getitem__(self, item):
        # Column names return a column
        if isinstance(item, str):
            if item not in self._headers:
                raise ValueError("Column name must be in known headers()!")
            index = self._headers.index(item)
            return [row[index] for row in self._rows]
        elif isinstance(item, int):
            return self._rows[item]
        else:
            raise ValueError("Must provide a string column name or row index!")

    def __contains__(self, item):
        if item in self._rows[0]:
            return True
        return False

    def __len__(self):
        return len(self._rows)

    def __iter__(self):
        return (list(row) for row in self._rows)

    def is_empty(self):
        return len(self._rows) == 0

    def headers(self):
        return list(self._headers)

    def rowsi(self):
        # Iterator (lazy load) over rows
        return (list(row) for row in self._rows)

    def rows(self):
        return [list(row) for row in self.rowsi()]

    def col_index(self, column_name):
        return self._headers.index(column_name)

    def columni(self, item):
        # Iterator (lazy-load) over a column
        if isinstance(item, str):
            if item not in self._headers:
                raise ValueError("Column name must be in known headers()!")
            index = self._headers.index(item)
            return (row[index] for row in self._rows)
        elif isinstance(item, int):
            return (row[item] for row in self._rows)
        else:
            raise TypeError("Must provide a string column name or row index!")

    def columnsi(self):
        # List of iterators (lazy load) for all columns
        return [self.columni(index) for index in range(len(self._headers))]

    def columns(self):
        return [self[colname] for colname in self._headers]


class Metrics(RowColumnView):

    TYPES = SimpleNamespace(
        TRAFFIC='TRAFFIC',
        SEARCH='SEARCH',
    )
    TRAFFIC_HEADERS = SimpleNamespace(  # Expected column names (in order)
        DATE='Date',
        VERSION='Version',
        PATH='Path',
        VIEWS='Views'
    )
    THDRS = TRAFFIC_HEADERS
    TRAFFIC_HDR_LIST = [TRAFFIC_HEADERS.DATE, TRAFFIC_HEADERS.VERSION,
                        TRAFFIC_HEADERS.PATH, TRAFFIC_HEADERS.VIEWS]
    SEARCH_HEADERS = SimpleNamespace(  # Expected column names (in order)
        CREATED_DATE='Created Date',
        QUERY='Query',
        TOTAL_RESULTS='Total Results',
    )
    SHDRS = SEARCH_HEADERS
    SEARCH_HDR_LIST = [SEARCH_HEADERS.CREATED_DATE, SEARCH_HEADERS.QUERY,
                       SEARCH_HEADERS.TOTAL_RESULTS]
    INPUTS = SimpleNamespace(
        CSV_STRING='CSV_STRING',
        FILEHANDLE='FILEHANDLE',
        PATH='PATH',
    )

    def __init__(self, rows_of_strings):
        # Validate/normalize columns before instantiating
        sheet = RowColumnView(rows_of_strings)
        normalized_data = self._normalize_sheet(sheet)

        super().__init__(normalized_data)

    @staticmethod
    def _normalize_sheet(sheet):
        """Take a RowColumnView and return plain rows of string lists, normalized"""
        # Keep only expected columns in expected order
        if not (set(sheet.headers()) >= set(Metrics.TRAFFIC_HDR_LIST)
                or (set(sheet.headers()) >= set(Metrics.SEARCH_HDR_LIST))):
            raise ValueError('Must provide valid traffic or search CSV data')

        # Figure out which columns we need to pull data from
        target_headers = (
            Metrics.TRAFFIC_HDR_LIST
            if set(sheet.headers()) >= set(Metrics.TRAFFIC_HDR_LIST)
            else Metrics.SEARCH_HDR_LIST
        )

        # Build rows with proper colnames and ordering
        normalized_rows = [list(target_headers)]
        for dirty_row in sheet:
            norm_row = [
                dirty_row[sheet.col_index(colname)]
                for colname in target_headers
            ]
            normalized_rows.append(norm_row)

        # Return string rows, for use with the base class constructor
        return normalized_rows

    def _clean_dups_and_merge(rows_of_strings):
        # Remove exact duplicate rows, and conflicting rows for partial days where
        # the view count is different (when date + version + path is the same but
        # view count is different for traffic CSVs, the smaller number is a partial
        # or earlier day so the larger number wins)...for search CSVs, same date/query
        # are duplicates (and query match count may differ but is not relevant for metrics)
        sheet = RowColumnView(rows_of_strings)
        source_headers = sheet.headers()
        if (not (set(source_headers) >= set(Metrics.SEARCH_HDR_LIST))
                and (not set(source_headers) >= set(Metrics.TRAFFIC_HDR_LIST))):
            raise ValueError('Cannot clean unknown CSV formats')
        cleaned = []

        # Clean exact duplicates where all columns are the same
        no_exact_row_duplicates = [source_headers]
        full_row_map = {}
        for row in sheet:
            row_tup = tuple(row)
            if row_tup in full_row_map:
                # Skip/don't append duplicate rows
                continue
            full_row_map[row_tup] = True
            no_exact_row_duplicates.append(row)
        # Reassign the sheet with the new data
        sheet = RowColumnView(no_exact_row_duplicates)

        no_conflicts = [source_headers]
        # Clean conflicting/partial day rows if this is a traffic CSV
        if set(source_headers) >= set(Metrics.TRAFFIC_HDR_LIST):
            unique_date_vers_path_map = {
                # Looks like
                # (date, vers, path): [row, row2]
                # Whole rows are stored for duplicates, most views wins
            }

            idate = sheet.col_index(Metrics.THDRS.DATE)
            ivers = sheet.col_index(Metrics.THDRS.VERSION)
            ipath = sheet.col_index(Metrics.THDRS.PATH)
            iviews = sheet.col_index(Metrics.THDRS.VIEWS)
            for row in sheet.rowsi():
                # Find conflicts
                fingerprint = (row[idate], row[ivers], row[ipath])
                unique_date_vers_path_map.setdefault(fingerprint, []).append(row)
            for row in sheet.rowsi():
                fingerprint = (row[idate], row[ivers], row[ipath])
                matching_rows = unique_date_vers_path_map[fingerprint]
                if len(matching_rows) == 1:
                    no_conflicts.append(row)
                else:
                    most_first = list(reversed(sorted(matching_rows, key=lambda r: int(r[iviews]))))
                    if most_first[0] == row:
                        no_conflicts.append(row)
        # Search CSVs don't need conflict resolution, only duplicate resolution
        elif set(source_headers) >= set(Metrics.SEARCH_HDR_LIST):
            no_conflicts.extend(sheet.rowsi())

        return no_conflicts

    @staticmethod
    def build(csv_string=None, filehandle=None, path=None, postproc=_clean_dups_and_merge):
        """Takes single items or lists of CSV sources, returns a merged sheet object"""
        if csv_string is None and filehandle is None and path is None:
            raise ValueError("Must provide a data source!")

        sources = [
            # List of dicts, like:
            # {
            #      'type': Metrics.INPUTS.FILEHANDLE,
            #      'data': RowColumnView(csv_to_rows_of_strings(foo)),
            #      'source': src_object
            # }
        ]
        if csv_string is not None:
            if not isinstance(csv_string, list):
                csv_string = [csv_string]
            for cstring in csv_string:
                sources.append({
                    'type': Metrics.INPUTS.CSV_STRING,
                    'data': RowColumnView(csv_to_rows_of_strings(csv_string=cstring)),
                    'source': cstring
                })
        if filehandle is not None:
            if not isinstance(filehandle, list):
                filehandle = [filehandle]
            for fhandle in filehandle:
                sources.append({
                    'type': Metrics.INPUTS.FILEHANDLE,
                    'data': RowColumnView(csv_to_rows_of_strings(filehandle=fhandle)),
                    'source': fhandle
                })
        if path is not None:
            if not isinstance(path, list):
                path = [path]
            for pth in path:
                sources.append({
                    'type': Metrics.INPUTS.PATH,
                    'data': RowColumnView(csv_to_rows_of_strings(path=pth)),
                    'source': pth
                })

        metrics_type = None
        sources_normalized = []
        for item in sources:
            # Figure out which metrics type we have
            source_sheet = item['data']
            if metrics_type is None:
                if set(source_sheet.headers()) >= set(Metrics.TRAFFIC_HDR_LIST):
                    metrics_type = Metrics.TYPES.TRAFFIC
                elif set(source_sheet.headers()) >= set(Metrics.SEARCH_HDR_LIST):
                    metrics_type = Metrics.TYPES.SEARCH
                else:
                    raise ValueError(f'Error, unknown data format for {item}')

            # Metrics hold (one of) either traffic data or search data,
            # only same-types are merged
            if ((metrics_type == Metrics.TYPES.TRAFFIC
                    and not set(source_sheet.headers()) >= set(Metrics.TRAFFIC_HDR_LIST))
                or (metrics_type == Metrics.TYPES.SEARCH
                    and not set(source_sheet.headers()) >= set(Metrics.SEARCH_HDR_LIST))):
                raise ValueError('Cannot merge disparate data types')

            sheet = RowColumnView(Metrics._normalize_sheet(source_sheet))
            if not sources_normalized:
                # Take normalized headers from the item as first string row
                sources_normalized.append(sheet.headers())
            sources_normalized.extend(sheet.rowsi())

        if postproc is not None:
            sources_normalized = postproc(sources_normalized)

        return Metrics(sources_normalized)

    def is_traffic(self):
        if set(self.headers()) >= set(Metrics.TRAFFIC_HDR_LIST):
            return True
        return False

    def is_search(self):
        if set(self.headers()) >= set(Metrics.SEARCH_HDR_LIST):
            return True
        return False

    def total_views(self):
        if not self.is_traffic():
            raise Exception('Cannot get views on non-traffic data')

        view_index = self.col_index(Metrics.THDRS.VIEWS)
        return sum(int(row[view_index]) for row in self._rows)

    def most_popular_queries(self, n=None):
        if not self.is_search():
            raise TypeError('Cannot get query counts for non-search data')
        counts = collections.Counter()

        headers = self.headers()
        query_hdr_index = headers.index(Metrics.SHDRS.QUERY)
        for row in self.rowsi():
            # Each row is a search, so the row adds 1 to the count (query
            # match count is also in each search row, but since the number
            # of matches is not needed/relevant, we don't use it)
            counts[row[query_hdr_index]] += 1
        return counts.most_common() if n is None else counts.most_common(n)

    def most_popular_pages(self, n=None):
        if not self.is_traffic():
            raise TypeError('Cannot get traffic counts for non-traffic data')
        counts = collections.Counter()

        headers = self.headers()
        path_hdr_index = headers.index(Metrics.THDRS.PATH)
        views_hdr_index = headers.index(Metrics.THDRS.VIEWS)
        for row in self.rowsi():
            counts[row[path_hdr_index]] += int(row[views_hdr_index])
        return counts.most_common() if n is None else counts.most_common(n)

    def most_popular_versions(self, n=None):
        if not self.is_traffic():
            raise TypeError('Cannot get version counts for non-traffic data')
        counts = collections.Counter()

        headers = self.headers()
        path_hdr_index = headers.index(Metrics.THDRS.VERSION)
        views_hdr_index = headers.index(Metrics.THDRS.VIEWS)
        for row in self.rowsi():
            counts[row[path_hdr_index]] += int(row[views_hdr_index])
        return counts.most_common() if n is None else counts.most_common(n)
