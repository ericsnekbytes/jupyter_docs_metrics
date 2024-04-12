"""Read and merge subproject metrics CSVs and build metrics outputs"""


import argparse
import csv
import datetime
import json
import logging
import os
import pprint
import re
import shutil
import sys
import traceback

from types import SimpleNamespace

from bokeh.plotting import figure, show, output_file, save, output_notebook, output_file
from mako.lookup import TemplateLookup
from mako.template import Template

from doc_metrics import csv_to_rows_of_strings, RowColumnView, Metrics


STATUS_OK = 0
STATUS_WARNINGS = 1
STATUS_ERRORS = 2
WARNING = 'WARNING'
ERROR = 'ERROR'
DATA_DIR = 'subproject_csvs'
OUTPUT_DIR = 'metrics_output'
LOGFILE = 'metrics_build.log'
logger = logging.getLogger(__name__)


def write_traffic_outputs(proj_name, proj_output_dir, proj_metadata, traffic_metrics):
    """Take subproject traffic data and write output files"""
    try:

        # Write merged CSV data for users to tinker with if desired
        logger.info(f'[BldMetrics]   Write merged traffic csv file')
        merged_csv_path = os.path.join(
            proj_output_dir,
            re.sub(r'[^A-Za-z0-9]', '_', os.path.basename(proj_name)) + '_traffic.csv'
        )
        with open(merged_csv_path, 'w', encoding='utf8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(traffic_metrics.headers())
            for row in traffic_metrics:
                writer.writerow(row)
        proj_metadata['merged_traffic_csv_path'] = os.path.join('.', merged_csv_path)
        popular_pages = traffic_metrics.most_popular_pages()
        if popular_pages:
            proj_metadata['popular_pages'] = popular_pages

        # Compile some data/info from the metrics
        dates = sorted([
            datetime.datetime.strptime(d, '%Y-%m-%d %X')
            for d in traffic_metrics[Metrics.THDRS.DATE]
        ])
        unique_dates = sorted(list(set(dates)))
        proj_metadata['latest_traffic_data_date'] = unique_dates[-1]
        proj_metadata['traffic_data_span'] = (unique_dates[-1] - unique_dates[0]).days if len(unique_dates) > 1 else 1
        proj_metadata['total_views'] = sum(i[1] for i in popular_pages)
        DAYS_IN_WEEK = 7
        most_pop = sorted(traffic_metrics.most_popular_pages(25), key=lambda item: item[1])
        vals_independent = [i[0] for i in most_pop]
        vals_dependent = [i[1] / len(unique_dates) * DAYS_IN_WEEK for i in most_pop]

        # views = traffic_metrics.total_views()
        # pop_versions = traffic_metrics.most_popular_versions(25)

        # Write interactive HTML plots
        plot1_path = os.path.join(proj_output_dir, 'popular_pages.html')
        proj_metadata['plot1_path'] = os.path.join('.', plot1_path)

        # Build/write the plot to the project output folder
        p = figure(y_range=[i[0] for i in most_pop], title="Popular Pages (Top 25)", x_axis_label='Avg. Views per Week', y_axis_label='Page', width=625, height=400)
        p.hbar(y=vals_independent, right=vals_dependent)

        output_file(filename=plot1_path, title="Static HTML file")
        save(p)

    except Exception as err:
        tb = traceback.format_exc()
        logger.error('[BldMetrics]   [E] Exception occurred, details:\n' + tb)
        logger.error(f'[BldMetrics]   [E] Error writing traffic outputs for: {proj_name}')


def write_search_outputs(proj_name, proj_output_dir, proj_metadata, search_metrics):
    """Take subproject search data and write output files"""
    try:

        # Write merged CSV data for users to tinker with if desired
        logger.info(f'[BldMetrics]   Write merged search csv file')
        merged_csv_path = os.path.join(
            proj_output_dir,
            re.sub(r'[^A-Za-z0-9]', '_', os.path.basename(proj_name)) + '_search.csv'
        )
        with open(merged_csv_path, 'w', encoding='utf8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(search_metrics.headers())
            for row in search_metrics:
                writer.writerow(row)
        proj_metadata['merged_search_csv_path'] = os.path.join('.', merged_csv_path)
        popular_searches = search_metrics.most_popular_queries()
        if popular_searches:
            proj_metadata['popular_searches'] = popular_searches

        # Compile some data/info from the metrics
        dates = sorted([
            datetime.datetime.strptime(d, '%Y-%m-%d %X')
            for d in search_metrics[Metrics.SHDRS.CREATED_DATE]
        ])
        unique_dates = sorted(list(set(dates)))
        proj_metadata['latest_search_data_date'] = unique_dates[-1]
        proj_metadata['search_data_span'] = (unique_dates[-1] - unique_dates[0]).days if len(unique_dates) > 1 else 1
        proj_metadata['total_searches'] = sum(i[1] for i in popular_searches)
        DAYS_IN_WEEK = 7
        most_pop = sorted(search_metrics.most_popular_queries(25), key=lambda item: item[1])
        vals_independent = [i[0] for i in most_pop]
        vals_dependent = [i[1] / len(unique_dates) * DAYS_IN_WEEK for i in most_pop]

        # Write interactive HTML plots
        plot2_path = os.path.join(proj_output_dir, 'popular_queries.html')
        proj_metadata['plot2_path'] = os.path.join('.', plot2_path)

        # Build/write the plot to the project output folder
        p = figure(y_range=[i[0] for i in most_pop], title="Popular Searches (Top 25)", x_axis_label='Searches per Week', y_axis_label='Page', width=625, height=400)
        p.hbar(y=vals_independent, right=vals_dependent)

        output_file(filename=plot2_path, title="Static HTML file")
        save(p)

    except Exception as err:
        tb = traceback.format_exc()
        logger.error('[BldMetrics]   [E] Exception occurred, details:\n' + tb)
        logger.error(f'[BldMetrics]   [E] Error writing search outputs for: {proj_name}')


def build_metrics():
    logger.info('[BldMetrics] **** Begin metrics build ****')
    logger.info(f'[BldMetrics] Started at {datetime.datetime.now().isoformat()}')

    # Hold/build compiled info about every project here
    all_project_metadata = {}
    if os.path.exists(OUTPUT_DIR):
        try:
            shutil.rmtree(OUTPUT_DIR)
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            logger.info(f'[BldMetrics] Old outputs removed successfully')
        except Exception as err:
            tb = traceback.format_exc()
            logger.error('[BldMetrics]   [E] Exception occurred, details:\n' + tb)
            raise Exception('[E] Error removing old output files') from err

    # Start looking for subproject folders in the data dir
    problems = []  # A simple list with string tuples describing warnings/errors
    for proj_path in os.listdir(DATA_DIR):
        proj_dir = os.path.join(os.path.abspath(DATA_DIR), proj_path)
        logger.info(f'\n[BldMetrics] Checking item in data path: {proj_path}')

        # CSV files should only be inside a subproj folder
        if not os.path.isdir(proj_dir):
            logger.warning(f'[BldMetrics]   Skipped orphan file in project folder: {proj_dir}')
            continue

        # Compile project metadata here
        proj_metadata = {
            'traffic_data': None,
            'traffic_inputs': None,
            'merged_traffic_csv_path': None,
            'traffic_empty': False,
            'popular_pages': None,
            'latest_traffic_data_date': None,
            'traffic_data_span': None,
            'total_views': None,

            'search_data': None,
            'search_inputs': None,
            'merged_search_csv_path': None,
            'search_empty': False,
            'popular_searches': None,
            'latest_search_data_date': None,
            'search_data_span': None,
            'total_searches': None,

            'plot1_path': None,
            'plot2_path': None,
        }
        all_project_metadata[proj_path] = proj_metadata

        # Traverse all subdirs and grab any CSVs
        logger.info('[BldMetrics]   Searching...')
        proj_traffic_csvs = []
        proj_search_csvs = []
        proj_metadata['traffic_inputs'] = proj_traffic_csvs
        proj_metadata['search_inputs'] = proj_search_csvs
        for dirpath, dirnames, filenames in os.walk(proj_dir):
            for target in filenames:
                tgt_path = os.path.join(dirpath, target)
                if not target.lower().endswith('.csv'):
                    logger.warning(f'[BldMetrics]     Skip file: {os.path.relpath(os.path.join(dirpath, target), DATA_DIR)}')
                    problems.append((WARNING, 'SKIPPED_FILE', tgt_path))
                    continue

                # Load the CSV and check if it's valid
                try:
                    logger.info(f'[BldMetrics]     Load CSV: {os.path.relpath(os.path.join(dirpath, tgt_path), DATA_DIR)}')
                    met = Metrics.build(path=tgt_path)
                    if not (met.is_traffic() or met.is_search()):
                        logger.error(f'[BldMetrics]       [E] Bad CSV format: {tgt_path}')
                        problems.append((ERROR, 'BAD_CSV_FMT', tgt_path))

                        continue
                    if met.is_empty():
                        if met.is_traffic():
                            logger.warning(f'[BldMetrics]       [W] Bad traffic CSV (Empty data rows): {tgt_path}')
                            proj_metadata['traffic_empty'] = True
                        if met.is_search():
                            logger.warning(f'[BldMetrics]       [W] Bad search CSV (Empty data rows): {tgt_path}')
                            proj_metadata['search_empty'] = True
                        problems.append((WARNING, 'EMPTY_CSV', tgt_path))

                        continue

                    # Add the path to the right target path list
                    if met.is_traffic():
                        proj_traffic_csvs.append(tgt_path)
                        logger.info(f'[BldMetrics]       Traffic data found')
                    if met.is_search():
                        proj_search_csvs.append(tgt_path)
                        logger.info(f'[BldMetrics]       Search data found')

                except Exception as err:
                    tb = traceback.format_exc()
                    logger.error('[BldMetrics]   [E] Exception occurred, details:\n' + tb)
                    logger.error(f'[BldMetrics]       [E] Error during file read: {tgt_path}')
                    problems.append((ERROR, 'FILE_READ_ERR', tgt_path, tb))

                    continue

        if not (proj_traffic_csvs or proj_search_csvs):
            logger.warning('[BldMetrics]   Warning: No valid metrics were found for this project...')
            problems.append((WARNING, 'MISSING_METRICS', os.path.basename(proj_dir)))
            continue

        # Merge/compile metrics by type
        logger.info('[BldMetrics]   Begin metrics merge...')
        try:
            # Build aggregated traffic data
            if proj_traffic_csvs:
                traffic_metrics = Metrics.build(path=proj_traffic_csvs)
                proj_metadata['traffic_data'] = traffic_metrics
                logger.info('[BldMetrics]     ...merged traffic CSVs')
            else:
                logger.warning(f'[BldMetrics]     Warning: no traffic metrics!')
                problems.append((WARNING, 'NO_TRAFFIC_DATA', os.path.basename(proj_dir)))
        except Exception as err:
            tb = traceback.format_exc()
            logger.error('[BldMetrics]   [E] Exception occurred, details:\n' + tb)
            logger.error(f'[BldMetrics]     [E] Error merging/building traffic CSVs: {proj_dir}')
            problems.append((ERROR, 'ERR_MERGING_TRAFFIC_CSVS', os.path.basename(proj_dir), tb))
        try:
            # Build aggregated search data
            if proj_search_csvs:
                search_metrics = Metrics.build(path=proj_search_csvs)
                proj_metadata['search_data'] = search_metrics
                logger.info('[BldMetrics]     ...merged search CSVs')
            else:
                logger.warning(f'[BldMetrics]     Warning: no search metrics!')
                problems.append((WARNING, 'NO_SEARCH_DATA', os.path.basename(proj_dir)))
        except Exception as err:
            tb = traceback.format_exc()
            logger.error('[BldMetrics]   [E] Exception occurred, details:\n' + tb)
            logger.error(f'[BldMetrics]     [E] Error merging/building search CSVs: {proj_dir}')
            problems.append((ERROR, 'ERR_MERGING_SEARCH_CSVS', os.path.basename(proj_dir), tb))

    # Build outputs/reporting for each subproject
    logger.error('\n[BldMetrics] ---- Begin output generation ----')
    proj_order = [item for item in ['Jupyter Notebook', 'JupyterLab', 'JupyterHub', 'Jupyter Server'] if item in all_project_metadata]
    proj_order.extend(item for item in all_project_metadata if item not in proj_order)
    for proj_name, proj_metadata in all_project_metadata.items():
        traffic_metrics = proj_metadata['traffic_data']
        search_metrics = proj_metadata['search_data']
        if traffic_metrics is None and search_metrics is None:
            logger.warning(f'[BldMetrics] Skipping outputs for project without valid data: {proj_name}')
            problems.append((WARNING, 'NO_METRICS', os.path.basename(proj_name)))
            continue

        # Ensure destination/output dirs exist before writing outputs to disk
        try:
            logger.info(f'[BldMetrics] Making output folder for: {os.path.basename(proj_name)}')
            proj_output_dir = os.path.join(OUTPUT_DIR, os.path.basename(proj_name))
            os.makedirs(proj_output_dir, exist_ok=True)
        except Exception:
            pass
        if not os.path.exists(proj_output_dir):
            raise Exception('  Could not make output directory!')

        # Build outputs for traffic data
        if traffic_metrics:
            write_traffic_outputs(proj_name, proj_output_dir, proj_metadata, traffic_metrics)

        # Build outputs for search data
        if search_metrics:
            write_search_outputs(proj_name, proj_output_dir, proj_metadata, search_metrics)

    # Build the summary page, with a section for each subproject found in the DATA_DIR
    # (Mako consumes the homepage HTML template file and adds entries per subproject)
    metrics_lookup = TemplateLookup(directories=['templates'])
    metrics_page_templ = metrics_lookup.get_template("index.html.template")
    # Turn project dicts into objects for easy access in the template
    project_page_values = [
        SimpleNamespace(
            name=key,
            **all_project_metadata[key]
        ) for key in proj_order
    ]
    output_page = metrics_page_templ.render(
        subprojects=project_page_values
    )
    with open(r'index.html', 'w', encoding='utf8') as fhandle:
        fhandle.write(output_page)

    return problems


if __name__ == '__main__':
    # TODO Gather CLI options
    parser = argparse.ArgumentParser(
        description='Metrics builder for Jupyter ReadTheDocs site stats.'
    )
    parser.add_argument(
        '--strict',
        action='store_true',
        help='Force failues on invalid data',
    )
    args = parser.parse_args()

    # Set up logs
    logger.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(message)s')
    console_output_handler = logging.StreamHandler()
    logfile_handler = logging.FileHandler(
        filename=LOGFILE,
        encoding='utf8',
        mode='w',
    )
    logger.addHandler(console_output_handler)
    logger.addHandler(logfile_handler)

    # Start the build process
    problems = build_metrics()

    exit_code = STATUS_OK
    logger.info(f'\n[BldMetrics] Summary of issues ({len(problems)}){":" + chr(10) if problems else ": (None)"}')
    logger.info(pprint.pformat(problems))
    if args.strict and problems:
        # There's at least 1 warning and/or error
        exit_code = STATUS_ERRORS if 'ERRORS' in [item[0] for item in problems] else STATUS_WARNINGS
    sys.exit(exit_code)
