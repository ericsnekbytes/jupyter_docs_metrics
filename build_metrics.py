"""Read and merge subproject metrics CSVs and build metrics outputs"""


import csv
import json
import matplotlib as mpl
import matplotlib.pyplot as plt
import os
import re
import shutil
import traceback

from pprint import pprint
from types import SimpleNamespace

from bokeh.plotting import figure, show, output_file, save, output_notebook, output_file
from mako.lookup import TemplateLookup
from mako.template import Template

from doc_metrics import csv_to_rows_of_strings, RowColumnView, Metrics


DATA_DIR = 'subproject_csvs'
OUTPUT_DIR = 'metrics_output'
# WEB_INFO = SimpleNamespace(
#     WEB='WEB',
#     traffic_csv_link='traffic_csv_link',
#     search_csv_link='search_csv_link',
#     notebook_link='notebook_link',
#     popular_pages_link='popular_pages_link',
#     popular_queries_link='popular_queries_link',
# )


def build_metrics():
    print("[BldMetrics] **** Begin metrics build ****")

    # Hold/build compiled info about every project here
    all_project_metadata = {}
    log_data = {  # Holds metadata and errors about the build process
        'metadata': [],
        'errors': [],
        # ^Each list item looks like:
        # {'tag': 'DESCRIPTIVE_TAG', 'data': whatever_you_want, 'extra_key': some_value}
        # {'tag': 'BAD_FILE_OPEN', 'data': formatted_traceback_text}
        # {'tag': 'successful_files', 'data': list_of_files}
    }
    files_success = []
    files_errors = []
    files_skipped = []
    log_data['metadata'].append({'tag': 'files_success', 'data': files_success})
    log_data['metadata'].append({'tag': 'files_errors', 'data': files_errors})
    log_data['metadata'].append({'tag': 'files_skipped', 'data': files_skipped})
    if os.path.exists(OUTPUT_DIR):
        try:
            shutil.rmtree(OUTPUT_DIR)
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            print(f'[BldMetrics] Old outputs removed successfully')
        except Exception as err:
            # tb = traceback.format_exc()
            # log_data['errors'].append({'tag': 'Error removing output dir...', 'data': tb})
            raise Exception('Error removing old output files') from err

    # Start looking for subproject folders in the data dir
    for proj_path in os.listdir(DATA_DIR):
        proj_dir = os.path.join(os.path.abspath(DATA_DIR), proj_path)
        print(f'\n[BldMetrics] Checking item in data path: {proj_path}')

        # CSV files should only be inside a subproj folder
        if not os.path.isdir(proj_dir):
            files_skipped.append(proj_dir)
            print(f'[BldMetrics]   Skipped orphan file in project folder: {proj_dir}')
            continue

        # Compile project metadata here
        proj_metadata = {
            'traffic_data': None,
            'traffic_inputs': None,
            'merged_traffic_csv_path': None,

            'search_data': None,
            'search_inputs': None,
            'merged_search_csv_path': None,

            'plot1_path': None,
            'plot2_path': None,
        }
        all_project_metadata[proj_path] = proj_metadata

        # Traverse all subdirs and grab any CSVs
        print('[BldMetrics]   Searching...')
        proj_traffic_csvs = []
        proj_search_csvs = []
        proj_metadata['traffic_inputs'] = proj_traffic_csvs
        proj_metadata['search_inputs'] = proj_search_csvs
        for dirpath, dirnames, filenames in os.walk(proj_dir):
            for target in filenames:
                tgt_path = os.path.join(dirpath, target)
                if not target.lower().endswith('.csv'):
                    print(f'[BldMetrics]     Skip file: {os.path.relpath(os.path.join(dirpath, target), DATA_DIR)}')
                    files_skipped.append(tgt_path)
                    continue

                # Load the CSV and check if it's valid
                try:
                    print(f'[BldMetrics]     Load CSV: {os.path.relpath(os.path.join(dirpath, tgt_path), DATA_DIR)}')
                    met = Metrics.build(path=tgt_path)
                    if not (met.is_traffic() or met.is_search()):
                        err_info = {'tag': 'BAD_CSV_FORMAT', 'data': tgt_path}
                        log_data['errors'].append(err_info)
                        files_errors.append(tgt_path)
                        print(f'[BldMetrics]       Bad CSV format')

                        continue

                    # Add the path to the right target path list
                    if met.is_traffic():
                        proj_traffic_csvs.append(tgt_path)
                        print(f'[BldMetrics]       Traffic data found')
                    if met.is_search():
                        proj_search_csvs.append(tgt_path)
                        print(f'[BldMetrics]       Search data found')
                    files_success.append(tgt_path)

                except Exception as err:
                    tb = traceback.format_exc()
                    err_info = {'tag': 'ERROR_READING_FILE', 'data': tgt_path, 'traceback': tb}
                    log_data['errors'].append(err_info)
                    files_errors.append(tgt_path)
                    print(f'[BldMetrics]       Error during file read')

                    continue

        if not (proj_traffic_csvs or proj_search_csvs):
            print('[BldMetrics]   Warning: No valid metrics were found for this project...')
            continue

        # Merge/compile metrics by type
        print('[BldMetrics]   Begin metrics merge...')
        try:
            # Build aggregated traffic data
            if proj_traffic_csvs:
                traffic_metrics = Metrics.build(path=proj_traffic_csvs)
                proj_metadata['traffic_data'] = traffic_metrics
                print('[BldMetrics]     ...merged traffic CSVs')
            else:
                print(f'[BldMetrics]     Warning: no traffic metrics!')
        except Exception as err:
            tb = traceback.format_exc()
            err_info = {'tag': 'ERROR_MERGING_PROJ_TRAFFIC_CSVS', 'data': proj_dir, 'traceback': tb}
            log_data['errors'].append(err_info)
            print(f'[BldMetrics]     Error merging/building traffic CSVs!')
        try:
            # Build aggregated search data
            if proj_search_csvs:
                search_metrics = Metrics.build(path=proj_search_csvs)
                proj_metadata['search_data'] = search_metrics
                print('[BldMetrics]     ...merged search CSVs')
            else:
                print(f'[BldMetrics]     Warning: no search metrics!')
        except Exception as err:
            tb = traceback.format_exc()
            err_info = {'tag': 'ERROR_MERGING_PROJ_SEARCH_CSVS', 'data': proj_dir, 'traceback': tb}
            log_data['errors'].append(err_info)
            print(f'[BldMetrics]     Error merging/building search CSVs!')

    # Build outputs/reporting for each subproject
    print('\n[BldMetrics] ---- Begin output generation ----')
    for proj_name, proj_metadata in all_project_metadata.items():
        traffic_metrics = proj_metadata['traffic_data']
        search_metrics = proj_metadata['search_data']
        if traffic_metrics is None and search_metrics is None:
            print(f'[BldMetrics] Skipping outputs for project without valid data: {proj_name}')
            continue

        # Ensure destination/output dirs exist before writing outputs to disk
        try:
            print(f'[BldMetrics] Making output folder for: {os.path.basename(proj_name)}')
            proj_output_dir = os.path.join(OUTPUT_DIR, os.path.basename(proj_name))
            os.makedirs(proj_output_dir, exist_ok=True)
        except Exception:
            pass
        if not os.path.exists(proj_output_dir):
            raise Exception('  Could not make output directory!')

        # Build outputs for traffic data
        if traffic_metrics:
            try:

                # Write merged CSV data for users to tinker with if desired
                print(f'[BldMetrics]   Write merged traffic csv file')
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

                # Write interactive HTML plots
                plot1_path = os.path.join(proj_output_dir, 'popular_pages.html')
                proj_metadata['plot1_path'] = os.path.join('.', plot1_path)

                most_pop = traffic_metrics.most_popular_pages(25)
                # views = traffic_metrics.total_views()
                # pop_versions = traffic_metrics.most_popular_versions(25)

                # Build/write the plot to the project output folder
                p = figure(y_range=[i[0] for i in most_pop], title="Popular Pages", x_axis_label='Views', y_axis_label='Page')
                p.hbar(y=[i[0] for i in most_pop], right=[i[1] for i in most_pop])

                output_file(filename=plot1_path, title="Static HTML file")
                save(p)

            except Exception as err:
                tb = traceback.format_exc()
                err_info = {'tag': 'ERROR_WRITING_TRAFFIC_OUTPUT', 'data': proj_name, 'traceback': tb}
                log_data['errors'].append(err_info)
                print(f'[BldMetrics]   Error writing traffic outputs!')

        # Build outputs for search data
        if search_metrics:
            try:

                # Write merged CSV data for users to tinker with if desired
                print(f'[BldMetrics]   Write merged search csv file')
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

                # Write interactive HTML plots
                plot2_path = os.path.join( proj_output_dir, 'popular_queries.html')
                proj_metadata['plot2_path'] = os.path.join('.', plot2_path)

                most_pop = search_metrics.most_popular_queries(25)

                # Build/write the plot to the project output folder
                p = figure(y_range=[i[0] for i in most_pop], title="Popular Queries", x_axis_label='Views', y_axis_label='Page')
                p.hbar(y=[i[0] for i in most_pop], right=[i[1] for i in most_pop])

                output_file(filename=plot2_path, title="Static HTML file")
                save(p)

            except Exception as err:
                tb = traceback.format_exc()
                err_info = {'tag': 'ERROR_WRITING_SEARCH_OUTPUT', 'data': proj_name, 'traceback': tb}
                log_data['errors'].append(err_info)
                print(f'[BldMetrics]   Error writing search outputs!')

    # Build the summary page, with a section for each subproject found in the DATA_DIR
    # (Mako consumes the homepage HTML template file and adds entries per subproject)
    metrics_lookup = TemplateLookup(directories=['templates'])
    metrics_page_templ = metrics_lookup.get_template("index.html.template")
    # Turn project dicts into objects for easy access in the template
    project_page_values = [
        SimpleNamespace(
            name=key,
            **val
        ) for key, val in all_project_metadata.items()
    ]
    output_page = metrics_page_templ.render(
        subprojects=project_page_values
    )
    with open(r'index.html', 'w', encoding='utf8') as fhandle:
        fhandle.write(output_page)

    with open(os.path.join(OUTPUT_DIR, 'metrics_build.log'), 'wb') as fhandle:
        fhandle.write(json.dumps(log_data).encode('utf8'))


if __name__ == '__main__':
    build_metrics()
