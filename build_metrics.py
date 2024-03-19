"""Read and merge subproject metrics CSVs and build metrics outputs"""


import json
import matplotlib as mpl
import matplotlib.pyplot as plt
import os
import shutil
import traceback

from pprint import pprint
from types import SimpleNamespace

from doc_metrics import csv_to_rows_of_strings, RowColumnView, Metrics


DATA_DIR = 'subproject_csvs'
OUTPUT_DIR = 'metrics_output'


def build_metrics():
    print("[BldMetrics] **** Begin metrics build ****")

    # Build separate metrics for each subproject
    all_subproj_metrics = {}
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
    for fpath in os.listdir(DATA_DIR):
        proj_dir = os.path.join(os.path.abspath(DATA_DIR), fpath)
        print(f'\n[BldMetrics] Checking item in data path: {fpath}')

        # CSV files should only be inside a subproj folder
        if not os.path.isdir(proj_dir):
            files_skipped.append(proj_dir)
            print('[BldMetrics]   Skipped')
            continue

        # Traverse all subdirs and grab any CSVs
        print('[BldMetrics]   Searching...')
        subproj_metrics = {
            Metrics.TYPES.TRAFFIC: None,
            Metrics.TYPES.SEARCH: None,
        }
        all_subproj_metrics[proj_dir] = subproj_metrics
        traffic_files = []
        search_files = []
        for dirpath, dirnames, filenames in os.walk(proj_dir):
            for fname in filenames:
                fpath = os.path.join(dirpath, fname)
                if not fname.lower().endswith('.csv'):
                    print(f'[BldMetrics]     Skip file: {os.path.relpath(os.path.join(dirpath, fname), DATA_DIR)}')
                    files_skipped.append(fpath)
                    continue

                # Load the CSV and check if it's valid
                try:
                    print(f'[BldMetrics]     Load CSV: {os.path.relpath(os.path.join(dirpath, fname), DATA_DIR)}')
                    met = Metrics.build(path=fpath)
                    if not (met.is_traffic() or met.is_search()):
                        err_info = {'tag': 'BAD_CSV_FORMAT', 'data': fpath}
                        log_data['errors'].append(err_info)
                        files_errors.append(fpath)
                        print(f'[BldMetrics]       Bad CSV format')

                        continue

                    # Add the path to the right target path list
                    if met.is_traffic():
                        traffic_files.append(fpath)
                        print(f'[BldMetrics]       Traffic data found')
                    if met.is_search():
                        search_files.append(fpath)
                        print(f'[BldMetrics]       Search data found')
                    files_success.append(fpath)

                except Exception as err:
                    tb = traceback.format_exc()
                    err_info = {'tag': 'ERROR_READING_FILE', 'data': fpath, 'traceback': tb}
                    log_data['errors'].append(err_info)
                    files_errors.append(fpath)
                    print(f'[BldMetrics]     Error during file read')

                    continue

        # Merge/compile metrics by type
        print('[BldMetrics]   Begin metrics merge...')
        # ....
        # Traffic
        try:
            if traffic_files:
                traffic_metrics = Metrics.build(path=traffic_files)
                subproj_metrics[Metrics.TYPES.TRAFFIC] = traffic_metrics
                print('[BldMetrics]     ...merged traffic CSVs')
            else:
                print(f'[BldMetrics]     Warning: no traffic metrics!')
        except Exception as err:
            tb = traceback.format_exc()
            err_info = {'tag': 'ERROR_MERGING_PROJ_TRAFFIC_CSVS', 'data': proj_dir, 'traceback': tb}
            log_data['errors'].append(err_info)
            print(f'[BldMetrics]     Error merging/building traffic CSVs!')
        # ....
        # Search
        try:
            if search_files:
                search_metrics = Metrics.build(path=search_files)
                subproj_metrics[Metrics.TYPES.SEARCH] = search_metrics
                print('[BldMetrics]     ...merged search CSVs')
            else:
                print(f'[BldMetrics]     Warning: no search metrics!')
        except Exception as err:
            tb = traceback.format_exc()
            err_info = {'tag': 'ERROR_MERGING_PROJ_SEARCH_CSVS', 'data': proj_dir, 'traceback': tb}
            log_data['errors'].append(err_info)
            print(f'[BldMetrics]     Error merging/building search CSVs!')

        all_subproj_metrics[proj_dir] = subproj_metrics

    # Build outputs/reporting for each subproject
    print('\n[BldMetrics] ---- Begin output generation ----')
    for proj_name, proj_metrics in all_subproj_metrics.items():
        traffic_metrics = proj_metrics[Metrics.TYPES.TRAFFIC]
        search_metrics = proj_metrics[Metrics.TYPES.SEARCH]

        # Determine/prep the output folder for this subproject
        if traffic_metrics or search_metrics:
            print(f'[BldMetrics] Making output folder for: {os.path.basename(proj_name)}')
            try:
                proj_output_dir = os.path.join(OUTPUT_DIR, os.path.basename(proj_name))
                os.makedirs(proj_output_dir, exist_ok=True)
            except Exception:
                pass

            if not os.path.exists(proj_output_dir):
                raise Exception('  Could not make output directory!')
        else:
            print(f'[BldMetrics] Warning, this project has no metrics! Skipping: {os.path.basename(proj_name)}')
            continue

        # Write any traffic metrics
        if traffic_metrics:
            try:
                output_path = os.path.join(proj_output_dir, 'popular_pages.png')

                # Gather plot data
                views = traffic_metrics.total_views()
                pop_pages = traffic_metrics.most_popular_pages(25)
                # pop_versions = traffic_metrics.most_popular_versions(25)

                # Build/write the plot to the project output folder
                fig = plt.figure()
                ax = fig.subplots()
                # ....
                fig.suptitle('Most Popular Pages')
                ax.invert_yaxis()
                ax.barh([i[0] for i in pop_pages], [i[1] for i in pop_pages])
                plt.savefig(output_path, bbox_inches="tight")
                print('[BldMetrics]   Traffic metrics write success')
            except Exception as err:
                tb = traceback.format_exc()
                err_info = {'tag': 'ERROR_WRITING_TRAFFIC_OUTPUT', 'data': proj_name, 'traceback': tb}
                log_data['errors'].append(err_info)
                print(f'[BldMetrics]   Error writing traffic outputs!')

        # Write any search metrics
        if search_metrics:
            try:
                output_path = os.path.join( proj_output_dir, 'popular_queries.png')

                # Gather plot data
                pop_queries = search_metrics.most_popular_queries(25)

                # Build/write the plot to the project output folder
                fig = plt.figure()
                ax = fig.subplots()
                # ....
                fig.suptitle('Most Popular Queries')
                ax.invert_yaxis()
                ax.barh([i[0] for i in pop_queries], [i[1] for i in pop_queries])
                plt.savefig(output_path, bbox_inches="tight")
                print('[BldMetrics]   Search metrics write success')
            except Exception as err:
                tb = traceback.format_exc()
                err_info = {'tag': 'ERROR_WRITING_SEARCH_OUTPUT', 'data': proj_name, 'traceback': tb}
                log_data['errors'].append(err_info)
                print(f'[BldMetrics]   Error writing search outputs!')

    with open(os.path.join(OUTPUT_DIR, 'metrics_build.log'), 'wb') as fhandle:
        fhandle.write(json.dumps(log_data).encode('utf8'))


if __name__ == '__main__':
    build_metrics()
