import os
import shutil


# path to store results
path_storing_results = '/Users/aaronyu/Dropbox/vsystems/node_graph/results'


def check_directory_storing_results(clear_old_results=False):
    if not os.path.exists(path_storing_results):
        os.makedirs(path_storing_results)

    if clear_old_results:
        shutil.rmtree(path_storing_results)
        os.makedirs(path_storing_results)

