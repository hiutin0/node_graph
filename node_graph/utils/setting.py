import os


# path to store results
path_storing_results = '/Users/aaronyu/Dropbox/vsystems/node_graph/results'


def check_directory_storing_results():
    if not os.path.exists(path_storing_results):
        os.makedirs(path_storing_results)

