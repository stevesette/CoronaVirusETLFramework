import os

import yaml


def read_config(filepath):
    # if os.getcwd()
    # do some sort of error handle and or checking if a file exists
    if not filepath.endswith(".yaml"):
        raise RuntimeError("Not a yaml file")
    with open(filepath) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        print(data)

# def handle_yaml_parameters(parameters):
#     pass

# def output(output_object):
#     pass

# def main_driver(filepath):
    # file = read_config(filepath)
    # yaml_ran = handle_yaml_parameters(file)
    # output(file)

print(read_config('/Users/Tim/Desktop/Final 4300 Project/CoronaVirusETLFramework/topN.yaml'))
