import yaml
import datetime

def read_config(filepath):
    # if os.getcwd()
    # do some sort of error handle and or checking if a file exists
    if not filepath.endswith(".yaml"):
        raise RuntimeError("Not a yaml file")
    with open(filepath) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data

fields = []

def window_checker(filepath):
    data = read_config(filepath)
    if 'window' not in data.keys():
        return True
    if 'window' in data.keys():
        if date_checker(data['window']):
            if timing_checker(data['window']):
                return True
            else:
                print('Your end_date comes before your start_date. Correct the issue in order to proceed.')
                return False
        else:
            print('One or both of your dates is invalid. Reformat to follow: mm/dd/yyyy')
            return False

def date_checker(date_data):
    dates = [date_data['start_date'], date_data['end_date']]
    for date in dates:
        month, day, year = date.split('/')
        try:
            datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            return False
    return True

def timing_checker(date_data):
    return date_data['start_date'] <= date_data['end_date']

# def handle_yaml_parameters(parameters):
#     pass

# def output(output_object):
#     pass

# def main_driver(filepath):
    # file = read_config(filepath)
    # yaml_ran = handle_yaml_parameters(file)
    # output(file)
#
print(window_checker('/Users/Tim/Desktop/Final 4300 Project/CoronaVirusETLFramework/mostNew.yaml'))
