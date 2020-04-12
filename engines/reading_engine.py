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


# This will contain all possible field names from our datasets, once they're finished
counties_fields = ['new_cases', 'new_deaths', 'cases', 'deaths', 'recoveries',
                   'mortality_rate', 'recovery_rate', 'new_recoveries',
                   'mortality_rate_delta', 'recovery_rate_delta']

tests_fields = ['state', 'date', 'positive', 'negative', 'totalResults', 'pending',
                'totalTests', 'hospitalizedCurrently', 'hospitalizedCumulative']
'min'
'max'
'mean'
'sum'
'fields'

def field_checker(field_list, d):
    if 'fields' not in d['aggregate'].keys():
        return True
    fields_in_dict = [value for value in d['aggregate']['fields']]
    for f in fields_in_dict:
        if f not in field_list:
            return False
    if 'rank_field' in d.keys():
        if d['rank_field'] not in field_list:
            return False
    return True


def top_n_checker(d):
    if 'top_n' not in d.keys():
        return True
    try:
        int(d['top_n'])
        return True
    except ValueError:
        return False

def area_checker(d):
    agg = d['aggregate']
    accepted = ['state', 'region', 'county']
    if 'area' not in agg.keys():
        return True
    if 'area' in agg.keys():
        if agg['area'] in accepted:
            return True\

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


date
county
state
fips
cases
deaths


state
date
positive
negative
totalResults
pending
totalTests
hospitalizedCurrently
hospitalizedCumulative


def window_checker(d):
    agg = d['aggregate']
    if 'window' not in agg.keys():
        return True
    if 'window' in agg.keys():
        if 'start_date' in agg['window'].keys() and 'end_date' in agg['window'].keys():
            if date_checker(agg['window']):
                if timing_checker(agg['window']):
                    return True
                else:
                    print('Your end_date comes before your start_date. Correct the issue in order to proceed.')
                    return False
            else:
                print('One or both of your dates is invalid. Reformat to follow: mm/dd/yyyy')
                return False
        else:
            print('Window is missing \'start_date\' and/or \'end_date\'')
            return False


# def most_least_checker(d):
#     if 'most_least' not in d.keys():
#         return True
#     if d['most_least'] == 'most' or d['most_least'] == 'least':
#         return True
#     return False



def input_output_checker(d):
    if d['input_file'] == 'data.csv' and d['output_method'] == 'console':
        return True

def window_creator(d):
    data = d['aggregate']
    if 'window' not in data:
        window = None
    else:
        window = (data['window']['start_date'], data['window']['end_date'])
    return window

def yaml_handler(filepath):
    data = read_config(filepath)
    if (
            window_checker(data) and
            field_checker(fields, data) and
            top_n_checker(data) and
            # most_least_checker(data) and
            input_output_checker(data) and
            area_checker(data)
            # Need to have something to check compare? Not sure on this since we're comparing states vs. counties, etc.
    ):
        window = window_creator(data)
        return window
    else:
        return 'False'


print(yaml_handler('/Users/Tim/Desktop/Final-4300-Project/CoronaVirusETLFramework/config_files/topN.yaml'))

# def handle_yaml_parameters(parameters):
#     pass

# def output(output_object):
#     pass

# def main_driver(filepath):
#     file = read_config(filepath)
#     yaml_ran = handle_yaml_parameters(file)
#     output(file)
#
