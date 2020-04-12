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


fields = {
    'counties':
        ['county', 'new_cases', 'new_deaths', 'cases', 'deaths',
         'mortality_rate', 'mortality_rate_delta', 'new_cases_delta',
         'new_deaths_delta', 'recoveries', 'recovery_rate'],
    'tests':
        ['positive', 'positive_tests', 'negative', 'totalResults',
         'total_tests', 'negative_tests', 'test_positive_rate', 'test_negative_rate']
}
categories = ['min', 'max', 'mean', 'sum', 'fields']


def field_checker(field_dict, d):
    for category in categories:
        if category in d['aggregate'].keys():
            for field in d['aggregate'][category]:
                if field not in field_dict['counties'] and field not in field_dict['tests']:
                    return False
    return True


def file_picker(field_dict, d):
    needed_files = []
    counties = 'CoronaVirusETLFramework/data/us-counties.csv'
    tests = 'CoronaVirusETLFramework/data/tests-by-state.csv'
    for category in categories:
        if category in d['aggregate'].keys():
            for field in d['aggregate'][category]:
                if field in field_dict['counties'] and counties not in needed_files:
                    needed_files.append(counties)
                if field in field_dict['tests'] and tests not in needed_files:
                    needed_files.append(tests)
    return needed_files


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
            return True


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


def input_output_checker(d):
    if d['output_method'] == 'console':
        return True
    else:
        return False


def window_creator(d):
    data = d['aggregate']
    if 'window' not in data:
        window = None
    else:
        window = (data['window']['start_date'], data['window']['end_date'])
    return window


def yaml_handler(filepath):
    data = read_config(filepath)
    if \
            (
                    window_checker(data) and
                    field_checker(fields, data) and
                    top_n_checker(data) and
                    input_output_checker(data) and
                    area_checker(data)
            ):
        return True
    else:
        return False


print(yaml_handler('/Users/Tim/Desktop/Final-4300-Project/CoronaVirusETLFramework/config_files/compare.yaml'))

# def handle_yaml_parameters(parameters):
#     pass

# def output(output_object):
#     pass

# def main_driver(filepath):
#     file = read_config(filepath)
#     yaml_ran = handle_yaml_parameters(file)
#     output(file)
#
