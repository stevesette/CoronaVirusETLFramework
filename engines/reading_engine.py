import yaml
import datetime


class ReadingEngine:
    def __init__(self, yaml_file):
        self.file_name = yaml_file
        self.fields = {
            'counties':
                ['county', 'new_cases', 'new_deaths', 'cases', 'deaths',
                 'mortality_rate', 'mortality_rate_delta', 'new_cases_delta',
                 'new_deaths_delta', 'recoveries', 'recovery_rate'],
            'tests':
                ['positive', 'negative', 'total_results',
                 'total_tests', 'test_positive_rate', 'test_negative_rate']
        }
        self.categories = ['min', 'max', 'mean', 'sum', 'fields']
        self.data = self.yaml_handler()

    def read_config(self, filepath):
        if not filepath.endswith(".yaml"):
            raise RuntimeError("Not a yaml file")
        with open(filepath) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        return data

    def field_checker(self, d):
        for category in self.categories:
            if category in d['aggregate'].keys():
                for field in d['aggregate'][category]:
                    if field not in self.fields['counties'] and field not in self.fields['tests']:
                        print("field not found in dataset")
                        return False
        return True

    def get_group_by(self):
        groups = []
        d = self.data['aggregate']
        if 'area' in d.keys():
            groups.append(self.area)
        if 'window' in d.keys():
            groups.append("date")
        return groups

    def get_functions(self):
        d = {}
        for key in self.data['aggregate'].keys():
            if key in self.categories:
                d[key] = self.data['aggregate'][key]
        return d

    def file_picker(self):
        d = self.data
        needed_files = []
        self.needed_columns = set()
        counties = 'data/us-counties.csv'
        tests = 'data/tests-by-state.csv'
        for category in self.categories:
            if category in d['aggregate'].keys():
                for field in d['aggregate'][category]:
                    self.needed_columns.add(field)
                    if field in self.fields['counties'] and counties not in needed_files:
                        needed_files.append(counties)
                    if field in self.fields['tests'] and tests not in needed_files:
                        needed_files.append(tests)
        return needed_files

    def get_columns(self):
        return self.needed_columns

    def area_checker(self, d):
        agg = d['aggregate']
        accepted = ['state', 'region', 'county']
        if 'area' not in agg.keys():
            self.area = 'county'
            return True
        if 'area' in agg.keys():
            if agg['area'] in accepted:
                self.area = agg['area']
                return True
            else:
                print(agg['area'] + ' is not accepted as an area.')

    def get_area(self):
        return self.area

    def date_checker(self, date_data):
        dates = [date_data['start_date'], date_data['end_date']]
        for date in dates:
            month, day, year = date.split('/')
            try:
                datetime.datetime(int(year), int(month), int(day))
            except ValueError:
                print('The dates you entered are invalid')
                return False
        return True

    def timing_checker(self, date_data):
        return date_data['start_date'] <= date_data['end_date']

    def window_checker(self, d):
        agg = d['aggregate']
        if 'window' not in agg.keys():
            return True
        if 'window' in agg.keys():
            if 'start_date' in agg['window'].keys() and 'end_date' in agg['window'].keys():
                if self.date_checker(agg['window']):
                    if self.timing_checker(agg['window']):
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

    def output_checker(self, d):
        if d['output_method'] == 'terminal' or '.csv' in d['output_method'] or '.txt' in d['output_method']:
            return True
        else:
            print("Invalid output method")
            return False

    def get_compare(self):
        if 'compare' in self.data['aggregate'].keys():
            return self.data['aggregate']['compare']
        else:
            None

    def window_creator(self, d):
        data = d['aggregate']
        if 'window' not in data:
            window = None
        else:
            window = (data['window']['start_date'], data['window']['end_date'])
        return window

    def window_getter(self):
        return self.window_creator(self.data)

    def yaml_handler(self):
        filepath = 'config_files/' + self.file_name
        data = self.read_config(filepath)
        if \
                (
                        self.window_checker(data) and
                        self.field_checker(data) and
                        self.output_checker(data) and
                        self.area_checker(data)
                ):
            return data
        else:
            raise RuntimeError('Error reading config file')

# def handle_yaml_parameters(parameters):
#     pass

    def get_output(self):
        return self.data['output_method']

# def main_driver(filepath):
#     file = read_config(filepath)
#     yaml_ran = handle_yaml_parameters(file)
#     output(file)
#
