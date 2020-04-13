import yaml
import datetime


class ReadingEngine:
    def __init__(self, yaml_file):
        """
        #Creates the reading engine class
        #Sets a dictionary to contain the fields and user-calculated fields of our two datasets
        #Creates list of different aggregation functions we utilize
        #Stores the data once read and validated
        :param yaml_file: Gives file name for the yaml
        """
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
        """
        Converts yaml file into a dictionary in order to perform needed operations on it
        :param filepath: takes filepath of the desired yaml file
        :return: returns the yaml file as a dictionary
        """
        if not filepath.endswith(".yaml"):
            raise RuntimeError("Not a yaml file")
        with open(filepath) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        return data

    def field_checker(self, d):
        """
        This method validates all of the fields from the yaml file
        Checks in each indicated aggregated function, and checks that each field within those
        functions is in the dictionary of accepted fields
        :param d: takes the dictionary created by read_config
        :return: Boolean (true = data is valid). Returns True if all fields are valid
        """
        for category in self.categories:
            if category in d['aggregate'].keys():
                for field in d['aggregate'][category]:
                    if field not in self.fields['counties'] and field not in self.fields['tests']:
                        print("Field not found in dataset")
                        return False
        return True

    def get_group_by(self):
        """
        This method creates a list of the groupings the yaml file indicates for the final result
        data will be grouped by area first, and then date
        :return: returns a list of the groupings
        """
        groups = []
        d = self.data['aggregate']
        if 'area' in d.keys():
            groups.append(self.area)
        if 'window' in d.keys():
            groups.append("date")
        return groups

    def get_functions(self):
        """
        Creates a dictionary containing all of the functions (such as min, max, etc.) that the yaml files indicates
        :return: returns the dictionary
        """
        d = {}
        for key in self.data['aggregate'].keys():
            if key in self.categories:
                d[key] = self.data['aggregate'][key]
        return d

    def file_picker(self):
        """
        This method accesses each field from the yaml file, and searches for them in the dictionaries set in the __init__
        When one of the fields matches up with one of the files, that file's path is added to a list
        It also creates a set of all needed columns, and sets it equal to self.needed_columns
        :return: returns a list of which files are needed to produce the yaml file's desired output
        """
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
        """
        This method just returns the set of columns needed, which is created in file_picker
        :return: the list of required columns
        """
        return self.needed_columns

    def area_checker(self, d):
        """
        This method validates the area entry in the yaml file. The valid options are stored in the list 'accepted'
        :param d: takes the dictionary created by read_config
        :return: Boolean (true = data is valid). Returns True if the area if valid
        """
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
                return False

    def get_area(self):
        """
        This method just returns the area indicated by the yaml file
        :return: the area indicated by the yaml file
        """
        return self.area

    def county_test_checker(self, d):
        """
        Since the testing dataset does not contain data past the state level, our program can't allow a yaml file to
        indicate 'county' as the area while also requesting data from
        :param d: takes the dictionary created by read_config
        :return: Boolean (true = data is valid). Returns True if there is no confict between files needed and area
        """
        if d['aggregate']['area'] == 'county' and 'data/tests-by-state.csv' in self.file_picker():
            print('Testing data is not available at the county level.')
            return False
        return True

    def window_checker(self, d):
        """
        This method checks to see if a date window is indicated by the yaml file, and if so, validates that information
        First, it checks to make sure both start_date and end_date are present
        Second, it calls date_checker to make sure both dates are valid dates
        Third, it calls timing_checker to make sure start_date is not after end_date
        If all of these prove true, the method returns True
        :param d: takes the dictionary created by read_config
        :return: Boolean (true = data is valid). Returns as true if there is no data window indicated, or if window is valid
        """
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

    def date_checker(self, date_data):
        """
        This method validates both start_date and end_date
        First it pulls the dates out of the dictionary
        Then it re-formats them into m/d/y format
        Then tries to convert into datetime, and if it works, the method returns True
        If not, it raises an error
        :param date_data: Takes in a section of the yaml file dictionary from window_checker
        :return: Boolean (true = data is valid). Returns True if both dates are valid
        """
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
        """
        This method validates that start_date comes before or is equal to end_date
        :param date_data: Takes in a section of the yaml file dictionary from window_checker
        :return: Boolean (true = data is valid). Returns True if start_date is before or equal to end_date
        """
        return date_data['start_date'] <= date_data['end_date']

    def window_creator(self, d):
        """
        This method creates the date window object
        :param d: takes the dictionary created by read_config
        :return: Returns the window stored as a tuple
        """
        data = d['aggregate']
        if 'window' not in data:
            window = None
        else:
            window = (data['window']['start_date'], data['window']['end_date'])
        return window

    def window_getter(self):
        """
        This method just calls window_creator to retrieve the window tuple
        :return: Returns the window
        """
        return self.window_creator(self.data)

    def output_checker(self, d):
        """
        This method validates the output method
        :param d: takes the dictionary created by read_config
        :return: Boolean (true = data is valid)
        """
        if d['output_method'] == 'terminal' or '.csv' in d['output_method'] or '.txt' in d['output_method']:
            return True
        else:
            print("Invalid output method")
            return False

    def get_output(self):
        """
        This method just retrieves the method of output indicated by the yaml file
        :return: returns the output method
        """
        return self.data['output_method']

    def get_compare(self):
        """
        If the yaml file indicates a comparison, this method returns the locations that are being compared
        :return: Returns a list of the locations being compared. if there is not comparison, it returns None
        """
        if 'compare' in self.data['aggregate'].keys():
            return self.data['aggregate']['compare']
        else:
            None

    def yaml_handler(self):
        """
        Calls read_config to create a dictionary, then calls all of the checkers to validate the data
        Raises an error if any checker fails
        :return: returns the dictionary after its been validated. In the __init__, this is stored as self.data
        """
        filepath = 'config_files/' + self.file_name
        data = self.read_config(filepath)
        if \
                (
                        self.window_checker(data) and
                        self.field_checker(data) and
                        self.output_checker(data) and
                        self.area_checker(data) and
                        self.county_test_checker(data)
                ):
            return data
        else:
            raise RuntimeError('Error reading config file')

