from engines.spark_engine import SparkEngine
from engines.reading_engine import ReadingEngine
from engines.output_engine import OutputEngine
import sys


def main():
    yaml_input = sys.argv[1]
    re = ReadingEngine(yaml_file=yaml_input)
    se = SparkEngine(filenames=re.file_picker(), area=re.get_area(), total_list_of_fields=re.get_columns(),
                     date_window=re.window_getter(), group_by=re.get_group_by(), functions=re.get_functions(),
                     compare=re.get_compare())
    oe = OutputEngine(df=se.compute_output(), output_type=re.get_output())
    oe.output()


if __name__ == '__main__':
    main()
