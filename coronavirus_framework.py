from engines.spark_engine import SparkEngine
from engines.reading_engine import ReadingEngine
from engines.output_engine import OutputEngine
import sys


def main():
    # reads system arguments so that you can run framework as "python coronavirus_framework.py config_filename.yaml"
    yaml_input = sys.argv[1]
    # Reads the yaml input into an organized format
    re = ReadingEngine(yaml_file=yaml_input)
    # Instantiates an engine to interface with spark and datasets based on the yaml config input
    se = SparkEngine(
        filenames=re.file_picker(),
        area=re.get_area(),
        total_list_of_fields=re.get_columns(),
        date_window=re.window_getter(),
        group_by=re.get_group_by(),
        functions=re.get_functions(),
        compare=re.get_compare(),
    )
    # Instantiates an engine to output the computations done by the spark engine
    oe = OutputEngine(df=se.compute_output(), output_type=re.get_output())
    # Outputs output based on format specified in config file
    oe.output()


if __name__ == "__main__":
    main()
