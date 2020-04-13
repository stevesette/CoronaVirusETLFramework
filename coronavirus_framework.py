from engines.spark_engine import SparkEngine
from engines.reading_engine import ReadingEngine
from engines.output_engine import OutputEngine
import sys


def main():
    yaml_input = sys.argv[1]
    re = ReadingEngine(yaml_input)
    se = SparkEngine()
    se.read(re.filenames())  # reads in csvs and joins
    se.group_by_area(level=re.level())
    se.calculate_columns(re.filenames())
    se.handle_window(re.window())
    se.handle_fields(group_by_fields=re.group_by(), functions=re.functions())
    oe = OutputEngine(re.output(), se.compute_output())
    oe.output()
