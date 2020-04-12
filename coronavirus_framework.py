from engines import output_engine, reading_engine, spark_engine
import sys


def main(yaml_input):
    yaml_input = sys.argv[1]
    re = reading_engine(yaml_input)
    se = spark_engine()
    se.read(re.filenames())  # reads in csvs and joins
    se.group_by_area(level=re.level())
    se.calculate_columns(re.filenames())
    se.handle_window(re.window())
    se.handle_fields(group_by_fields=re.group_by(), functions=re.functions())
    oe = output_engine(re.output(), se.compute_output())
    oe.output()
