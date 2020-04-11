import yaml
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from engines import output_engine, reading_engine, spark_engine

spark = SparkSession\
    .builder\
    .appName("example-spark")\
    .config("spark.sql.crossJoin.enabled", "true")\
    .getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

with open("config_files/mostNew.yaml") as f:
    yaml_data = yaml.load(f)

def read():
    return sqlContext.read.csv("data/us-counties.csv", header=True, inferSchema=True)

def main(yaml_input):
    re = reading_engine(yaml_input)
    se = spark_engine()
    dataframe = read()
    grouped_date = group_by_area(level=None, dataframe=dataframe)
    with_calculated_columns = calculated_columns(grouped_date, total_lof=None)
    limited_timeframe = handle_window(start=None, end=None, df=with_calculated_columns)
    aggregated_fields = handle_fields(df=limited_timeframe, group_by_fields=None, functions=None)
    oe = output_engine(se)

def main(yaml_input):
    re = reading_engine(yaml_input)
    se = spark_engine()
    se.read(re.filenames())  # reads in csvs and joins
    se.group_by_area(level=re.level())
    se.calculate_columns(re.filenames())
    se.handle_window(re.window())
    se.handle_fields(group_by_fields=re.group_by(), functions=re.functions())
    oe = output_engine(re.output(), se.compute_output())
    oe.output()

def calculated_columns(df, total_lof, window):
    needs_to_be_calculated = ["new_cases", "new_cases_delta", "new_deaths", "new_deaths_delta", "mortality_rate",
                              "mortality_rate_delta"]
    for field in needs_to_be_calculated:
        if field in total_lof:
            if field == 'new_cases':
                df = df.withColumn('previous_cases', F.lag(df.cases).over(w))
                df = df.withColumn("new_cases", df.cases - df.previous_cases)
            elif field == 'new_cases_delta':
                df = df.withColumn('previous_new_cases', F.lag(df.new_cases).over(w))
                df = df.withColumn('new_cases_delta', df.new_cases - df.previous_new_cases)
            elif field == 'new_deaths':
                df = df.withColumn('previous_deaths', F.lag(df.deaths).over(w))
                df = df.withColumn("new_deaths", df.deaths - df.previous_deaths)
            elif field == 'new_deaths_delta':
                df = df.withColumn('previous_new_deaths', F.lag(df.new_deaths).over(w))
                df = df.withColumn('new_deaths_delta', df.new_deaths - df.previous_new_deaths)
            elif field == 'mortality_rate':
                df = df.withColumn("mortality_rate", df.deaths / df.cases)
            elif field == "mortality_rate_delta":
                df = df.withColumn('previous_mortality_rate', F.lag(df.mortality_rate).over(w))
                df = df.withColumn('mortality_rate_delta', df.mortality_rate - df.previous_mortality_rate)
    return df

def handle_area(level, df):
    if level == 'region':
        pass
    if level == 'state':
        pass
    if level == 'county':
        pass

window = query['window']
start_date = window['start_date']
end_date = window['end_date']


def handle_window(start, end, df):
    return df.filter(start < df.date < end)

def group_by_area():
    pass


def agg(function_name, column_names, df):
    agg_query = {}
    for col_name in column_names:
        agg_query[col_name] = function_name
    return df.agg(agg_query)


def handle_fields(df, group_by_fields, functions):
    grouped = df.groupby(group_by_fields)
    rtn = df.selectExpr(group_by_fields).dropDuplicates()
    for func_field in functions:
        temp = agg(func_field, functions[func_field], grouped)
        rtn = rtn.alias('rtn').join(temp.alias(func_field), on=group_by_fields, how="inner")
    return rtn.orderBy(group_by_fields)
