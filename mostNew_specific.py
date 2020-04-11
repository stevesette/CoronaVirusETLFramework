import pyspark
import yaml
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

spark = SparkSession\
    .builder\
    .appName("example-spark")\
    .config("spark.sql.crossJoin.enabled", "true")\
    .getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

with open("config_files/mostNew.yaml") as f:
    yaml_data = yaml.load(f)
print(yaml_data.keys())
# dataframe = sqlContext.read.csv(yaml_data['input_file'])
dataframe = sqlContext.read.csv("data/us-counties.csv", header=True, inferSchema=True) #.selectExpr("_c0 as date", "_c1 as county", "_c2 as Month", "_c3 as Day", "_c4 as TempF")

query = yaml_data['aggregate']
query['area']


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


def handle_aggregations(query, df):
    function_key_value = {'area': group_by_area, 'window': handle_window, "min": agg,
                          "max": agg, "sum": agg, "mean": agg}
    # for func in query:
    #     handle_func()
    pass


def mult_aggs(query, df):
    rtn = None
    for agg_q in query:
        rtn = agg(agg_q, query[agg_q], )


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

# funcs = query.copy()
# funcs.pop('window')
# funcs.pop('area')
funcs = {}
fs = ['deaths', 'cases']
funcs['min'] = fs
funcs['max'] = fs
funcs['avg'] = fs
funcs['sum'] = fs
print(handle_fields(dataframe, ["date", 'state'], funcs).show())
# rtn = agg("max", ['deaths', 'cases'], grouped)
# print(rtn.show())
# rtn2 = agg('min', ['deaths', 'cases'], grouped)
# # rtn = rtn.join(agg('min', ['deaths', 'cases'], grouped), )
# print(rtn2.show())
# cond = [rtn.date == rtn2.date]
# rtn = rtn.join(rtn2, cond, "inner")
# print(rtn.show())
# print(agg("","",dataframe).show())


def output(output_method):
    if output_method == 'console':
        print()


