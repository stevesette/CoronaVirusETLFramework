import os

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SQLContext


class SparkEngine:
    def __init__(self, filenames, groupings):
        self.spark = SparkSession \
            .builder \
            .appName("example-spark") \
            .config("spark.sql.crossJoin.enabled", "true") \
            .getOrCreate()
        self.sc = SparkContext.getOrCreate()
        self.sqlContext = SQLContext(self.sc)
        self.df = self.read_csvs(filenames)
        self.groupings = groupings

    def read_csvs(self, filepaths):
        def read_one_csv(fpath_, sql_context):
            return sql_context.read.csv(fpath_, header=True, inferSchema=True)

        rtn = None
        for filepath in filepaths:
            if rtn == None:
                rtn = read_one_csv(filepath, self.sqlContext)
            else:
                temp = read_one_csv(filepath, self.sqlContext)
                rtn = rtn.join(temp, on=[self.groupings], how="inner")
        return

    def calculated_columns(self, total_lof, w):
        needs_to_be_calculated = ["new_cases", "new_cases_delta", "new_deaths", "new_deaths_delta", "mortality_rate",
                                  "mortality_rate_delta"]
        for field in needs_to_be_calculated:
            if field in total_lof:
                if field == 'new_cases':
                    self.df = self.df.withColumn('previous_cases', F.lag(self.df.cases).over(w))
                    self.df = self.df.withColumn("new_cases", self.df.cases - self.df.previous_cases)
                elif field == 'new_cases_delta':
                    self.df = self.df.withColumn('previous_new_cases', F.lag(self.df.new_cases).over(w))
                    self.df = self.df.withColumn('new_cases_delta', self.df.new_cases - self.df.previous_new_cases)
                elif field == 'new_deaths':
                    self.df = self.df.withColumn('previous_deaths', F.lag(self.df.deaths).over(w))
                    self.df = self.df.withColumn("new_deaths", self.df.deaths - self.df.previous_deaths)
                elif field == 'new_deaths_delta':
                    self.df = self.df.withColumn('previous_new_deaths', F.lag(self.df.new_deaths).over(w))
                    self.df = self.df.withColumn('new_deaths_delta', self.df.new_deaths - self.df.previous_new_deaths)
                elif field == 'mortality_rate':
                    self.df = self.df.withColumn("mortality_rate", self.df.deaths / self.df.cases)
                elif field == "mortality_rate_delta":
                    self.df = self.df.withColumn('previous_mortality_rate', F.lag(self.df.mortality_rate).over(w))
                    self.df = self.df.withColumn('mortality_rate_delta', self.df.mortality_rate - self.df.previous_mortality_rate)
        return self.df

    def handle_area(self, level):
        if level == 'region':
            pass
        if level == 'state':
            pass
        if level == 'county':
            pass

    def handle_window(self, start, end):
        return self.df.filter(start < self.df.date < end)

    def agg(self, function_name, column_names):
        agg_query = {}
        for col_name in column_names:
            agg_query[col_name] = function_name
        return self.df.agg(agg_query)

    def handle_fields(self, group_by_fields, functions):
        grouped = self.df.groupby(group_by_fields)
        rtn = self.df.selectExpr(group_by_fields).dropDuplicates()
        for func_field in functions:
            temp = self.agg(func_field, functions[func_field], grouped)
            rtn = rtn.alias('rtn').join(temp.alias(func_field), on=group_by_fields, how="inner")
        return rtn.orderBy(group_by_fields)
