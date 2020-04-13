import os

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, TimestampType, DateType


class SparkEngine:
    def __init__(self, filenames, area, total_list_of_fields, date_window, group_by, functions, compare):
        self.spark = SparkSession \
            .builder \
            .appName("example-spark") \
            .config("spark.sql.crossJoin.enabled", "true") \
            .getOrCreate()
        self.sc = SparkContext.getOrCreate()
        self.area = area
        self.tlof = total_list_of_fields
        self.date_window = date_window
        self.group_by = group_by
        self.functions = functions
        self.compare = compare
        self.sqlContext = SQLContext(self.sc)
        self.df = self.read_csvs(filenames)

    def read_csvs(self, filepaths):
        def read_one_csv(fpath_, sql_context):
            return sql_context.read.csv(fpath_, header=True, inferSchema=True)
        rtn = None
        for filepath in filepaths:
            if rtn == None:
                rtn = read_one_csv(filepath, self.sqlContext)
            else:
                temp = read_one_csv(filepath, self.sqlContext)
                rtn = rtn.alias('rtn').join(temp, on=['date', 'state'], how="inner")
        return rtn

    def calculate_columns(self):
        w = Window.partitionBy(self.area).orderBy('date')

        def new_cases():
            self.df = self.df.withColumn('previous_cases', F.lag(self.df.cases).over(w))
            self.df = self.df.withColumn("new_cases", self.df.cases - self.df.previous_cases)

        def new_cases_delta():
            if 'new_cases' not in self.df.columns:
                new_cases()
            self.df = self.df.withColumn('previous_new_cases', F.lag(self.df.new_cases).over(w))
            self.df = self.df.withColumn('new_cases_delta', self.df.new_cases - self.df.previous_new_cases)

        def new_deaths():
            self.df = self.df.withColumn('previous_deaths', F.lag(self.df.deaths).over(w))
            self.df = self.df.withColumn("new_deaths", self.df.deaths - self.df.previous_deaths)

        def new_deaths_delta():
            if 'new_deaths' not in self.df.columns:
                new_deaths()
            self.df = self.df.withColumn('previous_new_deaths', F.lag(self.df.new_deaths).over(w))
            self.df = self.df.withColumn('new_deaths_delta', self.df.new_deaths - self.df.previous_new_deaths)

        def mortality_rate():
            self.df = self.df.withColumn("mortality_rate", self.df.deaths / self.df.cases)

        def mortality_rate_delta():
            if 'mortality_rate' not in self.df.columns:
                mortality_rate()
            self.df = self.df.withColumn('previous_mortality_rate', F.lag(self.df.mortality_rate).over(w))
            self.df = self.df.withColumn('mortality_rate_delta', self.df.mortality_rate - self.df.previous_mortality_rate)

        def test_positive_rate():
            self.df = self.df.withColumn('test_positive_rate', self.df.positive / self.df.total_results)

        def test_negative_rate():
            self.df = self.df.withColumn('test_negative_rate', self.df.negative / self.df.total_results)
        # needs_to_be_calculated = {"new_cases", "new_cases_delta", "new_deaths", "new_deaths_delta", "mortality_rate",
        #                           "mortality_rate_delta"}
        needs_to_be_calculated = ["new_cases", "new_cases_delta", "new_deaths", "new_deaths_delta", "mortality_rate",
                                  "mortality_rate_delta", "test_positive_rate", "test_negative_rate"]
        for field in needs_to_be_calculated:
            if field in self.tlof:
                if field == 'new_cases':
                    new_cases()
                elif field == 'new_cases_delta':
                    new_cases_delta()
                elif field == 'new_deaths':
                    new_deaths()
                elif field == 'new_deaths_delta':
                    new_deaths_delta()
                elif field == 'mortality_rate':
                    mortality_rate()
                elif field == "mortality_rate_delta":
                    mortality_rate_delta()
                elif field == "test_positive_rate":
                    test_positive_rate()
                elif field == "test_negative_rate":
                    test_negative_rate()
        return self.df

    def handle_area(self):
        def rollup():
            default_cols = ['positive', 'negative', 'total_results', 'pending', 'total_tests', 'cases', 'deaths']
            agg_q = {}
            for col in default_cols:
                if col in self.df.columns:
                    agg_q[col] = 'sum'
            self.df = self.df.groupby(self.group_by).agg(agg_q).orderBy(self.group_by)
            for col in agg_q.keys():
                self.df = self.df.withColumnRenamed(f"sum({col})", col)
        if self.area == 'region':
            self.make_region_column()
            self.df = self.df.drop("state")
            self.df = self.df.drop("county")
            self.df = self.df.drop("fips")
            rollup()
        if self.area == 'state':
            self.df = self.df.drop("county")
            self.df = self.df.drop("fips")
            rollup()
        if self.area == 'county':
            pass

    def make_region_column(self):
        def get_region(state):
            lookup = {"Washington, Oregon, California, Alaska, Hawaii": "West",
                      "Idaho, Montana, Nevada, Utah, Wyoming, Colorado": "Rocky Mountains",
                      "Arizona, New Mexico, Texas, Oklahoma": "Southwest",
                      "North Dakota, South Dakota, Nebraska, Kansas, Missouri, Iowa, Minnesota, Wisconsin, Illinois, "
                      "Indiana, Michigan, Ohio": "Midwest",
                      "Arkansas, Louisiana, Mississippi, Alabama, Georgia, Florida, Tennessee, North Carolina, "
                      "South Carolina, Kentucky, West Virginia, Virginia, Delaware, Maryland": "Southeast",
                      "Pennsylvania, New Jersey, Connecticut, Rhode Island, Massachusetts, New York, Vermont, "
                      "New Hampshire, Maine": "Northeast"}
            for key in lookup.keys():
                if state in key:
                    return lookup[key]
            return "Other"
        region_udf = F.udf(get_region, StringType())
        self.df = self.df.withColumn('region', region_udf(self.df.state))

    def handle_window(self):
        if self.date_window:
            start_list = self.date_window[0].split("/")
            start_string = f"{start_list[2]}-{start_list[0]}-{start_list[1]} 00:00:00"
            end_list = self.date_window[1].split("/")
            end_string = f"{end_list[2]}-{end_list[0]}-{end_list[1]} 00:00:00"
            self.df = self.df.where(f"date > '{start_string}'")
            self.df = self.df.where(f"date < '{end_string}'")

    def handle_compare(self):
        if self.compare:
            self.df = self.df.where(f"{self.area} in {str(tuple(self.compare))}")

    def agg(self, function_name, column_names, grouped_data):
        agg_query = {}
        for col_name in column_names:
            agg_query[col_name] = function_name
        return grouped_data.agg(agg_query)

    def handle_fields(self):
        grouped = self.df.groupby(self.group_by)
        rtn = self.df.selectExpr(self.group_by).dropDuplicates()
        for func_field in self.functions:
            temp = self.agg(func_field, self.functions[func_field], grouped)
            rtn = rtn.alias('rtn').join(temp.alias(func_field), on=self.group_by, how="inner")
        return rtn.orderBy(self.group_by)

    def compute_output(self):
        self.handle_area()
        self.calculate_columns()
        self.handle_window()
        self.handle_compare()
        self.handle_fields()
        return self.df
