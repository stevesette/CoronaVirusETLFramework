import warnings

import findspark
import pyspark as ps
from pyspark.sql import SQLContext


class SparkEngine:
    def __init__(self):
        findspark.init()
        try:
            # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
            self.sc = ps.SparkContext('local[4]')
            self.sqlContext = SQLContext(self.sc)
            print("Just created a SparkContext")
        except ValueError:
            warnings.warn("SparkContext already exists in this scope")
        self.data = {}

    def read_csv(self, filepath, df_name):
        self.data[df_name] = self.sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(filepath)
        return

    def join_data(self, name1, name2, new_name, list_of_col_pairs):
        data1 = self.data[name1]
        data2 = self.data[name2]
        self.data[new_name] = data1.join(data2, list_of_col_pairs)
        return

    def group_by(self, df_name, field, new_name):
        self.data[new_name] = self.data[df_name].groupBy(field).collect()
        return

    def aggregate(self, df_name, agg_type, col_name):
        d = {'max', 'min', 'avg', 'sum',}

    def limit(self, df_name, top_n):
        return self.data[df_name]