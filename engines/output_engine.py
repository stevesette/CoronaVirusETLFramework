from pyspark.sql.functions import to_date
from pyspark.sql import functions as F
import os


class OutputEngine:
    def __init__(self, df, output_type):
        self.df = df
        self.output_type = output_type

    def output(self):
        if self.output_type == 'terminal':
            self.output_df()
        elif '.csv' in self.output_type:
            self.output_csv()
        elif '.txt' in self.output_type:
            self.output_txt()
        else:
            raise RuntimeError("invalid output option")

    def output_df(self):
        print(self.df.show()) # add false to print all

    def output_csv(self):
        self.df = self.df.withColumn("Date", F.to_date(F.col("date")))
        if not os.path.exists('./output'):
            os.mkdir('./output')
        self.df.toPandas().to_csv('output/'+self.output_type, index=False)

    def output_txt(self):
        self.df = self.df.withColumn("Date", F.to_date(F.col("date")))
        if not os.path.exists('./output'):
            os.mkdir('./output')
        self.df.toPandas().to_csv('output/' + self.output_type, index=False, sep=';')