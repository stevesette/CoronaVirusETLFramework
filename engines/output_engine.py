from pyspark.sql.functions import to_date
from pyspark.sql import functions as F
import os


class OutputEngine:
    def __init__(self, df, output_type):
        """
        :param df: Gives in dataframe from SparkEngine
        :param output_type: Takes output type from input YAML file, passes to functions below in order to
        output to desired location
        """
        self.df = df
        self.output_type = output_type

    def output(self):
        """
        Takes in the output type specified in YAML, and throws to function below to produce correct output
        :return: Throws error if the output type specified in input YAML is not one of: terminal, .csv, .txt
        """
        if self.output_type == 'terminal':
            self.output_df()
        elif '.csv' in self.output_type:
            self.output_csv()
        elif '.txt' in self.output_type:
            self.output_txt()
        else:
            raise RuntimeError("invalid output option")

    def output_df(self):
        """
        :return: Prints query results in dataframe to your terminal
        """
        print(self.df.show()) # add false to print all

    def output_csv(self):
        """
        If specified output method is .csv, calls this function which first checks if output folder exists.
         If not, creates folder called output in your directory and writes results to file within.
        :return: Writes results of query to a .csv file, and saves it into a  folder called Output in your
        directory.
        """
        self.df = self.df.withColumn("Date", F.to_date(F.col("date")))
        if not os.path.exists('./output'):
            os.mkdir('./output')
        self.df.toPandas().to_csv('output/'+self.output_type, index=False)

    def output_txt(self):
        """
        If specified output method is .txt, calls this function which first checks if output folder exists.
         If not, creates folder called output in your directory and writes results to file within.
        :return: Writes results of query to a .txt file, and saves it into a  folder called Output in your
        directory.
        """
        self.df = self.df.withColumn("Date", F.to_date(F.col("date")))
        if not os.path.exists('./output'):
            os.mkdir('./output')
        self.df.toPandas().to_csv('output/' + self.output_type, index=False, sep=';')