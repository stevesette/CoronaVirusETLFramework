aclass output:
    def __init__(self, df):
        df = self.df

    def output_df(self):
        return self.df

    def output_csv(self, write_to):
        self.df.write.format('csv').save(write_to)
        # 'com.databricks.spark.csv' if no run, try instead of 'csv'

    def output_textfile(self, write_to):
        self.df.write.format('csv').options("delimiter", "|").save(write_to)
        # 'com.databricks.spark.csv' if no run, try instead of 'csv'
