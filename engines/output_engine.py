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
        return self.df

    def output_csv(self):
        self.df.write.format('csv').save(self.output_type)
        # 'com.databricks.spark.csv' if no run, try instead of 'csv'

    def output_textfile(self):
        self.df.write.format('csv').options("delimiter", "|").save(self.output_type)
        