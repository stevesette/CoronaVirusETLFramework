from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


class SparkEngine:
    def __init__(
        self,
        filenames=None,
        area=None,
        total_list_of_fields=None,
        date_window=None,
        group_by=None,
        functions=None,
        compare=None,
    ):
        """
        An API for running a set of functions on datasets in Spark. Is meant to be run in conjunction with ReadingEngine
        to get configuration inputs from a properly formatted yaml configuration file and is meant to be run with
        OutputEngine for displaying or storing the data. However, if you so choose, you can instantiate this object
        without parameters and use as a customizable API.
        :param filenames: List[String] - names of files we need to build data frame
        :param area: String - Location group by criteria, one of county, state, or region
        :param total_list_of_fields: List[String] - List of fields that we need to pull together
        :param date_window: Tuple(String, String) - Start and End date string in MM-DD-YYYY format
        :param group_by: List[String] - List of fields we are grouping by, can contain 0 or 1 area and date
        :param functions: Dict{String:List[String]} - AggregationFunction:[fields to run aggregation on]
        :param compare: List[String] - List of areas to filter by {i.e. area=region -> compare=['Northeast', 'Midwest']
        """
        self.spark = (
            SparkSession.builder.appName("example-spark")
            .config("spark.sql.crossJoin.enabled", "true")
            .getOrCreate()
        )
        self.sc = SparkContext.getOrCreate()
        self.sqlContext = SQLContext(self.sc)
        self.filenames = filenames
        self.area = area
        self.tlof = total_list_of_fields
        self.date_window = date_window
        self.group_by = group_by
        self.functions = functions
        self.compare = compare
        self.df = self.read_csvs(filenames)

    def read_csvs(self, filenames=None):
        """
        Creates a dataframe from the csvs specified in filepaths. If multiple filepaths, joins on date and state.
        :param filepaths: List[String] of filenames that contain the necessary data for the dataframe, defaults to self.filenames if None
        :return: SparkDataFrame of the csv data joined together (if necessary)
        """
        if filenames is None:
            filenames = self.filenames

        if filenames is None:
            return

        def read_one_csv(fpath_, sql_context):
            return sql_context.read.csv(fpath_, header=True, inferSchema=True)

        rtn = None
        for filepath in filenames:
            if rtn == None:
                rtn = read_one_csv(filepath, self.sqlContext)
            else:
                temp = read_one_csv(filepath, self.sqlContext)
                rtn = rtn.alias("rtn").join(temp, on=["date", "state"], how="inner")
        return rtn

    def calculate_columns(self, tlof=None, window=None, df=None):
        """
        Checks the return fields for which ones need to be calculated by spark functions. I.e. new_cases is not a part
        of the dataset so we need to calculate that by comparing the aggregate number of cases with the previous
        number of cases in that area. When parameters are None, uses internal saved parameters from the class.
        :param tlof: reflects tlof in constructor above, defaults to self.tlot if None
        :param window: Window that can be partitioned by, defaults to partition by self.area and order by 'date' when None
        :param df: reflects df created in constructor above, defaults to self.df if None
        :return: SparkDataFrame with any fields that need to be calculated
        """
        if tlof is None:
            tlof = self.tlof
        if window is None:
            window = Window.partitionBy(self.area).orderBy("date")
        if df is None:
            df = self.df

        def new_cases(df_):
            df_ = df_.withColumn("previous_cases", F.lag(df_.cases).over(window))
            df_ = df_.withColumn("new_cases", df_.cases - df_.previous_cases)
            return df_

        def new_cases_delta(df_):
            if "new_cases" not in df_.columns:
                df_ = new_cases(df_)
            df_ = df_.withColumn(
                "previous_new_cases", F.lag(df_.new_cases).over(window)
            )
            df_ = df_.withColumn(
                "new_cases_delta", df_.new_cases - df_.previous_new_cases
            )
            return df_

        def new_deaths(df_):
            df_ = df_.withColumn("previous_deaths", F.lag(df_.deaths).over(window))
            df_ = df_.withColumn("new_deaths", df_.deaths - df_.previous_deaths)
            return df_

        def new_deaths_delta(df_):
            if "new_deaths" not in df_.columns:
                df_ = new_deaths(df_)
            df_ = df_.withColumn(
                "previous_new_deaths", F.lag(df_.new_deaths).over(window)
            )
            df_ = df_.withColumn(
                "new_deaths_delta", df_.new_deaths - df_.previous_new_deaths
            )
            return df_

        def mortality_rate(df_):
            df_ = df_.withColumn("mortality_rate", df_.deaths / df_.cases)
            return df_

        def mortality_rate_delta(df_):
            if "mortality_rate" not in df_.columns:
                df_ = mortality_rate(df_)
            df_ = df_.withColumn(
                "previous_mortality_rate", F.lag(df_.mortality_rate).over(window)
            )
            df_ = df_.withColumn(
                "mortality_rate_delta", df_.mortality_rate - df_.previous_mortality_rate
            )
            return df_

        def test_positive_rate(df_):
            df_ = df_.withColumn("test_positive_rate", df_.positive / df_.total_results)
            return df_

        def test_negative_rate(df_):
            df_ = df_.withColumn("test_negative_rate", df_.negative / df_.total_results)
            return df_

        needs_to_be_calculated = {
            "new_cases": new_cases,
            "new_cases_delta": new_cases_delta,
            "new_deaths": new_deaths,
            "new_deaths_delta": new_deaths_delta,
            "mortality_rate": mortality_rate,
            "mortality_rate_delta": mortality_rate_delta,
            "test_positive_rate": test_positive_rate,
            "test_negative_rate": test_negative_rate,
        }
        for field in needs_to_be_calculated:
            if field in tlof:
                df = needs_to_be_calculated[field](df)
        return df

    def handle_area(self, group_by=None, df=None, area=None):
        """
        Corrects aggregate datasets for the area at which the query is being run. Since the dataset stores aggregate
        data by county for us-counties.csv dataset or by state for test-by-state.csv dataset, trying to run any queries
        one a more aggregated level will cause the data to break. In order to avoid this, this function sums the data
        together by the group_by level.
        It also deletes columns that could confuse the ouput, i.e. if running on a region then there should not be any
        state, county, or fips information as it could mislead the user into thinking that the data is specifically for
        one county or one state or one fips.
        :param group_by: Reflects group_by in constructor, defaults to self.group_by if None
        :param df: Reflects df created in constructor, defaults to self.group_by if None
        :param area: Reflects area in constructor, defaults to self.area if None
        :return: adjusted df with area rollups and confusing columns removed
        """
        if group_by is None:
            group_by = self.group_by
        if df is None:
            df = self.df
        if area is None:
            area = self.area

        def rollup(df_, group_by_):
            default_cols = [
                "positive",
                "negative",
                "total_results",
                "pending",
                "total_tests",
                "cases",
                "deaths",
            ]
            agg_q = {}
            for col in default_cols:
                if col in self.df.columns:
                    agg_q[col] = "sum"
            df_ = df_.groupby(group_by_).agg(agg_q).orderBy(group_by_)
            for col in agg_q.keys():
                df_ = df_.withColumnRenamed(f"sum({col})", col)
            return df_

        if area == "region":
            df = self.make_region_column(df=df)
            df = df.drop("state")
            df = df.drop("county")
            df = df.drop("fips")
            df = rollup(df, group_by)
        elif area == "state":
            df = df.drop("county")
            df = df.drop("fips")
            df = rollup(df, group_by)
        elif area == "county":
            pass
        else:
            raise RuntimeError("Invalid area parameter")
        return df

    def make_region_column(self, df=None):
        """
        Adds a column called region to a dataframe.
        :param df: Reflects df created in constructor above, defaults to self.df if None
        :return: dataframe with region column attached
        """
        if df is None:
            df = self.df

        def get_region(state):
            lookup = {
                "Washington, Oregon, California, Alaska, Hawaii": "West",
                "Idaho, Montana, Nevada, Utah, Wyoming, Colorado": "Rocky Mountains",
                "Arizona, New Mexico, Texas, Oklahoma": "Southwest",
                "North Dakota, South Dakota, Nebraska, Kansas, Missouri, Iowa, Minnesota, Wisconsin, Illinois, "
                "Indiana, Michigan, Ohio": "Midwest",
                "Arkansas, Louisiana, Mississippi, Alabama, Georgia, Florida, Tennessee, North Carolina, "
                "South Carolina, Kentucky, West Virginia, Virginia, Delaware, Maryland": "Southeast",
                "Pennsylvania, New Jersey, Connecticut, Rhode Island, Massachusetts, New York, Vermont, "
                "New Hampshire, Maine": "Northeast",
            }
            for key in lookup.keys():
                if state in key:
                    return lookup[key]
            return "Other"

        region_udf = F.udf(get_region, StringType())
        df = df.withColumn("region", region_udf(df.state))
        return df

    def handle_window(self, date_window=None, df=None):
        """
        Filters rows based on a date window. Dates are a tuple of start then end date and are in the format MM-DD-YYYY.
        :param date_window: reflects date_window in constructor, defaults to self.date_window if None
        :param df: reflects df created in constructor, defaults to self.df if None
        :return: filtered dataframe
        """
        if date_window is None:
            date_window = self.date_window
        if df is None:
            df = self.df
        if date_window:
            start_list = date_window[0].split("/")
            start_string = f"{start_list[2]}-{start_list[0]}-{start_list[1]} 00:00:00"
            end_list = date_window[1].split("/")
            end_string = f"{end_list[2]}-{end_list[0]}-{end_list[1]} 00:00:00"
            df = df.where(f"date > '{start_string}'")
            df = df.where(f"date < '{end_string}'")
        return df

    def handle_compare(self, compare=None, df=None, area=None):
        """
        Filters rows based on areas that we want to include. If region then valid input is any of the 6 regions defined
        in self.make_region_column. If state then valid input is any of the 50 states or territories. If county then
        valid input is any of the counties that exist in the US. All compare fields must be in proper casing with spaces
        separating any that have them (i.e. "New York" or "South Dakota")
        :param compare: reflects compare in the constructor, defaults to self.compare if None
        :param df: reflects df in the constructor, defaults to self.df if None
        :param area: reflects area in the constructor, defaults to self.area if None
        :return: filtered dataframe
        """
        if compare is None:
            compare = self.compare
        if df is None:
            df = self.df
        if area is None:
            area = self.area
        if compare:
            df = df.where(f"{area} in {str(tuple(compare))}")
        return df

    def handle_fields(self, group_by=None, df=None, functions=None):
        """
        Handles aggregation of all fields in the functions parameter on the dataframe parameter by grouping by the
        group_by parameter. When any of these fields are left as none they default to what the spark engine was
        initialized with. Should be run after fields are calculated and area is rolled up.
        :param group_by: reflects group_by in constructor, defaults to self.group_by if None
        :param df: reflects df in constructor, defaults to self.df if None
        :param functions: reflects functions in constructor, defaults to self.functions if None
        :return: dataframe with all aggregation functions run and columns for the results added
        """
        if group_by is None:
            group_by = self.group_by
        if df is None:
            df = self.df
        if functions is None:
            functions = self.functions

        grouped_data = df.groupby(group_by)

        def agg(function_name, column_names):
            agg_query = {}
            for col_name in column_names:
                agg_query[col_name] = function_name
            return grouped_data.agg(agg_query)

        rtn = df.selectExpr(group_by).dropDuplicates()
        for func_field in functions:
            temp = agg(func_field, functions[func_field])
            rtn = rtn.alias("rtn").join(
                temp.alias(func_field), on=group_by, how="inner"
            )
        return rtn.orderBy(group_by)

    def compute_output(self):
        """
        Runs handle_area, calculate_columns, handle_window, handle_compare, and handle_fields all with default values.
        This is the method which we recommend running the spark engine as everything is properly ordered here and
        guarantees no errors provided the input to create the spark engine has been done properly.
        :return: dataframe with all calculations and data manipulations handled
        """
        self.df = self.handle_area()
        self.df = self.calculate_columns()
        self.df = self.handle_window()
        self.df = self.handle_compare()
        self.df = self.handle_fields()
        return self.df
