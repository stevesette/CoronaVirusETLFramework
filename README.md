# CoronaVirusETLFramework
Final Project for DS_4300 by Tim Lexa, Caelan Schneider, Steve Setteducati Jr., and Jack Tonina <br />
Demo Video: https://www.youtube.com/watch?v=1SfJoIcZ0M0 <br />
Presentation: https://docs.google.com/presentation/d/17F11slKvWAxjm3LhkSH-uFt0maV1THv2Hw823_HHr3g/edit?usp=sharing

## Introduction
COVID-19, commonly referred to as Coronavirus, has caused an unprecedented disruption in daily life across the world. Roughly two-million people have been infected with the deadly virus, and over a quarter of all confirmed cases are in the United States. Maintaining consistent, accurate, and thorough data has been, and will remain, vital to governments and individuals as they act to mitigate the spread of the disease. Several sources of data about COVID-19 are freely available online. In this project we utilize public data sets from The New York Times and The COVID Tracking Project, and have built a tool using Spark and Python that enables the end user to conduct basic analysis on this data through the input of YAML configuration files.

## Supported Datasets
The New York Times dataset breaks down the cases and deaths of counties by day. It includes the following fields: date, state, county, fips, cases, and deaths. The “fips” field is a unique integer identifier on county. Cases and deaths are cumulative statistics so in order to determine how many cases or deaths were added per day we calculate the change over day of the fields using spark’s lag functionality, creating the “new_cases” and “new_deaths” fields. In addition we calculated the change of “new_cases” and “new_deaths” as a lag over these fields which are referred to as “new_cases_delta” and “new_deaths_delta”. Finally, we added a “mortality_rate” field as the number of cumulative deaths divided by the number of cumulative cases and added a “mortality_rate_delta” field as the lagged difference of mortality rate per day. 

The COVID Tracking Project breaks down testing data by state. In this dataset, we do not have access to data at a county level so any queries requesting a “county” area which include any fields in this dataset will result in a thrown error to the user. The available fields here are “state”, “date”, “positive”, “negative”, “total_results”, “pending”, “total_tests”, “hospitalizedCurrently”, and “hospitalizedCumulative”. The “pending”, “total_tests”, “hospitalizedCurrently”, and “hospitalizedCumulative” fields are largely missing data and seem unreliable so we decided to remove them from our functionality. The fields we offer to calculate here are “test_positive_rate” and “test_negative_rate” which are calculated by dividing “positive” by “total_results” and “negative” by “total_results” respectively.

Neither of these datasets comes default with a “region”. This is a functionality that we built into our system to aggregate states to the general geographic region to which they belong such as “Northeast” or “Rocky Mountains”. When both datasets are being used we join the data together on the “date” and “state” fields. If the user would like to aggregate by region, that calculation comes afterwards.

## Configuration Files
We used YAML files to feed input into our program which allows for this tool to be used by a wider audience without programming knowledge. COVID-19 affects the entire world, and so it was important to us for our tool to have this functionality, since everyone should have the power to stay informed. We structured our various YAML files based on function rather than a particular dataset, which allows you to then pull in fields from different data sets into one output. We wrote four types of YAML files to handle unique types of queries: <br/>
* **compare.yaml**: allows you to input multiple areas (counties, states, regions - must all be same area type) in order to compare desired statistics across geographies<br/>
* **mostNew.yaml**: takes in area type, start and end dates (to create a date range) as well as multiple statistics in order to compare statistics in different geographies over a specified time frame <br/>
* **tests.yaml**: specifically for COVID-19 testing data, this input file takes in an area type, date range (start and end dates) as well as multiple statistics and compares test results across geographies over the specified time range <br/>
* **error.yaml**: this YAML file exists to expose any major discrepancies in the two source data sets. It takes in an area size and returns statistics for total positive tests and total cases, which should be the same. 

Originally we hoped to be able to find more datasets that would be complete and compatible enough in order to also work into this framework, but we realized that completeness of data was more important than additional sources. Many of the other datasets we came across were either missing too much data to be useful or organized in a manner that would be too complex to write into a flexible program. However, looking back on our original goals of the project we were able to successfully check a majority of the boxes. We created a program that (1) consumes, manipulates and joins multiple large data sets , (2) calculates and aggregates various metrics from across both data sets and (3) transforms and outputs aggregated data to multiple output output types (terminal, .csv file, .txt file).

## YAML Input Fields and Specifications:
| Filter Fields | Inputs / Requirements |
| ------------- | --------------------- |
| output_method | terminal, filename.csv, filename.txt |
| area | county, state, region |
| window (start date & end date) | mm/dd/yyyy formatted dates |
| compare (for compare.yaml only) | Takes in unlimited values all belonging to one area type (i.e. unlimited counties, states, regions) |
| Aggregation Methods: | Min, max, mean, sum |
| Available Fields: | cases, deaths, new_cases, new_deaths, mortality_rate, mortality_rate_delta, new_cases_delta, new_deaths_delta, positive*, negative*, total_results, total_tests, test_positive_rate, test_negative_rate |

*positive and negative in ‘Available Fields’ refers to positive and negative test results 


The rules that each YAML file should adhere to are as follows:
1. Each YAML file must have a specified output method.
1. Each YAML file must have a specified area.
1. Each YAML file must have a specified window field.
1. Each YAML file that references testing data must have an area of state or region. County-level testing data is not available.


## Program Architecture
Any analysis queries are started by the coronavirus_framework python script. This script takes in a configuration file name as a system argument. The process can be run by running “python coronavirus_framework.py configuration_file.yaml” where the configuration_file refers to the name of the configuration file. The configuration filename is then passed to the ReadingEngine. 

The ReadingEngine relies on the fact that the passed in YAML filename exists within the “config_files” directory and reads the file. This engine interprets the fields provided in the YAML files and checks them for all of the above guidelines to ensure that the configuration file was properly constructed. It also provides API access to the data so that analysis queries can be run from our SparkEngine and outputted by our OutputEngine. 

The SparkEngine is an API structured to operate in two ways. Firstly, as an extension of the ReadingEngine which performs the tasks described in a configuration file. Its “compute_output” function runs any area aggregations, computes any calculated fields,  filters on date range and on area (if applicable), and performs any aggregations on the resulting dataset. Alternatively, the SparkEngine could be used as a standalone API for spark leveraging any of the functionality of the aforementioned functions in a customizable way. In the larger project scope, the SparkEngine prepares the data for the OutputEngine.
  
The OutputEngine is a visualization API which takes in any Spark Dataframe and outputs it in one of three ways: to the terminal, to a text file, or to a csv file.

