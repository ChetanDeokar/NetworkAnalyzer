import datetime
from itertools import chain

from pyspark.sql import functions as F

from nw_constants.constants import CALL_DROP_FILE_PATH, AGEING_DAY_WEIGHT_MAP, CALL_DROP_DELTA_DAYS
from utility import spark_utility, services_utility


class CallDropHelper(spark_utility.SparkUtility, services_utility.ServicesUtility):
     spark_session = None
     data = None
     msisdn = None
     sr_date = datetime.datetime.now()

     def __init__(self, msisdn, filepath=CALL_DROP_FILE_PATH, sr_date=None):
         self.set_master("local")
         self.set_app("CallDropAnalyzer")
         self.set_spark_session()
         self.data = self.read_csv_as_dataframe(file_path=filepath, header='true', inferschema='true')
         self.msisdn = msisdn
         if sr_date:
             if isinstance(sr_date, (datetime.datetime, datetime.date)):
                 self.sr_date = sr_date

     def get_msisdn_base_view(self, dateformat="dd-MM-yyyy", delta_days=CALL_DROP_DELTA_DAYS):
         start_date = (self.sr_date - datetime.timedelta(days=delta_days)).strftime("%Y-%m-%d %H:%M:%S")
         end_date = self.sr_date.strftime("%Y-%m-%d %H:%M:%S")
         filtered_df = self.data.select("MSISDN", F.to_timestamp("Call Date", dateformat).alias("CallDate"),
                                        F.col('Cell Id').alias('CellId'), F.col('Flag').alias('CallStatus'))\
             .filter((F.col("MSISDN") == self.msisdn) & (F.col("CallDate") >= start_date) & (F.col("CallDate") <= end_date))
         return filtered_df

     def process_for_counts_and_ageing_days(self, filtered_df):
         mapping_expr = F.create_map([F.lit(x) for x in chain(*AGEING_DAY_WEIGHT_MAP.items())])
         count_segregation_df = filtered_df.withColumn("SuccessFlag",
                                                       F.when(filtered_df.CallStatus == 'Success', 1).otherwise(0))\
             .withColumn("DropFlag", F.when(filtered_df.CallStatus == 'Drop', 1).otherwise(0))\
             .withColumn("Age", F.datediff(F.lit(datetime.datetime.today().date()), F.col("CallDate")))\
             .withColumn("AgeScore", mapping_expr.getItem(F.datediff(F.lit(datetime.datetime.today().date()),
                                                                     F.col("CallDate")))).\
             groupBy(F.col("MSISDN"), F.col("CallDate"), F.col("CellId"), F.col('Age'), F.col('AgeScore')).\
             agg(F.sum('SuccessFlag').alias('SuccessCount'), F.sum('DropFlag').alias('DropCount'),
                 F.sum(F.col('SuccessFlag') + F.col('DropFlag')).alias("CellCallCount"),
                 (F.sum('DropFlag') / F.sum(F.col('SuccessFlag')+F.col('DropFlag'))).alias('DropRatio')).\
             orderBy(F.col('CellId'), ascending=True)
         return count_segregation_df

     def process_for_total_cell_calls_count(self, df):
         total_cell_call_count = df.groupBy('CellId').agg(F.sum('CellCallCount').alias("TotalCellCallCount"))
         return total_cell_call_count

     def get_total_call_count(self, df):
         total_call_count = df.agg(F.sum('CellCallCount').alias("TotalCallCount"))
         count = 0
         if len(total_call_count.collect()) > 0:
             count = total_call_count.collect()[0][0]
         return count

     def calculate_call_passing_probability(self, dataset, total_call_count):
         return dataset.select("*", (F.col('TotalCellCallCount')/total_call_count).alias('CallPassProbability'))

     def calculate_overall_score(self, dataset):
         return dataset.withColumn('OverallScore', F.round(F.when(dataset.CallPassProbability > 0,
                                                                 (dataset.AgeScore * dataset.DropRatio *
                                                                  dataset.CallPassProbability)).\
                                                          otherwise(0),2)).select("*")

     def get_ranked_list_desc_order(self, dataset):
         scored_dataset = self.calculate_overall_score(dataset)
         return scored_dataset.groupBy('MSISDN', 'CellId').agg(F.sum('OverallScore').alias("Score")).orderBy("Score",
                                                                                                      ascending=False)
