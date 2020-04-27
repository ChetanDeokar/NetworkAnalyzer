from os.path import abspath

from pyspark import HiveContext
from pyspark import SQLContext
from pyspark import SparkContext, sql


class SparkUtility(object):

    spark = None
    spark_context = None
    sql_context = None
    app_name = 'testApp'
    master_name = 'local'

    def set_app(self, app_name):
        self.app_name = app_name

    def set_master(self, master):
        self.master_name = master

    def set_spark_session(self):
        #warehouse_location = "hdfs://192.168.43.185:9000/user/hive/warehouse"
        self.spark = sql.SparkSession \
            .builder \
            .master(self.master_name)\
            .appName(self.app_name) \
            .enableHiveSupport()\
            .getOrCreate()
            #.config("spark.sql.warehouse.dir", warehouse_location)\

    def get_spark_session(self):
        if not self.spark:
            self.set_spark_session()
        return self.spark

    def stop_session(self):
        if self.spark:
            self.spark.stop()

    def get_spark_context(self):
        return SparkContext(self.master_name, self.app_name)

    def get_sql_context(self):
        return SQLContext(self.get_spark_context())

    def read_csv_as_dataframe(self, file_path, **options):
        sql_context = self.spark if self.spark else self.get_sql_context()
        return sql_context.read.format('com.databricks.spark.csv')\
            .options(**options).load(file_path)

    def inner_join_dataframe_over_column(self, source_dataframe, to_be_joined_dataframe, column_name):
        return source_dataframe.join(to_be_joined_dataframe, column_name)

    def read_hive_table(self, db_name, table_name, select_clause="*", where_clause=None):
        sql_str = "select %s from %s.%s" % (select_clause, db_name, table_name)
        if where_clause and where_clause.strip():
            sql_str += " where %s" % where_clause
        return self.spark.sql(sql_str)
