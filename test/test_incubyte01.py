import unittest
from incubyte import incubyte01
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("incubyte01take1") \
    .master("local[2]") \
    .getOrCreate()

incu = incubyte01.Incu(sparksession=spark)

class TestIncubyteAssessment(unittest.TestCase):


    def test_createDF(self):
        """test df creation when data is given"""
        header = ['Name', 'Cust_l', 'Open_Dt', 'Consul_Dt', 'VAC_ID', 'DR_Name', 'State', 'Country', 'DOB', 'FLAG']
        data = [('John', 123458, 20101012, 20101013, 'MVD', '', 'TN', 'IND', 6031987, 'A')]
        df = spark.createDataFrame(data=data,schema=header).select('Name', 'Cust_l').collect()

        expecteddf = spark.read.option('header', True).option('inferSchema', True).format('csv').load('ut-data/userid.csv')\
            .select('Name', 'Cust_l').collect()

        self.assertEqual(df, expecteddf)

    def test_readTable(self):
        df = spark.read.option('header', True).format('csv').load('ut-data/table_au.csv')
        cn = df.count()
        self.assertEqual(cn, 1)



if __name__ == '__main__':
    unittest.main()
