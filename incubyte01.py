from pyspark.sql import *
from pyspark.sql.functions import *

class Incu:

    def __init__(self, sparksession):
        self.spark = sparksession
        self.df = None


    def createDF(self):
        try:
            header = ['Name', 'Cust_l', 'Open_Dt', 'Consul_Dt', 'VAC_ID', 'DR_Name', 'State', 'Country', 'DOB', 'FLAG']
            data = [('Alex', 123457, 20101012, 20101013, 'MVD', 'Paul', 'SA', 'USA', 6031987, 'A'),
                    ('John', 123458, 20101012, 20101013, 'MVD', None, 'TN', 'IND', 6031987, 'A'),
                    ('Mathew', 123459, 20101012, 20101013, 'MVD', None, 'WAS', 'PHIL', 6031987, 'A'),
                    ('Matt', 12345, 20101012, 20101013, 'MVD', None, 'BOS', 'NYC', 6031987, 'A'),
                    ('Jacob', 1256, 20101012, 20101013, 'MVD', None, 'VIC', 'AU', 6031987, 'A')]
            self.df = self.spark.createDataFrame(data=data, schema=header)
            self.df.show()
        except:
            Exception

    def saveDF(self):
        try:
            myrowlist = self.df.select('Country').distinct().collect()
            mylist = [row['Country'] for row in myrowlist]
            print(mylist)
            for i in mylist:
                self.df.filter(col('Country') == i) \
                    .write.mode('overwrite')\
                    .option('header', True)\
                    .saveAsTable('table_{}'.format(i))
        except:
            Exception


    def readTable(self, Cn):
        try:
            print("""this function will read data from table based on the country name given""")
            df = spark.read.option('header', True).format('csv').load('spark-warehouse/table_{}'.format(Cn))
            df.show()
            return df
        except:
            Exception

if __name__ == '__main__':
    spark = SparkSession \
    .builder \
    .appName("incubyte01take1") \
    .master("local[2]") \
    .getOrCreate()

    incu = Incu(sparksession=spark)
    incu.createDF()
    incu.saveDF()
    incu.readTable('au')




