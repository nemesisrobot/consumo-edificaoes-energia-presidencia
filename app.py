#Author:Diego Silva
#Date:17/07/2021
#Description:Scrit to analyse datas of consumption energy of palace of president

#load envoriment operation system
import datetime
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, DateType, StringType 
from pyspark.sql.functions import col

#class to get session to process datas
class LoadSettingsSparkConnection:

    #method to get session
    def getSessionSpark(self, master_name='local[1]', app='treino') -> SparkSession:
        return SparkSession.builder.master(master_name).appName(app).getOrCreate()


class KeepDatas:

    @staticmethod
    def save_data_bases(dataframe, path_save):
        aux_path = ((str(datetime.datetime.now())[0:19].replace('-','')).replace(' ','')).replace(':','')
        dataframe.write.format("csv").option("header",True).save('{}/{}'.format(path_save,aux_path))


#read database
spark = LoadSettingsSparkConnection()
sparksession = spark.getSessionSpark()
df = sparksession.read.csv('base/PesquisaDesenvolvimentoTema.csv', header=True, sep=',')

#alter datatype of column 
df = df.withColumn("vlrEstimado", col("vlrEstimado").cast(DecimalType()))
print(df.printSchema())
df.createTempView("table_projects")

#split datas inf down and up price
downprice = sparksession.sql("select dscProjeto, vlrEstimado from table_projects where vlrEstimado <= (select avg(vlrEstimado) from table_projects)")
upprice = sparksession.sql("select dscProjeto, vlrEstimado from table_projects where vlrEstimado > (select avg(vlrEstimado) from table_projects)")

#cast column decimal to string
downprice = downprice.withColumn("vlrEstimado", col("vlrEstimado").cast(StringType()))
upprice = upprice.withColumn("vlrEstimado", col("vlrEstimado").cast(StringType()))


#save datas
KeepDatas.save_data_bases(upprice,"reports/up")
KeepDatas.save_data_bases(downprice,"reports/down")

#upprice.write.format
sparksession.stop()