"""
- - - - - - - - - - - - - - - - - - - - - -
 TRUATA DE CODING CHALLENGE V1
- - - - - - - - - - - - - - - - - - - - - -
 Candidate: Weverton de Souza Castanho
 Email: wevertonsc@gmail.com
 Data: 28.APRIL.2021
- - - - - - - - - - - - - - - - - - - - - -
"""
from pyspark.shell import spark, sqlContext

df = sqlContext.read.parquet("../in/sf-airbnb-clean.parquet")

df.show()

df.toPandas().to_csv('../out/out_2_1.csv')
