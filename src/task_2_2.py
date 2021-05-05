"""
- - - - - - - - - - - - - - - - - - - - - -
 TRUATA DE CODING CHALLENGE V1
- - - - - - - - - - - - - - - - - - - - - -
 Candidate: Weverton de Souza Castanho
 Email: wevertonsc@gmail.com
 Data: 28.APRIL.2021
- - - - - - - - - - - - - - - - - - - - - -
"""
from pyspark.shell import sqlContext

df = sqlContext.sql("SELECT neighbourhood_cleansed"
                    ",min(price) as min_price "
                    ",max(price) as_max_price "
                    ",count(price) as row_count "
                    "FROM parquet.`../in/sf-airbnb-clean.parquet` "
                    "group by  neighbourhood_cleansed")

df.toPandas().to_csv('../out/out_2_2.csv', index=False)
