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

df = sqlContext.sql("SELECT neighbourhood_cleansed "
                    ",avg(bathrooms) as avg_bathrooms "
                    ",avg(bedrooms) as avg_bedrooms"
                    " FROM parquet.`../in/sf-airbnb-clean.parquet` "
                    "where price > 5000 "
                    "group by  neighbourhood_cleansed")

df.toPandas().to_csv('../out/out_2_3.csv', index=False)
