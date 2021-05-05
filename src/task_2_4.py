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

df = sqlContext.sql("SELECT  neighbourhood_cleansed "
                    ",max(accommodates) as max_accommodates"
                    ",min(price) as min_price "
                    ",max(review_scores_rating) as max_ratings "
                    "FROM parquet.`../in/sf-airbnb-clean.parquet` "
                    "group by neighbourhood_cleansed ")

df.toPandas().to_csv('../out/out_2_4.csv', index=False)
