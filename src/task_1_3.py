"""
- - - - - - - - - - - - - - - - - - - - - -
 TRUATA DE CODING CHALLENGE V1
- - - - - - - - - - - - - - - - - - - - - -
 Candidate: Weverton de Souza Castanho
 Email: wevertonsc@gmail.com
 Data: 28.APRIL.2021
- - - - - - - - - - - - - - - - - - - - - -
"""
from pyspark.shell import sc, sqlContext
from pyspark.sql.functions import desc

rdd = sc.textFile("../in/groceries.csv").flatMap(lambda line: line.split(","))
Counts = rdd.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
df = sqlContext.createDataFrame(Counts, ["product", "value"]).orderBy(desc("value")).limit(10).toPandas()

result = str(df)
print(result)

count = 0
f3 = open("../out/out_1_3.txt", "w")
for line in result.splitlines():
    if count > 0:
        f3.write("('" + (line[1:].lstrip()).replace("   ", "',") + ")" + "\n")
    count += 1

