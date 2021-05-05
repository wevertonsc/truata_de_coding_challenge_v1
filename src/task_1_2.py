"""
- - - - - - - - - - - - - - - - - - - - - -
 TRUATA DE CODING CHALLENGE V1
- - - - - - - - - - - - - - - - - - - - - -
 Candidate: Weverton de Souza Castanho
 Email: wevertonsc@gmail.com
 Data: 28.APRIL.2021
- - - - - - - - - - - - - - - - - - - - - -
"""
from pyspark.shell import sc

rdd = sc.textFile("../in/groceries.csv").map(lambda line: line.split(','))
rddDistinct = rdd.flatMap(lambda x: x).distinct()

f2a = open("../out/out_1_2a.txt", "w")
for i in rddDistinct.collect():
    f2a.write(str(i) + "\n")
f2a.close()

f2b = open("../out/out_1_2b.txt", "w")
f2b.write("Count:" + "\n")
f2b.write(str(rddDistinct.count()))
f2b.close()