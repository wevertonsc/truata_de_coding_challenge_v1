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

f = open("../out/out_1_1.txt", "a")
f.write(str(rdd.collect()))
f.close()
