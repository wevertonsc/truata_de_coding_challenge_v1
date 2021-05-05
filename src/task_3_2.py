"""
- - - - - - - - - - - - - - - - - - - - - -
 TRUATA DE CODING CHALLENGE V1
- - - - - - - - - - - - - - - - - - - - - -
 Candidate: Weverton de Souza Castanho
 Email: wevertonsc@gmail.com
 Data: 28.APRIL.2021
- - - - - - - - - - - - - - - - - - - - - -
"""
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from pyspark.shell import sc

iris_class = ["Iris-setosa", "Iris-versicolor", "Iris-virginica"]


def map_classes(class_name):
    if class_name == "Iris-setosa":
        return 0
    elif class_name == "Iris-versicolor":
        return 1
    elif class_name == "Iris-virginica":
        return 2


def get_label_points(fields):
    sepal_length = float(fields[0])
    sepal_width = float(fields[1])
    petal_length = float(fields[2])
    petal_width = float(fields[3])
    species = map_classes(fields[4])
    return LabeledPoint(species, [sepal_length, sepal_width, petal_length, petal_width])


trainingData = sc.textFile("../in/iris.data").map(lambda x: x.split(",")).map(get_label_points)
mlr = LogisticRegressionWithLBFGS.train(trainingData, iterations=10, numClasses=3)

f32 = open("../out/out_3_2.txt", "w")
f32.write("Class" + "\n")
f32.write(str(iris_class[mlr.predict([5.1, 3.5, 1.4, 0.2])]))
f32.write("\n")
f32.write(str(iris_class[mlr.predict([6.2, 3.4, 5.4, 2.3])]))
f32.close()
