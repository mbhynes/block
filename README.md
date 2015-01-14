Dense distributed block matrix library for Apache Spark.

This is mainly an academic project; I spent two feverish weeks writing it in order to teach myself Scala and the Spark API. However it does indeed multiply Block Matrices, and has only MLlib as its dependency.

I was inspired and initially guided by both:
https://github.com/PasaLab/marlin
and
https://github.com/amplab/ml-matrix

However, as I said, this was a project for me to learn Scala and Spark, so I have only borrowed inspiritation from the above, not their source. I did steal a bit of their more utilitarian code for the testing/ScalaTestSuite/LocalSparkContext/etc stuff that wasn't very interesting to write. 
