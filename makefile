SCALA_VER=2.10
CLASS=Axb
JAR=$(shell find . -name '*.jar')

ARGS="-n 22 -c 31 -p 2000 -s 100M test_out"
# ARGS="-n 22 -c 31 -p 2000 -s 100M "

package:
	sbt package
compile:
	sbt compile
run:
	echo Running $(JAR)
	spark_run $(CLASS) $(ARGS)
clean:
	rm -r out_*
