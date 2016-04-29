#!/bin/bash
mvn clean package
cd target
cp modeler-lda-online-0.1.jar /opt/spark/inbox
scp modeler-lda-online-0.1.jar cbadenes@minetur.dia.fi.upm.es:/opt/spark/inbox/
/Users/cbadenes/Tools/spark-1.6.1-bin-hadoop2.6/bin/spark-submit -v \
 --class org.librairy.Application \
 --master spark://zavijava.dia.fi.upm.es:3333 local:/opt/spark/inbox/modeler-lda-online-0.1.jar 1000 100 100

#--executor-memory 10G \
  #--total-executor-cores 52 \

# --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \

# --class org.springframework.boot.loader.JarLauncher \
