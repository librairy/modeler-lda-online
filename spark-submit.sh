#!/bin/bash
mvn clean package
cd target
cp modeler-lda-online-0.1.jar /opt/spark/inbox
scp modeler-lda-online-0.1.jar cbadenes@zavijava.dia.fi.upm.es:/opt/spark/inbox/


#/Users/cbadenes/Tools/spark-1.6.1-bin-hadoop2.6/bin/spark-submit -v \
# --class org.librairy.Application \
# --master spark://zavijava.dia.fi.upm.es:3333 local:/opt/spark/inbox/modeler-lda-online-0.1.jar 1000 100 100

#./spark-submit -v \
#--class org.librairy.Application \
#--conf spark.driver.maxResultSize=0 \
#--conf spark.driver.cores=1 \
#--conf spark.driver.memory=6G \
#--conf spark.executor.memory=10G \
#--conf spark.executor.cores=10 \
#--conf spark.reducer.maxSizeInFlight=200m \
#--conf spark.default.parallelism=6 \
#--conf spark.executor.heartbeatInterval=30s \
#--conf spark.akka.threads=10 \
#--conf spark.akka.frameSize=2047 \
#--conf spark.core.connection.auth.wait.timeout=1000 \
#--master spark://zavijava.dia.fi.upm.es:3333 local:/opt/spark/inbox/modeler-lda-online-0.1.jar train 1000 100 100


# --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
# --class org.springframework.boot.loader.JarLauncher \


## JConsole
#-conf "spark.driver.extraJavaOptions=-Dcom.sun.management.jmxremote \
#-Dcom.sun.management.jmxremote.port=54321 \
#-Dcom.sun.management.jmxremote.rmi.port=54320 \
#-Dcom.sun.management.jmxremote.authenticate=false \
#-Dcom.sun.management.jmxremote.ssl=false \
#-Djava.rmi.server.hostname=zavijava.dia.fi.upm.es" \
