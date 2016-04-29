#!/bin/bash
mvn clean package
cd target
/Users/cbadenes/Tools/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class org.springframework.boot.loader.JarLauncher --master spark://zavijava.dia.fi.upm.es:3333 modeler-lda-online-0.1.jar