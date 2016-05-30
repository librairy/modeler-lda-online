#!/bin/bash
mvn clean package
cd target
cp modeler-lda-online-0.1.jar /opt/spark/inbox