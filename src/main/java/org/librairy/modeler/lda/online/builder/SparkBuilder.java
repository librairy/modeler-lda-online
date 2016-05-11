package org.librairy.modeler.lda.online.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Component
public class SparkBuilder {

    @Value("${spark.master}")
    private String master;

    public SparkConf sconf;

    public JavaSparkContext sc;

    @PostConstruct
    public void setup(){
        sconf = new SparkConf()
                .setAppName("librairy.lda.modeler")
                .setMaster(master)
                //.setMaster("spark://adamuz.local:7077")
//                .setMaster("spark://zavijava.dia.fi.upm.es:3333")
//                .setJars(new String[]{"/opt/spark/inbox/modeler-lda-online-0.1.jar"})
//                .set("spark.akka.frameSize","2047")
//                .set("spark.akka.threads","10")
//
        ;

        sc = new JavaSparkContext(sconf);
    }
}
