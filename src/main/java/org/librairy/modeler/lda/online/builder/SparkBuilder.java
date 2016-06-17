package org.librairy.modeler.lda.online.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.datastax.spark.connector.*;

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
//                .setMaster("spark://adamuz.local:7077")
//                .setJars(new String[]{"/opt/spark/inbox/modeler-lda-online-0.1.jar"})
                .set("spark.cassandra.connection.host", "wiig.dia.fi.upm.es")
                .set("spark.cassandra.connection.port", "5011")
                .set("spark.task.maxFailures","1")
                .set("spark.cassandra.input.split.size_in_mb","1280")
                .set("spark.driver.maxResultSize","0")
                .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps " +
                        "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8090 -Dcom.sun.management" +
                        ".jmxremote.rmi.port=8091 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun" +
                        ".management.jmxremote.ssl=false -Djava.rmi.server.hostname=138.100.15.128 -Djava.net.preferIPv4Stack=true")
                .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps " +
                        "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8095 -Dcom.sun.management" +
                        ".jmxremote.rmi.port=8096 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun" +
                        ".management.jmxremote.ssl=false -Djava.rmi.server.hostname=138.100.15.128 -Djava.net.preferIPv4Stack=true")
                .set("spark.reducer.maxSizeInFlight","200m")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "hdfs://zavijava.dia.fi.upm.es/tmp/logs")
                .set("spark.default.parallelism","500")
//                .set("spark.akka.frameSize","2047")
//                .set("spark.network.timeout","1200")
//                .set("spark.executor.heartbeatInterval","300")
//                .set("spark.cassandra.input.fetch.size_in_rows","50000")
//                .set("spark.cassandra.input.metrics","false")
//                .setMaster("spark://zavijava.dia.fi.upm.es:3333")
//                .set("spark.akka.threads","10")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.kryoserializer.buffer.max","2024m")
//                .set("spark.kryo.registrationRequired", "true")
//                .registerKryoClasses(new Class[]{
//                        java.util.HashMap.class,
//                        java.lang.String.class,
//                        java.util.List.class,
//                        scala.Tuple2.class,
//                        java.lang.Long.class,
//                        org.apache.spark.mllib.linalg.Vector.class,
//                        org.apache.spark.mllib.linalg.DenseVector.class,
//                        double[].class,
//                        breeze.linalg.DenseVector.class,
//                        org.apache.spark.api.java.JavaUtils.SerializableMapWrapper.class
//                })
//                .set("spark.storage.memoryFraction", "0.7") //0.6
//                .set("spark.shuffle.memoryFraction", "0.1") //0.2
//                .set("spark.memory.useLegacyMode","true")
//                .set("spark.io.compression.codec", "lz4")

        ;

        sc = new JavaSparkContext(sconf);
        sc.setCheckpointDir("hdfs://zavijava.dia.fi.upm.es/tmp/checkpoints");
    }


    public void close(){
        sc.close();
    }
}
