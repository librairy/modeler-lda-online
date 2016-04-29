package org.librairy.modeler.lda.online.data;

import lombok.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;
import java.util.Map;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Data
public class Corpus implements Serializable {

    JavaPairRDD<Long, Vector> bagsOfWords;

    Map<String, Long> vocabulary;

    Map<Long, String> documents;

}