package org.librairy.modeler.lda.online.data;

import lombok.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Map;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
@Data
public class Vocabulary implements Serializable {

    Map<String,Long> words;

    int size;

    public void setWords(Map<String,Long> words){
        this.words = words;
        this.size = words.size();
    }

}
