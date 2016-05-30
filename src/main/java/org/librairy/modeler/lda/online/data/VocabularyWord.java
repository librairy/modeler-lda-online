package org.librairy.modeler.lda.online.data;

import lombok.Data;

import java.io.Serializable;

/**
 * Created on 16/05/16:
 *
 * @author cbadenes
 */
@Data
public class VocabularyWord implements Serializable{
    String word;
    Long id;

    public VocabularyWord(String word, Long id){
        this.word   = word;
        this.id     = id;
    }
}
