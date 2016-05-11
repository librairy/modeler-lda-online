package org.librairy.modeler.lda.online.data;

import lombok.Data;

import java.util.Map;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
@Data
public class Vocabulary {

    Map<String, Long> words;

    public Long getWordId(String word){
        return words.get(word.toLowerCase());
    }

}
