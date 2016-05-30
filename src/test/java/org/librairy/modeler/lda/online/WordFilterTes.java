package org.librairy.modeler.lda.online;

import com.google.common.base.CharMatcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created on 17/05/16:
 *
 * @author cbadenes
 */
public class WordFilterTes {


    @Test
    public void checkWord(){

        List<String> words = Arrays.asList(new String[]{
                "aa.bb.cc.dd",
                "tpt_island_map",
                "<fourth embodiment of pics providing procedure>",
                "tire/surface",
                "08099668.txt",
                "uridine-kinase",
                "rule9",
                "πiri",
                "spxaudio"
        });

        for (String word : words){

            boolean c1 = CharMatcher.JAVA_LETTER_OR_DIGIT.matchesAllOf(word);

            boolean c2 = Pattern.matches("[^0-9()+\\-*\\/%]+", word);
            
            System.out.println("Word: " + word + "|  c1=" + c1+ " / c2=" +c2 );
        }



    }

}
