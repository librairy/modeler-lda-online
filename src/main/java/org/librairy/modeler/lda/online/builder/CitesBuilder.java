package org.librairy.modeler.lda.online.builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Component
public class CitesBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CitesBuilder.class);


    @Value("${base.uri}")
    private String baseUri;

    @Value("${patent.reference.file}")
    private Resource refResource;

    ConcurrentHashMap<String,List<String>> references;

    @PostConstruct
    public void setup() throws IOException {

        references = new ConcurrentHashMap<>();

        LOG.info("Loading reference patents from : " + refResource.getURL());

        String line = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(refResource.getInputStream()));
        try {
            while((line = in.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line,"\t");

                String id           = tokenizer.nextToken();
                String uri          = baseUri + id;

                String x            = tokenizer.nextToken();
                String title        = tokenizer.nextToken();
                String date         = tokenizer.nextToken();
                String type         = tokenizer.nextToken();
                if ( tokenizer.hasMoreTokens()){
                    String categories   = tokenizer.nextToken();
                }


                // Only takes referenced
                List<String> cites = new ArrayList<String>();
                if ( tokenizer.hasMoreTokens()){
                    String citesString    = tokenizer.nextToken();

                    StringTokenizer valueTokenizer = new StringTokenizer(citesString,"|");
                    while(valueTokenizer.hasMoreTokens()){
                        String refId    =  valueTokenizer.nextToken();
                        String refUri   =  (refId.startsWith("H"))? baseUri + refId :  baseUri + "0" + refId;
                        cites.add(refUri);
                    }
                    references.put(uri,cites);
                }
            }
        } catch (IOException e) {
            LOG.warn("Error reading file",e);
            throw new RuntimeException(e);
        }
    }

    public List<String> getCitesFrom(String uri){
        List<String> result = references.get(uri);
        if (result == null){
            return Collections.EMPTY_LIST;
        }
        return result;
    }

    public List<String> getAllPatents(){
        return Collections.list(references.keys());
    }

    public List<String> getPatentsWithCites(){
        return references.entrySet().stream().
                filter(entry -> !entry.getValue().isEmpty()).
                map(entry -> entry.getKey()).
                collect(Collectors.toList());
    }

    public List<String> getPatentsWithoutCites(){
        return references.entrySet().stream().
                filter(entry -> entry.getValue().isEmpty()).
                map(entry -> entry.getKey()).
                collect(Collectors.toList());
    }
}
