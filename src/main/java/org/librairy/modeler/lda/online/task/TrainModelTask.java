package org.librairy.modeler.lda.online.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.spark.mllib.clustering.LDAModel;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.FileBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.IOException;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
public class TrainModelTask {

    private static final Logger LOG = LoggerFactory.getLogger(TrainModelTask.class);

    public static final String BASE_DIRECTORY = "/opt/spark/model";

    private final CorpusBuilder corpusBuilder;
    private final ModelBuilder modelBuilder;
    private final SparkBuilder sparkBuilder;

    public TrainModelTask(ApplicationContext ctx){
        this.corpusBuilder  = ctx.getBean(CorpusBuilder.class);
        this.modelBuilder   = ctx.getBean(ModelBuilder.class);
        this.sparkBuilder   = ctx.getBean(SparkBuilder.class);


    }


    public void run(String[] args) throws IOException {

        Integer size        = 50;
        Integer vocabSize   = 100;
        Integer topics      = 50;
        Integer iterations  = 50;
        Double alpha        = 0.1;
        Double beta         = 0.1;
        Boolean perplexity  = false;



        if (args.length >1) {
            size = Integer.valueOf(args[1]);


            if (args.length > 2){
                vocabSize = Integer.valueOf(args[2]);

                if (args.length > 3){
                    topics = Integer.valueOf(args[3]);

                    if (args.length > 4){
                        iterations = Integer.valueOf(args[4]);

                        if (args.length > 5){
                            perplexity = Boolean.valueOf(args[5]);


                            if (args.length > 6){
                                alpha = Double.valueOf(args[6]);


                                if (args.length > 7){
                                    beta = Double.valueOf(args[7]);
                                }
                            }
                        }
                    }
                }

            }

        }


        //Corpus corpus = corpusBuilder.newTrainingCorpus(size);

        try{
            LDAModel model = modelBuilder.train(size,vocabSize,alpha,beta,topics,iterations,perplexity);

            String name = "/model-"+size+"-"+topics+"-"+iterations;
//            LOG.info("Saving the model: " + name);
//            model.save(sparkBuilder.sc.sc(),"hdfs://zavijava.dia.fi.upm.es/tmp/models/"+name);
        }catch (Exception e){
            if (e instanceof FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error building model", e);
            }
        }

    }
}
