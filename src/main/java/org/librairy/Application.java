package org.librairy;

import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.JndiConnectionFactoryAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
//@SpringBootApplication
@Configuration
@EnableAutoConfiguration(exclude = {JndiConnectionFactoryAutoConfiguration.class,DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,JpaRepositoriesAutoConfiguration.class,DataSourceTransactionManagerAutoConfiguration.class})
@ComponentScan({"org.librairy"})
@PropertySource({"classpath:application.properties","classpath:boot.properties"})
//@EnableConfigurationProperties
public class Application {


    @Bean
    public static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
        PropertySourcesPlaceholderConfigurer c = new PropertySourcesPlaceholderConfigurer();
//        c.setLocations(new ClassPathResource("application.properties"),new ClassPathResource("boot.properties"));
        return c;
    }

    @Bean
    public static EmbeddedServletContainerFactory getTomcatEmbeddedFactory(){
        TomcatEmbeddedServletContainerFactory servlet = new TomcatEmbeddedServletContainerFactory();
        servlet.setPort(5555);
        return servlet;
    }


    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args){


        try {
            ApplicationContext ctx = SpringApplication.run(Application.class, args);

            Integer size        = 50;
            Integer topics      = 50;
            Integer iterations  = 50;
            Double alpha        = 0.1;
            Double beta         = 0.1;

            if (args != null){

                if (args.length >0) {
                    size = Integer.valueOf(args[0]);


                    if (args.length > 1){
                        topics = Integer.valueOf(args[1]);

                        if (args.length > 2){
                            iterations = Integer.valueOf(args[2]);


                            if (args.length > 3){
                                alpha = Double.valueOf(args[3]);


                                if (args.length > 4){
                                    beta = Double.valueOf(args[4]);
                                }
                            }

                        }

                    }

                }


            }


            CorpusBuilder corpusBuilder = ctx.getBean(CorpusBuilder.class);
            Corpus corpus = corpusBuilder.newTrainingCorpus(size);

            ModelBuilder modelBuilder = ctx.getBean(ModelBuilder.class);
            LDAModel model = modelBuilder.newModel(corpus,alpha,beta,topics,iterations);

            File file = new File("/opt/spark/inbox/model-"+topics);
            if (file.exists()){
                Files.walkFileTree(file.toPath(), new FileVisitor() {

                    @Override
                    public FileVisitResult preVisitDirectory(Object dir, BasicFileAttributes attrs) throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Object file, BasicFileAttributes attrs) throws IOException {
                        System.out.println("Deleting file: "+file);
                        Files.delete((Path)file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Object file, IOException exc) throws IOException {
                        System.out.println(exc.toString());
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Object dir, IOException exc) throws IOException {
                        System.out.println("deleting directory :"+ dir);
                        Files.delete((Path)dir);
                        return FileVisitResult.CONTINUE;
                    }

                });
            }


            LOG.info("Saving the model: " + file.getAbsolutePath());
            SparkBuilder sparkBuilder = ctx.getBean(SparkBuilder.class);
            model.save(sparkBuilder.sc.sc(),file.getAbsolutePath());

            System.exit(0);
        } catch (Exception e) {
            LOG.error("Error executing test",e);
            System.exit(-1);
        }

    }
}
