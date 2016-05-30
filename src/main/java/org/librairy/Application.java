package org.librairy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.FileBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.librairy.modeler.lda.online.task.PrepareModelTask;
import org.librairy.modeler.lda.online.task.TestModelTask;
import org.librairy.modeler.lda.online.task.TrainModelTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.JndiConnectionFactoryAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;

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
//@PropertySource({"classpath:application.properties","classpath:boot.properties"})
@PropertySource({"classpath:application.properties"})
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
            Instant start = Instant.now();

            ApplicationContext ctx = SpringApplication.run(Application.class, args);


            if (args == null || args.length == 0) throw new RuntimeException("Invalid Arguments. You must define " +
                    "'train' or 'test' as first argument");


            String task = args[0];
            if (task.equalsIgnoreCase("train")) new TrainModelTask(ctx).run(args);
            else if (task.equalsIgnoreCase("test")) new TestModelTask(ctx).run(args);
            else if (task.equalsIgnoreCase("prepare")) new PrepareModelTask(ctx).run(args);
            else throw new RuntimeException
                        ("Task not handled: " + task + ". Only accepted 'train' or 'test'");

            Instant end = Instant.now();
            LOG.info("Total elapsed Time: "       + ChronoUnit.MINUTES.between(start,end) + "min " + (ChronoUnit.SECONDS
                    .between(start,end)%60) + "secs");

            System.exit(0);
        } catch (Exception e) {
            LOG.error("Error executing test",e);
            System.exit(-1);
        }

    }
}
