package org.librairy.modeler.lda.online.builder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
public class FileBuilder {

    public static File newFile(String path, Boolean override) throws IOException {
        File file = new File(path);
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
        return file;
    }

}
