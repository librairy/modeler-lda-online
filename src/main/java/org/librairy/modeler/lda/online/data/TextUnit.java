package org.librairy.modeler.lda.online.data;

import lombok.Data;

import java.io.Serializable;

/**
 * Created on 13/05/16:
 *
 * @author cbadenes
 */
@Data
public class TextUnit implements Serializable{

    private String uri;

    private String tokens;
}
