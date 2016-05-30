package org.librairy.modeler.lda.online.data;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Created on 29/04/16:
 *
 * @author cbadenes
 */
@Data
public class UriSet implements Serializable {
    List<String> uris;
}
