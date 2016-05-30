package org.librairy.modeler.lda.online.data;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * Created on 17/05/16:
 *
 * @author cbadenes
 */
@Data
public class BOW implements Serializable{

    Map<String,Long> frequencies;
}
