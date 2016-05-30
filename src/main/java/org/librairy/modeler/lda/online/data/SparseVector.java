package org.librairy.modeler.lda.online.data;

import lombok.Data;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created on 17/05/16:
 *
 * @author cbadenes
 */
@Data
public class SparseVector implements Serializable{

    int size;

    Map<Integer,Double> elements;

//    List<Integer> index;
//
//    List<Double> value;
}
