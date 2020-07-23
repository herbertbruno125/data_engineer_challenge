package br.com.challenge.engineer;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class NasaAnalysisUtils {

    public static final PairFunction pairFunction = new PairFunction<Object, Integer, String>() {
        @Override
        public Tuple2<Integer, String> call(Object o) throws Exception {
            String[] values = o.toString().replaceAll("\\(", "")
                    .replaceAll("\\)", "")
                    .split(",", -1);
            return new Tuple2<Integer, String>(Integer.parseInt(values[0]), values[1]);
        }
    };

}
