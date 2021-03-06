package br.com.challenge.engineer;

import br.com.challenge.engineer.exceptions.NasaAnalysisException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;

public class NasaAnalysisDriver implements Serializable {

    public static final Logger LOG = Logger.getLogger(NasaAnalysisDriver.class);
    private static JavaSparkContext sc;
    private static FileSystem fs;
    private static JavaRDD<String> acessLogInputPath;
    private static String[] arguments;


    public NasaAnalysisDriver() throws Exception {
        init();

        start();
    }

    public static void main(String[] args) throws Exception {
        NasaAnalysisDriver nasaAnalysisDriver = new NasaAnalysisDriver();
        arguments = args;

    }

    private void init() throws IOException {
        sc = new JavaSparkContext(new SparkConf().setAppName("data engineer challenge nasa analysis")
                .setMaster("local[*]")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "4g"));

        fs = FileSystem.get(sc.hadoopConfiguration());

        sc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

        setInputPaths();

        setOutputPaths();

        clearOutputDirectory();

    }


    private void start() throws Exception {
        try {
            hostsUnicos(acessLogInputPath);
            totalErros404(acessLogInputPath);
            totalBytes(acessLogInputPath);
            uRLsQueMaisCausaramErro404();
            quantidadeErros404PorDia();
            acessLogInputPath.unpersist();
        } catch (Exception e) {
            e.printStackTrace();
            e.getCause();
            throw new NasaAnalysisException("Erro no processamento, abortando a execução");
        }
    }

    private void hostsUnicos(JavaRDD<String> input) throws IOException {
        System.out.println("\n\n>>>>>>> START OF APPLICATION: HOSTS UNICOS <<<<<<<\n\n");

        JavaPairRDD<String, Integer> outputMap = input.mapToPair(t -> {
            Matcher matcher = NasaAnalysisAux.hostsUnicosPattern.matcher(t);
            String line = null;
            if (matcher.find()) line = matcher.group();
            return new Tuple2<>(line, 1);
        }).reduceByKey(Integer::sum);
        outputMap.saveAsTextFile(NasaAnalysisAux.outputHostsUnicos);
        System.out.println("\n\nForam encontrados um total de " + outputMap.count() + " de Hosts Unicos");
        System.out.println("Para mais detalhes o arquivo de saída encontra-se em: " + NasaAnalysisAux.outputHostsUnicos);

        System.out.println("\n\n>>>>>>> END OF PROGRAM <<<<<<<\n\n");
        outputMap.unpersist();

    }


    private void totalErros404(JavaRDD<String> input) throws IOException {
        System.out.println("\n\n>>>>>>> START OF APPLICATION: TOTAL ERROS 404 <<<<<<<\n\n");

        JavaRDD<String> result = input.filter(line -> line.contains(NasaAnalysisAux._404)).coalesce(1);

        result.saveAsTextFile(NasaAnalysisAux.outputTotalErros404Aux);

        JavaRDD<Long> longJavaRDD = sc.parallelize(Collections.singletonList(result.count()));
        longJavaRDD.repartition(1).saveAsTextFile(NasaAnalysisAux.outputTotalErros404);
        System.out.println("\n\n numero total de errors 404 = " + result.count());
        System.out.println("Para mais detalhes o arquivo de saída encontra-se em: " + NasaAnalysisAux.outputTotalErros404);
        System.out.println("\n\n>>>>>>> END OF PROGRAM <<<<<<<\n\n");
        result.unpersist();
    }

    private void totalBytes(JavaRDD<String> input) {
        System.out.println("\n\n>>>>>>> START OF APPLICATION: TOTAL BYTES <<<<<<<\n\n");
        JavaPairRDD<Integer, Integer> pairRDD = input.mapToPair(t -> {
            Matcher matcher = NasaAnalysisAux.totalBytesPattern.matcher(t);
            String line = null;
            if (matcher.find()) line = matcher.group();
            try {
                return new Tuple2<>(1, Integer.parseInt(StringUtils.defaultString(line, NasaAnalysisAux._ZERO)));
            } catch (Exception e) {
                return new Tuple2<>(1, 0);
            }
        }).reduceByKey(Integer::sum);
        pairRDD.coalesce(1).saveAsTextFile(NasaAnalysisAux.outputTotalBytes);
        System.out.println("\n\n numero total de Bytes = " + pairRDD.take(1));
        System.out.println("Para mais detalhes o arquivo de saída encontra-se em: " + NasaAnalysisAux.outputTotalBytes);
        System.out.println("\n\n>>>>>>> END OF PROGRAM <<<<<<<\n\n");
        pairRDD.unpersist();
    }


    private void uRLsQueMaisCausaramErro404() {
        System.out.println("\n\n>>>>>>> START OF APPLICATION: URLS QUE MAIS CAUSARAM ERRO 404 <<<<<<<\n\n");

        JavaRDD<String> totalErros404AuxInput = sc.textFile(NasaAnalysisAux.outputTotalErros404Aux);

        JavaPairRDD<String, Integer> pairRDD = totalErros404AuxInput.mapToPair(t -> {
            Matcher matcher = NasaAnalysisAux.urlMaisCausaramError404Pattern.matcher(t);
            String line = null;
            if (matcher.find()) line = matcher.group();
            line = line.replaceAll("GET ", "");
            return new Tuple2<>(line, 1);
        }).reduceByKey(Integer::sum);

        List<Object> take = pairRDD.map(Tuple2::swap).mapToPair(NasaAnalysisUtils.pairFunction).sortByKey(false).take(5);

        sc.parallelize(take).coalesce(1).saveAsTextFile(NasaAnalysisAux.outputUrlsQueMaisCausaramErro404);
        System.out.println("\n\n URLS QUE MAIS CAUSARAM ERRO 404: \n\n");
        for (Object o : take) {
            System.out.println(o);
        }
        System.out.println("Para mais detalhes o arquivo de saída encontra-se em: " + NasaAnalysisAux.outputTotalBytes);
        System.out.println("\n\n>>>>>>> END OF PROGRAM <<<<<<<\n\n");
        pairRDD.unpersist();
    }


    private void quantidadeErros404PorDia() {
        System.out.println("\n\n>>>>>>> START OF APPLICATION: QUANTIDADE ERROS 404 POR DIA <<<<<<<\n\n");
        JavaRDD<String> totalErros404AuxInput = sc.textFile(NasaAnalysisAux.outputTotalErros404Aux).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Integer> pairRDD = totalErros404AuxInput.mapToPair(t -> {
            Matcher matcher = NasaAnalysisAux.quantidadeErros404PorDiaPattern.matcher(t);
            String line = null;
            if (matcher.find())
                line = matcher.group();
            return new Tuple2<>(line, 1);
        }).reduceByKey(Integer::sum);

        pairRDD.sortByKey().coalesce(1).saveAsTextFile(NasaAnalysisAux.outputQuantidadeErros404PorDia);
        System.out.println("\n\n QUANTIDADE ERROS 404 POR DIA = ");
        for (Object o : pairRDD.collect()) {
            System.out.println(o.toString());
        }
        System.out.println("Para mais detalhes o arquivo de saída encontra-se em: " + NasaAnalysisAux.outputTotalBytes);
        System.out.println("\n\n>>>>>>> END OF PROGRAM <<<<<<<\n\n");
        pairRDD.unpersist();
    }


    private void setInputPaths() {
        acessLogInputPath = sc.textFile(getClass().getClassLoader().getResource("") + "input/*").persist(StorageLevel.MEMORY_ONLY());
    }

    private void clearOutputDirectory() throws IOException {
        Path basePath = new Path(getClass().getClassLoader().getResource("") + "/output");
        if (fs.exists(basePath)) {
            fs.delete(basePath, true);
        }
        fs.mkdirs(basePath);
    }

    private void setOutputPaths() throws IOException {
        NasaAnalysisAux.outputHostsUnicos = getClass().getClassLoader().getResource("output") + "/hosts_unicos";
        NasaAnalysisAux.outputTotalErros404 = getClass().getClassLoader().getResource("output") + "/total_erros_404";
        NasaAnalysisAux.outputTotalErros404Aux = getClass().getClassLoader().getResource("output") + "/total_erros_aux";
        NasaAnalysisAux.outputTotalBytes = getClass().getClassLoader().getResource("output") + "/output_total_bytes";
        NasaAnalysisAux.outputUrlsQueMaisCausaramErro404 = getClass().getClassLoader().getResource("output") + "/URLsQueMaisCausaramErro404";
        NasaAnalysisAux.outputQuantidadeErros404PorDia = getClass().getClassLoader().getResource("output") + "/quantidadeErros404PorDia";
    }


}
