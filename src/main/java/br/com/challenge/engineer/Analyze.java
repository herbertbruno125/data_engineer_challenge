package br.com.challenge.engineer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Analyze implements Serializable {

    protected SparkConf conf;
    protected JavaRDD<String> input;
    protected JavaSparkContext sc;
    protected String outputHostsUnicos, outputTotalErros404, outputTotalErros404Aux, outputTotalBytes,
            outputUrlsQueMaisCausaramErro404, outputQuantidadeErros404PorDia;
    private FileSystem fs;

    public static void main(String[] args) throws IOException {
        Analyze analyze = new Analyze();
        analyze.init(args);
        analyze.hostsUnicos();
        analyze.totalErros404();
        analyze.uRLsQueMaisCausaramErro404();
        analyze.totalBytes();
        analyze.quantidadeErros404PorDia();
    }

    private void init(String[] args) throws IOException {

        //Configurações do Spark
        conf = new SparkConf().setAppName("data engineer challenge")
                .setMaster("local[*]")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "4g");

        sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        fs = FileSystem.get(sc.hadoopConfiguration());

        sc.hadoopConfiguration().addResource(new Path(new File(".").getCanonicalPath() + "/src/main/resources/teste.xml"));

        //Diretório de entrada
        setInputPath();

        //Diretórios de saida
        clearOutputDirectory();
        setOutputPaths();

    }

    private void hostsUnicos() throws IOException {
        Pattern pattern = Pattern.compile("^\\S*");
        JavaPairRDD<String, Integer> outputMap = input.mapToPair(t -> {
            Matcher matcher = pattern.matcher(t);
            String line = null;
            if (matcher.find()) line = matcher.group();
            return new Tuple2<>(line, 1);
        }).reduceByKey(Integer::sum);

        outputMap.saveAsTextFile(outputHostsUnicos);
    }

    private void totalErros404() throws IOException {
        System.out.println("\n\n>>>>>>> START OF PROGRAM <<<<<<<\n\n");

        JavaRDD<String> result = input.filter(line -> line.contains(" 404 ")).coalesce(1);

        result.saveAsTextFile(outputTotalErros404Aux);

        JavaRDD<Long> longJavaRDD = sc.parallelize(Arrays.asList(result.count()));
        longJavaRDD.saveAsTextFile(outputTotalErros404);
        System.out.println("\n\nThe total number of errors 404 = ");
        System.out.println("\n\n>>>>>>> END OF PROGRAM <<<<<<<\n\n");
    }

    private void totalBytes() {
        Pattern pattern = Pattern.compile("[\\d-]+$");

        JavaPairRDD<Integer, Integer> pairRDD = input.mapToPair(t -> {
            Matcher matcher = pattern.matcher(t);
            String line = null;
            if (matcher.find()) line = matcher.group();

            try {
                return new Tuple2<>(1, Integer.parseInt(line));
            } catch (Exception e) {
                return new Tuple2<>(1, 0);
            }

        }).reduceByKey(Integer::sum);

        pairRDD.coalesce(1).saveAsTextFile(outputTotalBytes);
    }

    private void uRLsQueMaisCausaramErro404() {
        Pattern pattern = Pattern.compile("\"(.*?)\"");

        JavaRDD<String> input = sc.textFile(outputTotalErros404Aux);

        JavaPairRDD<String, Integer> pairRDD = input.mapToPair(t -> {
            Matcher matcher = pattern.matcher(t);
            String line = null;
            if (matcher.find()) line = matcher.group();
            line = line.replaceAll("GET ", "");
            return new Tuple2<>(line, 1);
        }).reduceByKey(Integer::sum).persist(StorageLevel.MEMORY_ONLY());

        List<Object> take = pairRDD.map(Tuple2::swap).mapToPair(Utils.pairFunction).sortByKey(false).persist(StorageLevel.MEMORY_ONLY()).take(5);

        sc.parallelize(take).coalesce(1).saveAsTextFile(outputUrlsQueMaisCausaramErro404);
    }


    private void quantidadeErros404PorDia() {
        Pattern pattern = Pattern.compile("(([0-9])|([0-2][0-9])|([3][0-1]))\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}");

        JavaRDD<String> input = sc.textFile(outputTotalErros404Aux).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Integer> pairRDD = input.mapToPair(t -> {
            Matcher matcher = pattern.matcher(t);
            String line = null;
            if (matcher.find())
                line = matcher.group();
            return new Tuple2<>(line, 1);
        }).reduceByKey(Integer::sum).persist(StorageLevel.MEMORY_ONLY());

        pairRDD.sortByKey().coalesce(1).saveAsTextFile(outputQuantidadeErros404PorDia);
    }

    private void clearOutputDirectory() throws IOException {
        Path basePath = new Path(new File(".").getCanonicalPath() + "/src/output/");
        if (fs.exists(basePath)) {
            fs.delete(basePath, true);
        }
        fs.mkdirs(basePath);
    }

    private void setOutputPaths() throws IOException {

        outputHostsUnicos = new File(".").getCanonicalPath() + "/src/output/hosts_unicos";
        outputTotalErros404 = new File(".").getCanonicalPath() + "/src/output/total_erros_404";
        outputTotalErros404Aux = new File(".").getCanonicalPath() + "/src/output/total_erros_aux";
        outputTotalBytes = new File(".").getCanonicalPath() + "/src/output/output_total_bytes";
        outputUrlsQueMaisCausaramErro404 = new File(".").getCanonicalPath() + "/src/output/URLsQueMaisCausaramErro404";
        outputQuantidadeErros404PorDia = new File(".").getCanonicalPath() + "/src/output/quantidadeErros404PorDia";
    }

    private void setInputPath() throws IOException {
        input = sc.textFile(new File(".").getCanonicalPath() + "/src/input/*").persist(StorageLevel.MEMORY_ONLY());
    }
}
