package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVilleEtAnnee").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/opt/bitnami/spark/ventes.txt");

        JavaPairRDD<Tuple2<String, String>, Double> ventesParVilleAnnee = lines
                .mapToPair(line -> {
                    String[] parts = line.split(" ");
                    String ville = parts[1];  // ville
                    String annee = parts[0].split("-")[0];  // année extraite de la date (ex: "2023-05-20" -> "2023")
                    Double prix = Double.parseDouble(parts[3]);
                    return new Tuple2<>(new Tuple2<>(ville, annee), prix);
                })
                .reduceByKey(Double::sum);

        ventesParVilleAnnee.foreach(tuple ->
                System.out.println("Ville: " + tuple._1._1 + ", Année: " + tuple._1._2 + ", Total: " + tuple._2)
        );

        sc.close();
    }
}
