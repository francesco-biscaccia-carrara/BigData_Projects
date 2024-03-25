
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class G021HW1 {
    public static void main(String[] args) {
        //Commandline check
        if(args.length != 5){
            throw new IllegalArgumentException("USAGE: filepath D M K L");
        }

        //command line parameter saving
        String file_path = args[0];
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);

        System.out.println("File path: " + file_path);
        System.out.println("D = " + D + " M = " + M + " K = " + K + " L = " + L);

        //Spark setup
        SparkConf conf = new SparkConf().setAppName("Outlier Detection").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            //Reduce verbosity
            sc.setLogLevel("WARN");


            JavaRDD<String> rawData = sc.textFile(file_path);

            JavaPairRDD<Float, Float> inputPoints;

            inputPoints = rawData.mapToPair(line -> {
                String[] coordinates = line.split(",");
                float x_coord = Float.parseFloat(coordinates[0]);
                float y_coord = Float.parseFloat(coordinates[1]);

                return new Tuple2<>(x_coord, y_coord);
            });

            inputPoints.repartition(L).cache();

            long num_points = inputPoints.count();
            System.out.println("Number of input points in the document = " + num_points);

            if (num_points <= 200000) {
                List<Tuple2<Float, Float>> listOfPoints = inputPoints.collect();

                long stopwatch_start = System.currentTimeMillis();
                Methods.ExactOutliers(listOfPoints, D, M, K);
                long stopwatch_stop = System.currentTimeMillis();
                long exec_time = stopwatch_stop - stopwatch_start;

                System.out.println("Running time for ExactOutliers: " + exec_time + " millisec");
            }

            long stopwatch_startMR = System.currentTimeMillis();
            Methods.MRApproxOutliers(inputPoints, D, M, K);
            long stopwatch_stopMR = System.currentTimeMillis();
            long exec_time = stopwatch_stopMR - stopwatch_startMR;

            System.out.println("Running time for MRApproxOutliers: " + exec_time + " millisec");

            sc.stop();
        }
    }
}

class Methods{
    private static float eucDistance(Tuple2<Float,Float> p1, Tuple2<Float,Float> p2){
        float x_diff = p1._1 - p2._1;
        float y_diff = p1._2 - p2._2;

        return (float) Math.sqrt(Math.pow(x_diff,2)+Math.pow(y_diff,2));
    }
    public static void ExactOutliers(List<Tuple2<Float,Float>> points, float D, int M, int K){

        ArrayList<Tuple2<Integer,Integer>> outliersPoints = new ArrayList<>();

        for(int i=0;i<points.size();i++){
            int dNeighborCountI = 0;
            for(int j=0;j<points.size();j++){
                if(i==j) continue;
                if(eucDistance(points.get(i), points.get(j)) <= D) dNeighborCountI++;
            }
            if(dNeighborCountI <= M)  outliersPoints.add(new Tuple2<>(i,dNeighborCountI));
        }
        
        //Sorting the list of outliers points by dNeighborCount(|B(p,D)|)
        outliersPoints.sort((e1, e2) -> e1._2().compareTo(e2._2));

        System.out.println("Number of ("+D+","+M+")-outliers: "+outliersPoints.size());
        for(int i = 0; i< (Math.min(outliersPoints.size(), K)); i++){
            //The first min(size of outliersPoints list,K) outliers points
            System.out.println(points.get(outliersPoints.get(i)._1));
        }
    }

    public static void MRApproxOutliers(JavaPairRDD<Float, Float> points, float D, int M, int K){
        //step a
        float lambda = (float) (D/(2*Math.sqrt(2)));

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCount = points.flatMapToPair(
                (pair)->{
                    HashMap<Tuple2<Integer, Integer>, Integer> count = new HashMap<>();
                    int i = (int) Math.floor(pair._1/lambda);
                    int j = (int) Math.floor(pair._2/lambda);

                    count.put(new Tuple2<>(i, j), 1+count.getOrDefault(new Tuple2<>(i, j), 0));

                    ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> pairsList = new ArrayList<>();
                    for (Map.Entry<Tuple2<Integer, Integer>, Integer> e : count.entrySet()) {
                        pairsList.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }

                    return pairsList.iterator();
                }
        )
                        .groupByKey();
        System.out.println(cellCount.collect());
        //step b

    }
}