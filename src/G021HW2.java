import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

public class G021HW2 {
    public static void main(String[] args) {
        //Commandline check
        if(args.length != 4){
            throw new IllegalArgumentException("USAGE: filepath M K L");
        }

        //Task 3 point 1 - Prints the command-line arguments and stores M,K,L into suitable variables.
        String file_path = args[0];
        int M = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int L = Integer.parseInt(args[3]);

        System.out.println(file_path + " M=" + M + " K=" + K + " L=" + L);

        //Spark setup

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true)
                .setAppName("Outlier Detection V2")
                .setMaster("local[*]")
                .set("spark.locality.wait", "0s");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.setLogLevel("WARN");

            //Task 3 point 2 - Reads the input points into an RDD of strings (called rawData) and transform it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
            JavaRDD<String> rawData = sc.textFile(file_path).repartition(L).cache();

            //Conversion of string to a pair of points and storing in a new RDD
            JavaRDD<Tuple2<Float, Float>> inputPoints = rawData.map(line -> {
                String[] coordinates = line.split(",");
                float x_coord = Float.parseFloat(coordinates[0]);
                float y_coord = Float.parseFloat(coordinates[1]);

                return new Tuple2<>(x_coord, y_coord);
            });

            //Task 3 point 3 - Prints the total number of points.
            long num_points = inputPoints.count();
            System.out.println("Number of points = " + num_points);

            //Executes MRFFT with parameters inputPoints and K and stores the returned radius into a float D.
            float D = MethodsHW2.MRFFT(inputPoints,K,sc);
            System.out.println("Radius = "+ D);

            long stopwatch_startMR = System.currentTimeMillis();
            //Executes MRApproxOutliers, modified as described above, with parameters inputPoints, D,M.
            MethodsHW2.MRApproxOutliers(inputPoints, D,M);
            long stopwatch_stopMR = System.currentTimeMillis();
            long exec_time = stopwatch_stopMR - stopwatch_startMR;
            //Prints MRApproxOutliers' running time. Again the stopwatch variable saves the current time when method starts and finishes
            System.out.println("Running time of MRApproxOutliers = " + exec_time + " ms");

            sc.stop();
        }
    }
}

class MethodsHW2{
    private static float eucDistance(Tuple2<Float,Float> p1, Tuple2<Float,Float> p2){
        float x_diff = p1._1 - p2._1;
        float y_diff = p1._2 - p2._2;

        return (float) Math.sqrt(Math.pow(x_diff,2)+Math.pow(y_diff,2));
    }

    private static Tuple2<Integer, Integer> determineCell(Tuple2<Float, Float> point, float D) {
        float lambda = (float) (D/(2*Math.sqrt(2)));

        int i = (int) Math.floor(point._1/lambda);
        int j = (int) Math.floor(point._2/lambda);

        return new Tuple2<>(i, j);
    }

    public static void MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> points, float D, int M) {

        JavaPairRDD<Tuple2<Integer, Integer>, Long> cellCount = points.mapToPair(
                (pair) -> new Tuple2<>(determineCell(pair, D), 1L)
        ).reduceByKey(Long::sum);

        Map<Tuple2<Integer, Integer>, Long> tmpMap = cellCount.collectAsMap();
        HashMap<Tuple2<Integer, Integer>, Tuple3<Long, Long, Long>> pairSizeN3N7 = new HashMap<>();

        for(Map.Entry<Tuple2<Integer, Integer>, Long> e : tmpMap.entrySet()){
            Tuple2<Tuple2<Integer,Integer>,Long> pair =  new Tuple2<>(e.getKey(),e.getValue());

            pairSizeN3N7.put(pair._1, new Tuple3<>(pair._2, 0L, 0L));

            for (int i = -3; i < 4; i++) {
                for (int j = -3; j < 4; j++) {
                    if (tmpMap.get(new Tuple2<>(pair._1._1 + i, pair._1._2 + j)) != null) {
                        long cellIJCount = tmpMap.get(new Tuple2<>(pair._1._1 + i, pair._1._2 + j));
                        if ((i < -1 || i > 1) || (j < -1 || j > 1))
                            pairSizeN3N7.put(pair._1, new Tuple3<>(pair._2, pairSizeN3N7.get(pair._1)._2(), pairSizeN3N7.get(pair._1)._3() + cellIJCount));
                        else
                            pairSizeN3N7.put(pair._1, new Tuple3<>(pair._2, pairSizeN3N7.get(pair._1)._2() + cellIJCount, pairSizeN3N7.get(pair._1)._3() + cellIJCount));
                    }
                }
            }
        }

        int outliers = 0, uncertains = 0;
        for (Map.Entry<Tuple2<Integer, Integer>, Tuple3<Long, Long, Long>> elem : pairSizeN3N7.entrySet()) {
            if (elem.getValue()._3() <= M) outliers += elem.getValue()._1();
            if (elem.getValue()._2() <= M && elem.getValue()._3() > M) uncertains += elem.getValue()._1();
        }
        System.out.println("Number of sure outliers = " + outliers);
        System.out.println("Number of uncertain points = " + uncertains);
    }

    private static Float findRadius(Tuple2<Float,Float> point,ArrayList<Tuple2<Float,Float>> centers){
        float distFromS = Float.MAX_VALUE;

        for(Tuple2<Float,Float> center: centers)
            if (eucDistance(point, center) < distFromS) distFromS = eucDistance(point, center);

        return distFromS;
    }

    private static void updatePointDistance(ArrayList<Tuple2<Tuple2<Float,Float>,Float>> pointsDist, Tuple2<Float,Float> newCenter){
        for(int i =0;i<pointsDist.size();i++)
            if (eucDistance(pointsDist.get(i)._1, newCenter) < pointsDist.get(i)._2)  pointsDist.set(i,new Tuple2<>(pointsDist.get(i)._1,eucDistance(pointsDist.get(i)._1, newCenter)));
    }

    private static ArrayList<Tuple2<Float,Float>> SequentialFFT(ArrayList<Tuple2<Float,Float>> points, int K){
        ArrayList<Tuple2<Float,Float>> centers = new ArrayList<>();

        //Initialization of tmp list
        ArrayList<Tuple2<Tuple2<Float,Float>,Float>> pointsDist = new ArrayList<>();
        for(Tuple2<Float,Float> point : points) pointsDist.add(new Tuple2<>(point,Float.MAX_VALUE));

        centers.add(pointsDist.remove(0)._1);

        for(int i=1;i<K;i++){
            updatePointDistance(pointsDist,centers.get(i-1));
            int newCenterIndex = pointsDist.indexOf(Collections.max(pointsDist, (e1,e2)-> e1._2.compareTo(e2._2)));
            centers.add(pointsDist.remove(newCenterIndex)._1);
        }

        return centers;
    }

    public static float MRFFT(JavaRDD<Tuple2<Float, Float>> points, int K, JavaSparkContext sc){

        long stopwatch_startRound1 = System.currentTimeMillis();
        //Round 1
        JavaRDD<Tuple2<Float,Float>> coresets = points.mapPartitions(
                (partition) -> {
                    ArrayList<Tuple2<Float,Float>> partitionPoints = new ArrayList<>();
                    partition.forEachRemaining(partitionPoints::add);

                    return SequentialFFT(partitionPoints,K).iterator();
                }
        ).persist(StorageLevel.MEMORY_AND_DISK());
        coresets.count(); //Dummy action
        //System.out.println("Coreset size = "+ coresets.count());
        System.out.println("Running time of MRFFT Round 1 = " + (System.currentTimeMillis() - stopwatch_startRound1) + " ms");


        long stopwatch_startRound2= System.currentTimeMillis();
        //Round 2
        Broadcast<ArrayList<Tuple2<Float, Float>>> kCenters = sc.broadcast(SequentialFFT(new ArrayList<>(coresets.collect()),K));
        System.out.println("Running time of MRFFT Round 2 = " + (System.currentTimeMillis() - stopwatch_startRound2) + " ms");


        long stopwatch_startRound3 = System.currentTimeMillis();
        //Round 3
        float radius = points.map(
                    (point) -> findRadius(point, kCenters.value())
                ).reduce(Math::max);
        System.out.println("Running time of MRFFT Round 3 = " + (System.currentTimeMillis() - stopwatch_startRound3) + " ms");

        return radius;
    }
}
