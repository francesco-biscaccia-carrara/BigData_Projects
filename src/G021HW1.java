import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class G021HW1 {
    public static void main(String[] args) {
        //Commandline check
        if(args.length != 5){
            throw new IllegalArgumentException("USAGE: filepath D M K L");
        }

        //Task 3 point 1 - Prints the command-line arguments and stores D,M,K,L into suitable variables.
        String file_path = args[0];
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);

        System.out.println(file_path + " D=" +D + " M=" + M + " K=" + K + " L=" + L);

        //Spark setup

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true).setAppName("Outlier Detection").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            //Reduce verbosity -- does not work somehow
            sc.setLogLevel("WARN");

            //Task 3  point 2 - Reads the input points into an RDD of strings (called rawData) and transform it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
            JavaRDD<String> rawData = sc.textFile(file_path);

            //Conversion of string to a pair of points and storing in a new RDD
            JavaRDD<Tuple2<Float, Float>> inputPoints = rawData.map(line -> {
                String[] coordinates = line.split(",");
                float x_coord = Float.parseFloat(coordinates[0]);
                float y_coord = Float.parseFloat(coordinates[1]);

                return new Tuple2<>(x_coord, y_coord);
            });

            inputPoints.repartition(L).cache();

            //Task 3 point 3 - Prints the total number of points.
            long num_points = inputPoints.count();
            System.out.println("Number of points = " + num_points);

            //Task 3 point 4 - Only if the number of points is at most 200000:
            if (num_points <= 200000) {
                //Downloads the points into a list called listOfPoints
                List<Tuple2<Float, Float>> listOfPoints = inputPoints.collect();

                long stopwatch_start = System.currentTimeMillis();
                //Executes ExactOutliers with parameters listOfPoints,  D,M and K. The execution will print the information specified above.
                MethodsHW1.ExactOutliers(listOfPoints, D, M, K);
                long stopwatch_stop = System.currentTimeMillis();
                long exec_time = stopwatch_stop - stopwatch_start;
                //Prints ExactOutliers' running time. The stopwatch variable saves the current time when method starts and finishes
                System.out.println("Running time of ExactOutliers: " + exec_time + " ms");
            }

            //Task 3 point 5 - In all cases:

            long stopwatch_startMR = System.currentTimeMillis();
            //Executes MRApproxOutliers with parameters inputPoints, D,M and K. The execution will print the information specified above.
            MethodsHW1.MRApproxOutliers(inputPoints, D, M, K);
            long stopwatch_stopMR = System.currentTimeMillis();
            long exec_time = stopwatch_stopMR - stopwatch_startMR;
            //Prints MRApproxOutliers' running time. Again the stopwatch variable saves the current time when method starts and finishes
            System.out.println("Running time of MRApproxOutliers: " + exec_time + " ms");

            sc.stop();
        }
    }
}

class MethodsHW1 {
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

    public static void ExactOutliers(List<Tuple2<Float,Float>> points, float D, int M, int K){

        ArrayList<Tuple2<Integer,Long>> outliersPoints = new ArrayList<>();

        for(int i=0;i<points.size();i++){
            long dNeighborCountI = 0L;
            for (Tuple2<Float, Float> point : points)
                if (eucDistance(points.get(i), point) <= D) dNeighborCountI++;

            if(dNeighborCountI <= M)  outliersPoints.add(new Tuple2<>(i,dNeighborCountI));
        }
        
        //Sorting the list of outliers points by dNeighborCount(|B(p,D)|)
        outliersPoints.sort((e1, e2) -> e1._2().compareTo(e2._2));

        System.out.println("Number of outliers = "+outliersPoints.size());
        for(int i = 0; i< (Math.min(outliersPoints.size(), K)); i++){
            //Printing the first min(size of outliersPoints list,K) outliers points
            System.out.println("Point: "+points.get(outliersPoints.get(i)._1));
        }
    }

    public static void MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> points, float D, int M, int K) {
        /*  Step A
            - Map phase: (x,y) (coordinates of point) -> emit ( (i,j), 1 ) (key: identifier of cell)
            - Reduce phase: for each cell (i,j), L_ij = { values of pairs with key (i,j) } = {1,1,...} ->
            emit ( (i,j), |L_ij| ); |L_ij| = number of points in cell (i,j)
         */
        JavaPairRDD<Tuple2<Integer, Integer>, Long> cellCount = points.mapToPair(
                (pair) -> new Tuple2<>(determineCell(pair, D), 1L)
        ).reduceByKey(Long::sum);

        /*  Step B
            Collect the RDD as a Map and use a sequential algorithm to process the N3 and N7 values of each
            cell.
         */
        Map<Tuple2<Integer, Integer>, Long> tmpMap = cellCount.collectAsMap();
        HashMap<Tuple2<Integer, Integer>, Tuple3<Long, Long, Long>> pairSizeN3N7 = new HashMap<>();

        for(Map.Entry<Tuple2<Integer, Integer>, Long> e : tmpMap.entrySet()){
            Tuple2<Tuple2<Integer,Integer>,Long> pair =  new Tuple2<>(e.getKey(),e.getValue());

            pairSizeN3N7.put(pair._1, new Tuple3<>(pair._2, 0L, 0L));
            //Notice: this for count itself too
            for (int i = -3; i < 4; i++) {
                for (int j = -3; j < 4; j++) {
                    if (tmpMap.get(new Tuple2<>(pair._1._1 + i, pair._1._2 + j)) != null) {
                        long cellIJCount = tmpMap.get(new Tuple2<>(pair._1._1 + i, pair._1._2 + j));
                        if ((i < -1 || i > 1) || (j < -1 || j > 1))
                            //In region C7
                            pairSizeN3N7.put(pair._1, new Tuple3<>(pair._2, pairSizeN3N7.get(pair._1)._2(), pairSizeN3N7.get(pair._1)._3() + cellIJCount));
                        else
                            //In region C3
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

        //Map phase: ((i,j),|size of cell ij|) -> emit (|size of cell ij|, (i,j))
        JavaPairRDD<Long, Tuple2<Integer, Integer>> sortedCell = cellCount.mapToPair(
                (pair) -> new Tuple2<>(pair._2, new Tuple2<>(pair._1._1(), pair._1._2()))
        );

        for (Tuple2<Long, Tuple2<Integer, Integer>> e : sortedCell.sortByKey().take(K)) {
            System.out.println("Cell: " + e._2 + " Size=" + e._1);
        }
    }
}
