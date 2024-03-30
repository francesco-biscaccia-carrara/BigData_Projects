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

        //Task 3 point 1 - Prints the command-line arguments and stores D,M,K,L into suitable variables.
        String file_path = args[0];
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);

        System.out.println("File path: " + file_path + "D = " + D + " M = " + M + " K = " + K + " L = " + L);

        //Spark setup
        SparkConf conf = new SparkConf().setAppName("Outlier Detection").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            //Reduce verbosity -- does not work somehow
            sc.setLogLevel("WARN");

            //Task 3  point 2 - Reads the input points into an RDD of strings (called rawData) and transform it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
            JavaRDD<String> rawData = sc.textFile(file_path);
            JavaPairRDD<Float, Float> inputPoints;

            //Conversion of string to a pair of points and storing in a new RDD
            inputPoints = rawData.mapToPair(line -> {
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
                Methods.ExactOutliers(listOfPoints, D, M, K);
                long stopwatch_stop = System.currentTimeMillis();
                long exec_time = stopwatch_stop - stopwatch_start;
                //Prints ExactOutliers' running time. The stopwatch variable saves the current time when method starts and finishes
                System.out.println("Running time for ExactOutliers: " + exec_time + " millisec");
            }

            //Task 3 point 5 - In all cases:

            long stopwatch_startMR = System.currentTimeMillis();
            //Executes MRApproxOutliers with parameters inputPoints, D,M and K. The execution will print the information specified above.
            Methods.MRApproxOutliers(inputPoints, D, M, K);
            long stopwatch_stopMR = System.currentTimeMillis();
            long exec_time = stopwatch_stopMR - stopwatch_startMR;
            //Prints MRApproxOutliers' running time. Again the stopwatch variable saves the current time when method starts and finishes
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

    private static Tuple2<Integer, Integer> determineCell(Tuple2<Float, Float> point, float D) {
        float lambda = (float) (D/(2*Math.sqrt(2)));

        int i = (int) Math.floor(point._1/lambda);
        int j = (int) Math.floor(point._2/lambda);

        return new Tuple2<>(i, j);
    }

    public static void ExactOutliers(List<Tuple2<Float,Float>> points, float D, int M, int K){

        ArrayList<Tuple2<Integer,Integer>> outliersPoints = new ArrayList<>();

        for(int i=0;i<points.size();i++){
            int dNeighborCountI = 0;
            for (Tuple2<Float, Float> point : points) {
                //if(i==j) continue;
                if (eucDistance(points.get(i), point) <= D) dNeighborCountI++;
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

        final int[] xCentralCellMax = {Integer.MIN_VALUE};
        final int[] yCentralCellMax = {Integer.MIN_VALUE};

        /* step A
        - Map phase: (x,y) (coordinates of point) -> emit ( (i,j), 1 ) (key: identifier of cell)
        - Reduce phase: for each cell (i,j), L_ij = { values of pairs with key (i,j) } = {1,1,...} ->
            emit ( (i,j), |L_ij| ); |L_ij| = number of points in cell (i,j)
         */
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellCount = points
            .flatMapToPair(
                    (pair) -> {
                        ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> pointsPairs = new ArrayList<>();

                        Tuple2<Integer, Integer> cell = determineCell(pair, D);
                        if (cell._1>xCentralCellMax[0]) xCentralCellMax[0]=cell._1;
                        if (cell._2>yCentralCellMax[0]) yCentralCellMax[0]=cell._2;
                        pointsPairs.add(new Tuple2<>(cell, 1));

                        return pointsPairs.iterator();
                    }
            )
            .reduceByKey(Integer::sum);

        /* step B
        - Map phase: ( (i,j), |L_ij| ) -> emit ( ( (h,k), |L_hk| ), ( (i,j), |L_ij| ) ); (h,k) is the central cell
            of one of the 7x7 grids R_7(h,k) to which (i,j) belongs
        - Reduce phase: for every central cell (h,k), L_hk={ pairs ( (i,j), |L_ij| ) belonging to R_7(h,k) } -> emit
            ( ( (h,k), |L_hk| ), ( |N_3(i,k)|, |N_7(i,k)| ) )
         */
        Map<Tuple2<Integer, Integer>, Integer> cells = cellCount.collectAsMap();
        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Integer, Integer>> regionCounts = cellCount.
            flatMapToPair(
                (pair) -> {
                    ArrayList<Tuple2<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Tuple2<Integer, Integer>, Integer>>>
                        regionPairs = new ArrayList<>();

                    Tuple2<Integer, Integer> cell = pair._1;
                    // compute all 7x7 grid to which cell belongs
                    for (int i=-3; i<4; i++) {
                        for (int j=-3; j<4; j++) {
                            int xCentralCell = cell._1+i;
                            int yCentralCell = cell._2+j;
                            Tuple2<Integer, Integer> centralCell = new Tuple2<>(xCentralCell, yCentralCell);
                            // if the computed central cell is not present among cells's keys, skip it
                            if (!cells.containsKey(centralCell)) continue;
                            regionPairs.add(new Tuple2<>(new Tuple2<>(centralCell, cells.get(centralCell)), pair));
                        }
                    }

                    return regionPairs.iterator();
                }
            )
            .groupByKey()
            .flatMapToPair(
                (pair) -> {
                    ArrayList<Tuple2<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Integer, Integer>>> regionNumbers =
                            new ArrayList<>();

                    Tuple2<Integer, Integer> centralCell = pair._1._1;
                    int sum3 = 0;
                    int sum7 = 0;

                    for (Tuple2<Tuple2<Integer, Integer>, Integer> value : pair._2) {
                        sum7 += value._2;

                        /* if a cell and centralCell have a distance of {-1,0,1} on both components, then the cell also
                        belongs to the 3x3 grid with centralCell in the middle */
                        int xDistance = value._1._1 - centralCell._1;
                        int yDistance = value._1._2 - centralCell._2;
                        if (xDistance>=-1 && xDistance<=1 && yDistance>=-1 && yDistance<=1) sum3 += value._2;
                    }

                    regionNumbers.add(new Tuple2<>(pair._1, new Tuple2<>(sum3, sum7)));
                    return regionNumbers.iterator();
                }
            );

        Map<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Integer, Integer>> results = regionCounts.collectAsMap();
        int sureOutliers = 0, uncertains = 0;
        for (Tuple2<Tuple2<Integer, Integer>, Integer> cell : results.keySet()) {
            int pointsNumber = cell._2;
            Tuple2<Integer, Integer> sizes = results.get(cell);
            if (sizes._2 <= M) sureOutliers += pointsNumber;
            else if (sizes._1 <= M) uncertains += pointsNumber;
        }
        System.out.println("Number of sure outliers = "+ sureOutliers);
        System.out.println("Number of uncertain points = "+uncertains);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> tmp = cellCount.mapToPair(
                (pair) -> new Tuple2<>(pair._2, pair._1)
        );
        tmp = tmp.sortByKey(true);
        List<Tuple2<Integer, Tuple2<Integer, Integer>>> firstElements = tmp.take(K);
        for (Tuple2<Integer, Tuple2<Integer, Integer>> element : firstElements) {
            System.out.println("Cell: (" + element._2._1 + "," + element._2._2 + ")   Size = " + element._1);
        }

    }
}