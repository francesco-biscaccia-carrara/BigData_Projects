
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rawData = sc.textFile(file_path);

        JavaPairRDD<Float, Float> inputPoints;

        inputPoints = rawData.mapToPair(line -> {
            String[] coordinates = line.split(",");
            float x_coord = Float.parseFloat(coordinates[0]);
            float y_coord = Float.parseFloat(coordinates[1]);

            return new Tuple2<>(x_coord,y_coord);
        });

        inputPoints.repartition(L).cache();

        long num_points = inputPoints.count();
        System.out.println("Number of input points in the document = " + num_points);

        if (num_points <= 200000){
            List<Tuple2<Float, Float>> listOfPoints = inputPoints.collect();

            long stopwatch_start = System.currentTimeMillis();
            ExactOutliers(listOfPoints, D, M, K);
            long stopwatch_stop = System.currentTimeMillis();
            long exec_time = stopwatch_stop-stopwatch_start;

            System.out.println("Running time for ExactOutliers: " + exec_time " millisec");
        }

        long stopwatch_start = System.currentTimeMillis();
        MRApproxOutliers(listOfPoints, D, M, K);
        long stopwatch_stop = System.currentTimeMillis();
        long exec_time = stopwatch_stop-stopwatch_start;

        System.out.println("Running time for MRApproxOutliers: " + exec_time " millisec");

        sc.stop();
    }
    class Methods{

        //useless imo, Tuple2 can handle it
        public class Point{

            public float x_coord;
            public float y_coord;

            public Point(float x_coord,float y_coord){
                this.x_coord = x_coord;
                this.y_coord = y_coord;
            }
            @Override
            public String toString() {
                return "("+this.x_coord + "," + this.y_coord+")";
            }
        }
        private float EucDistance(Point point_a, Point point_b){
            float x_diff = point_a.x_coord - point_b.x_coord;
            float y_diff = point_a.y_coord - point_b.y_coord;

            return (float) Math.sqrt(Math.pow(x_diff,2)-Math.pow(y_diff,2));
        }
        public void ExactOutliers(ArrayList<Point> points, float D, int M, int K){

            int[] Outliers= new int[points.size()];

            for(int i=0;i<points.size();i++){
                for(int j=0;j<points.size();j++){
                    if(i==j) continue;
                    if(EucDistance(points.get(i), points.get(j))<D) Outliers[i]+=1;
                }
            }

            TreeMap<Integer,Integer> OutliersPoints = new TreeMap<>();

            for(int i=0;i<points.size();i++) {
                if (Outliers[i] < M) {
                    OutliersPoints.put(Outliers[i], i);
                }
            }
            System.out.println("Number of ("+D+","+M+")-outliers: "+OutliersPoints.size());

            for(int i = 0; i< (Math.min(OutliersPoints.size(), K)); i++){
                   int l = OutliersPoints.firstKey();
                   System.out.println(points.get(OutliersPoints.get(l)));
                   OutliersPoints.remove(l);
            }
        }
    }
}
