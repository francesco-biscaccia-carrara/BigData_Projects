
import java.util.*;

public class G021HW1 {
    public static void main(String[] args) {


    }
    class Methods{

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
