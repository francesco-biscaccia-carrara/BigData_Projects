import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class G021HW3 {

    public static void main(String[] args) throws Exception{

        if (args.length != 5) throw new IllegalArgumentException("USAGE: n phi epsilon delta portExp");

        int n = Integer.parseInt(args[0]);
        float phi = Float.parseFloat(args[1]);
        float epsilon = Float.parseFloat(args[2]);
        float delta = Float.parseFloat(args[3]);
        int portExp = Integer.parseInt(args[4]);

        System.out.println("INPUT PROPERTIES");
        System.out.println("n = " + n + " phi = " + phi + " epsilon = " + epsilon + " delta = "+ delta +" port = "+ portExp);

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]")
                .setAppName("Frequent Items")
                .set("spark.log.level", "OFF");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        long[] streamLength = new long[1];

        //Data structure necessary to the stream
        HashMap<Long,Long> trueFrequentItemsMap = new HashMap<>();
        ArrayList<Long> reservoirSampleList = new ArrayList<>();
        HashMap<Long,Long> stickySamplingMap = new HashMap<>();

        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < n){
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;

                        List<Long> batchItems = batch.map(Long::parseLong).collect();
                        MethodsHW3.trueFrequentItemsUpdate(batchItems,trueFrequentItemsMap);
                        MethodsHW3.reservoirSampleUpdate(batchItems,reservoirSampleList,streamLength[0] - batchSize, (int) Math.ceil(1/phi));
                        MethodsHW3.stickySamplingUpdate(batchItems,stickySamplingMap,(float) Math.log(1/(delta*phi))/epsilon,n);

                        if (streamLength[0] >= n) stoppingSemaphore.release();
                    }
                });

        sc.start();
        stoppingSemaphore.acquire();

        sc.stop(false, false);

        HashMap<Long,Long> trueFrequentItems = MethodsHW3.printInfoTrueFreqItems(trueFrequentItemsMap,phi,n);
        MethodsHW3.printInfoReservoirSample(reservoirSampleList,trueFrequentItems);
        MethodsHW3.printInfoStickySampling(stickySamplingMap,trueFrequentItems,phi,epsilon,n);

    }

}

class MethodsHW3 {

    public static void trueFrequentItemsUpdate(List<Long> streamItems, HashMap<Long,Long> trueFrequentItemsMap){

        for (Long item : streamItems) {
            if (!trueFrequentItemsMap.containsKey(item)) trueFrequentItemsMap.put(item, 1L);
            else trueFrequentItemsMap.replace(item, trueFrequentItemsMap.get(item) + 1L);
        }

    }

    public static void reservoirSampleUpdate(List<Long> streamItems, ArrayList<Long> reservoirSample, long startIndex, int m) {

        long t = startIndex;
        for (Long item : streamItems) {
            if (reservoirSample.size() < m) reservoirSample.add(item);
            else
                if (Math.random() <= (double) m/t) reservoirSample.set((int) (Math.random() * reservoirSample.size()),item);
            t++;
        }

    }

    public static void stickySamplingUpdate(List<Long> streamItems, HashMap<Long,Long> stickySamplingMap, float r, int n) {

        for (Long item:streamItems) {
            if (stickySamplingMap.containsKey(item)) stickySamplingMap.replace(item,stickySamplingMap.get(item)+1L);
            else
                if (Math.random() <= (double) r/n) stickySamplingMap.put(item,1L);
        }

    }

    public static HashMap<Long,Long> printInfoTrueFreqItems(HashMap<Long,Long> trueFrequentItemsMap, float phi, int n){

        System.out.println("EXACT ALGORITHM");
        System.out.println("Number of items in the data structure = "+trueFrequentItemsMap.keySet().size());

        HashMap<Long,Long>  trueFreqItems = new HashMap<>();
        for(Map.Entry<Long, Long> pair : trueFrequentItemsMap.entrySet())
            if(pair.getValue() >= phi*n) trueFreqItems.put(pair.getKey(),pair.getValue());
        System.out.println("Number of true frequent items = " + trueFreqItems.keySet().size());

        ArrayList<Long> sortedItems = new ArrayList<>(trueFreqItems.keySet());
        sortedItems.sort(Long::compareTo);
        System.out.println("True frequent items:");
        for(Long item:sortedItems) System.out.println(item);

        return trueFreqItems;

    }

    public static void printInfoReservoirSample(ArrayList<Long> reservoirSample, HashMap<Long,Long> trueFreqItems) {

        System.out.println("RESERVOIR SAMPLING");
        System.out.println("Size m of the sample = "+reservoirSample.size());

        HashMap<Long,Integer> reservoirSampleMap = new HashMap<>();
        for (Long item: reservoirSample) reservoirSampleMap.put(item,1);
        System.out.println("Number of estimated frequent items = " +reservoirSampleMap.keySet().size());

        ArrayList<Long> sortedItems = new ArrayList<>(reservoirSampleMap.keySet());
        sortedItems.sort(Long::compareTo);
        System.out.println("Estimated frequent items:");
        for (Long item:sortedItems) System.out.println(item + (trueFreqItems.containsKey(item) ? " +" : " -"));

    }

    public static void printInfoStickySampling(HashMap<Long,Long> stickySamplingMap, HashMap<Long,Long> trueFreqItems, float phi, float epsilon, int n){

        System.out.println("STICKY SAMPLING");
        System.out.println("Number of items in the Hash Table = "+stickySamplingMap.keySet().size());

        ArrayList<Long> stickySamplingItems = new ArrayList<>();
        for (Map.Entry<Long, Long> pair : stickySamplingMap.entrySet())
            if (pair.getValue() >= (long) ((phi - epsilon)*n)) stickySamplingItems.add(pair.getKey());
        System.out.println("Number of estimated frequent items = " +stickySamplingItems.size());

        stickySamplingItems.sort(Long::compareTo);
        System.out.println("Estimated frequent items:");
        for (Long item: stickySamplingItems) System.out.println(item + (trueFreqItems.containsKey(item) ? " +" : " -"));

    }

}