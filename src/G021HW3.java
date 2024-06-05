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

        System.out.println("n=" + n + " phi=" + phi + " epsilon=" + epsilon + " delta="+ delta +" portExp="+ portExp);

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

        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < n){
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;

                        List<Long> batchItems = batch.map(Long::parseLong).collect();
                        MethodsHW3.trueFrequentItemsUpdate(batchItems,trueFrequentItemsMap);

                        if (batchSize > 0) System.out.println("Batch size at time [" + time + "] is: " + batchSize); //DEBUG ONLY

                        if (streamLength[0] >= n) stoppingSemaphore.release();
                    }
                });

        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");

        sc.stop(false, false);
        System.out.println("Streaming engine stopped");

        //HW output
        System.out.println("Number of items processed = " + streamLength[0]);
        MethodsHW3.printInfoTrueFreqItems(trueFrequentItemsMap,phi,streamLength[0]);
    }
}

class MethodsHW3 {
    public static void trueFrequentItemsUpdate(List<Long> streamItems,HashMap<Long,Long> trueFrequentItemsMap){
        for(Long item : streamItems) {
            if (!trueFrequentItemsMap.containsKey(item))
                trueFrequentItemsMap.put(item, 1L);
            else
                trueFrequentItemsMap.replace(item, trueFrequentItemsMap.get(item) + 1L);
        }
    }

    public static void printInfoTrueFreqItems(HashMap<Long,Long> trueFrequentItemsMap, float phi, long streamLentgh){
        System.out.println("TrueFrequentItemsMap size: "+trueFrequentItemsMap.size());

        ArrayList<Long> trueFreqItems = new ArrayList<>();
        for(Map.Entry<Long, Long> pair : trueFrequentItemsMap.entrySet())
            if(pair.getValue() >= phi*streamLentgh) trueFreqItems.add(pair.getKey());

        System.out.println("Number of True Frequent Items: " +trueFreqItems.size());

        trueFreqItems.sort(Long::compareTo);
        for(Long item: trueFreqItems) System.out.println(item);
    }
}
