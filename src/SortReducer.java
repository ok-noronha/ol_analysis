import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	TreeMap<Integer, Text> topTenItemsMap = new TreeMap();
    	for (Text item : values) {
            String[] fields = item.toString().split(",");
            int ratings = Integer.parseInt(fields[1]);
            String work = fields[0];
            
            topTenItemsMap.put(ratings, new Text(work+","+fields[1]));

            // If TreeMap has more than 10 entries, remove the lowest one
            if (topTenItemsMap.size() > 10) {
              topTenItemsMap.remove(topTenItemsMap.firstKey());
            }
         }
         for (Text  item : topTenItemsMap.descendingMap().values()) {
        	 String[] fields = item.toString().split(",");
             int ratings = Integer.parseInt(fields[1]);
             String work = fields[0];
             context.write(new Text(work),new IntWritable(ratings));
          }   	        	
        }
}
