import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> topTenItemsMap = new TreeMap();

	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\s+");
		int ratings = Integer.parseInt(fields[1]);
		String work = fields[0];
		topTenItemsMap.put(ratings, new Text(work + "," + ratings));
		// If TreeMap has more than 10 entries, remove the lowest one
		if (topTenItemsMap.size() > 10) {
			topTenItemsMap.remove(topTenItemsMap.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Output top ten items in descending order of cost
		for (Text item : topTenItemsMap.descendingMap().values()) {
			context.write(NullWritable.get(), item);
		}
	}
}
