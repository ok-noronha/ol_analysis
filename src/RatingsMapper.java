import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static IntWritable outputVal = new IntWritable(0);
    private Text outputKey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split("\t");
        String work = "";
        int rating = 0;

        // Extracting the year and category fields
        try {
        	work = recordFields[0].substring(7);
        }
        catch (Exception e) {
        	work = "";
        }
        try {
            rating = Integer.parseInt(recordFields[2]);
        }
        catch (Exception e){    	
            rating = -1;
        }
        if (work != "" && rating != -1){
        	outputKey = new Text(work);
        	outputVal = new IntWritable(rating);     
        	context.write(outputKey, outputVal);
        }
    }
}
