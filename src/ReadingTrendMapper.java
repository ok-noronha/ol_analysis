import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadingTrendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text outputKey = new Text();
  private IntWritable outputValue = new IntWritable(1);

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] recordFields = value.toString().split("[\\t]");
    String datePublished = "";
    try {
      datePublished = recordFields[1];
    } catch (Exception e) {

    }
    String year = "";
    if (!datePublished.isEmpty()) {
      year = datePublished.substring(0, 4);
    }
    if (!year.isEmpty()) {
      outputKey.set(year);
      outputValue.set(1);
      context.write(outputKey, outputValue);
    }
  }
}
