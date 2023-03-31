import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadingTrendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable outputValue = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    // Summing up the counts for each year and category
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }

    // Emitting the year and category as the output key, and the count as the output
    // value
    outputValue.set(count);
    context.write(key, outputValue);
  }
}
