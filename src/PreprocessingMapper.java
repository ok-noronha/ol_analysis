import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] fields = line.split("\t");
    
    // Filter out unwanted lines
    if (fields.length != 5) {
      return;
    }
    
    // Get book ID and publication year
    String bookID = fields[1].toString().substring(7);
    String pubTime = fields[3].toString();
    String jsons = fields[4].toString();
    String v = fields[2].toString();
    
    context.write(new Text(bookID), new Text(pubTime+"\t\t"+jsons+"\t\t"+v));
  }
}
