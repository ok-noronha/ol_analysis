import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NameiiMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] record = value.toString().split("\t");
            String bookID = record[0];
            String name = record[2];
            context.write(new Text(bookID), new Text("name:" + name));
        } catch (Exception e) {
            String[] record = value.toString().split("\t");
            String bookID = record[0];
            context.write(new Text(bookID), new Text("name:" + "Unknown"));
        }
    }
}
