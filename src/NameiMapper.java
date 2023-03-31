import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NameiMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t");
        String bookID = record[0];
        String rating = record[1];
        // context.write(new Text(bookID), new Text("name:" + "data not present"));
        context.write(new Text(bookID), new Text("rating:" + rating));
    }
}
