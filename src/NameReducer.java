import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NameReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean tt = false;
        String rating = "0";
        String name = "data not present";
        try {
            for (Text v : values) {
                String[] fields = v.toString().split(":");
                if (fields[0].equals("rating")) {
                    tt = true;
                    rating = fields[1];
                }
                if (fields[0].equals("name")) {
                    name = fields[1];
                }
            }
        } catch (Exception e) {

        }
        if (tt) {
            context.write(new Text(key + "\t" + name), new Text(rating));
        }
    }
}
