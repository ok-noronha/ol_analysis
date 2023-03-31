import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<NullWritable, Text, LongWritable, DoubleWritable> {

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Initialize variables
		double sum_x = 0.0;
		double sum_y = 0.0;
		double sum_xy = 0.0;
		double sum_xx = 0.0;
		double n = 0.0;

		// Calculate the linear regression coefficients
		for (Text value : values) {
			String[] fields = value.toString().split("\t");
			double x = Double.parseDouble(fields[0]);
			double y = Double.parseDouble(fields[1]);
			sum_x += x;
			sum_y += y;
			sum_xy += x * y;
			sum_xx += x * x;
			n += 1.0;
		}
		double mean_x = sum_x / n;
		double mean_y = sum_y / n;
		double slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
		double intercept = (sum_y - slope * sum_x) / n;

		// Calculate the predicted sales value for the given year
		long queryYear = Long.parseLong(context.getConfiguration().get("queryYear"));
		double predictedSales = intercept + slope * queryYear;

		// Emit the predicted sales value
		context.write(new LongWritable(queryYear), new DoubleWritable(predictedSales));
	}
}
