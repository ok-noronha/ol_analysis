import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String mt = "", mj = "";
		int mv = -1;
		for (Text value : values) {
			String[] fields = value.toString().split("\t\t");
			String t = fields[0];
			String j = fields[1];
			int v = Integer.parseInt(fields[2]);
			if (v > mv) {
				mv = v;
				mt = t;
				mj = j;
			}
		}
		JSONObject json = null;
		try {
			json = new JSONObject(mj);
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String title = "uk";
		try {
			if (json == null)
				throw new JSONException("still null");
			title = json.getString("title");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			title = "Unknown";
		}
		context.write(key, new Text(mt + "\t" + title));
	}
}
