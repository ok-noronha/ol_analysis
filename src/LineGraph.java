import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class LineGraph {

    public static void run() throws Exception {

        // Set up HDFS configuration and client
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inFile = new Path("/year_count/part-r-00000");

        // Read file from HDFS into list of data points
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
        String line;
        List<Double> xData = new ArrayList<Double>();
        List<Double> yData = new ArrayList<Double>();
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            xData.add(Double.parseDouble(parts[0]));
            yData.add(Double.parseDouble(parts[1]));
        }
        br.close();

        // Convert data points into XYSeriesCollection
        XYSeries series = new XYSeries("" +
                "publications per year");
        for (int i = 0; i < xData.size(); i++) {
            series.add(xData.get(i), yData.get(i));
        }
        XYSeriesCollection dataset = new XYSeriesCollection(series);

        // Create line chart and display in a window
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Line Chart", "Year", "Publications", dataset, PlotOrientation.VERTICAL, true, true, false);
        ChartFrame chartFrame = new ChartFrame("Line Chart", chart);
        chartFrame.pack();
        chartFrame.setVisible(true);
        Path outFile = new Path("/chart.png");
        FSDataOutputStream out = fs.create(outFile);
        try {
            ChartUtilities.writeChartAsPNG(out, chart, 800, 600);
        } catch (Exception e) {

        }
        out.close();
        fs.close();
    }
}
