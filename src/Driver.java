import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path("/tt1"))) {
            fs.delete(new Path("/tt1"), true);
        }

        if (fs.exists(new Path("/pp"))) {
            fs.delete(new Path("/pp"), true);
        }
        Job job = new Job();
        job.setJobName("Preprocessing");
        job.setJarByClass(Driver.class);
        job.setReducerClass(PreprocessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/ol_works_tt"), TextInputFormat.class, PreprocessingMapper.class);
        MultipleInputs.addInputPath(job, new Path("/ol_works1"), TextInputFormat.class, PreprocessingMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("/pp"));
        System.out.println("\nPreprocessing Job Started\n================================\n");
        job.waitForCompletion(true);

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter the job number to run:");
        System.out.println("1: Reading Trends");
        System.out.println("2: Top Rated Books");
        int jobNumber = sc.nextInt();

        job = null;
        switch (jobNumber) {
            case 1:
                if (fs.exists(new Path("/year_count"))) {
                    fs.delete(new Path("/year_count"), true);
                }
                job = new Job();
                job.setJarByClass(Driver.class);
                job.setJobName("Find Year Count");
                job.setMapperClass(ReadingTrendMapper.class);
                job.setReducerClass(ReadingTrendReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.addInputPath(job, new Path("/pp/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/year_count"));
                System.out.println("\nFinding Year Count\n================================\n");
                job.waitForCompletion(true);

                if (fs.exists(new Path("/pred"))) {
                    fs.delete(new Path("/pred"), true);
                }
                job = new Job();
                System.out.println("\n\nEnter Year to Predict Published Books :");
                job.getConfiguration().set("queryYear", new Integer(sc.nextInt()).toString());
                job.setJobName("Predict Published Books");
                job.setJarByClass(Driver.class);
                job.setMapperClass(SalesMapper.class);
                job.setReducerClass(SalesReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("/year_count/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/pred"));
                System.out.println("\nPredicting Published Books\n================================\n");
                job.waitForCompletion(true);
                if (fs.exists(new Path("/chart.png"))) {
                    fs.delete(new Path("/chart.png"), true);
                }
                LineGraph.run();
                break;
            case 2:
                if (fs.exists(new Path("/book_rat"))) {
                    fs.delete(new Path("/book_rat"), true);
                }
                job = new Job();
                job.setJarByClass(Driver.class);
                job.setJobName("Find Book Ratings");
                job.setMapperClass(RatingsMapper.class);
                job.setReducerClass(RatingsReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.addInputPath(job, new Path("/ol_ratings"));
                FileOutputFormat.setOutputPath(job, new Path("/book_rat"));
                System.out.println("\nFinding Book Ratings\n================================\n");
                job.waitForCompletion(true);
                if (fs.exists(new Path("/tt"))) {
                    fs.delete(new Path("/tt"), true);
                }
                job = new Job();
                job.setJobName("Ratings Sort");
                job.setJarByClass(Driver.class);
                job.setMapperClass(SortMapper.class);
                job.setReducerClass(SortReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.addInputPath(job, new Path("/book_rat/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/tt"));
                System.out.println("\nSorting Book Ratings\n================================\n");
                job.waitForCompletion(true);
                if (fs.exists(new Path("/tt1"))) {
                    fs.delete(new Path("/tt1"), true);
                }
                job = new Job();
                job.setJobName("Map Book Names");
                job.setJarByClass(Driver.class);
                job.setReducerClass(NameReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                MultipleInputs.addInputPath(job, new Path("/tt/part-r-00000"), TextInputFormat.class,
                        NameiMapper.class);
                MultipleInputs.addInputPath(job, new Path("/pp/part-r-00000"), TextInputFormat.class,
                        NameiiMapper.class);
                FileOutputFormat.setOutputPath(job, new Path("/tt1"));
                System.out.println("\nMapping Book Names\n================================\n");
                job.waitForCompletion(true);
                if (fs.exists(new Path("/top_ten"))) {
                    fs.delete(new Path("/top_ten"), true);
                }
                job = new Job();
                job.setJobName("Top Ten");
                job.setJarByClass(Driver.class);
                job.setMapperClass(SortiMapper.class);
                job.setReducerClass(SortiReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.addInputPath(job, new Path("/tt1/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/top_ten"));
                System.out.println("\nTop Ten Books\n================================\n");
                job.waitForCompletion(true);
                break;
            default:
                System.out.println("Invalid job number entered.");
                System.exit(1);
        }
    }
}
