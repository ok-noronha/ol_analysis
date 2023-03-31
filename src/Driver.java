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
    if(fs.exists(new Path("/pp"))){
       fs.delete(new Path("/pp"),true);
       }
    if(fs.exists(new Path("/book_rat"))){
        fs.delete(new Path("/book_rat"),true);
        }
    if(fs.exists(new Path("/year_count"))){
        fs.delete(new Path("/year_count"),true);
        }
    if(fs.exists(new Path("/top_ten"))){
        fs.delete(new Path("/top_ten"),true);
        }
    if(fs.exists(new Path("/tt"))){
        fs.delete(new Path("/tt"),true);
        }
    if(fs.exists(new Path("/pred"))){
        fs.delete(new Path("/pred"),true);
        }
    if(fs.exists(new Path("/chart.png"))){
        fs.delete(new Path("/chart.png"),true);
        }
    if(fs.exists(new Path("/tt1"))){
        fs.delete(new Path("/tt1"),true);
        }
    
    
    Job job = new Job ();
    job.setJarByClass(Driver.class);
    //job.setMapperClass(PreprocessingMapper.class);
    job.setReducerClass(PreprocessingReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //FileInputFormat.addInputPath(job, new Path("/ol_works1"));
    //FileInputFormat.addInputPath(job, new Path("/ol_works2"));
    //FileInputFormat.addInputPath(job, new Path("/ol_works3"));
    //FileInputFormat.addInputPath(job, new Path("/ol_works_tt"));
    MultipleInputs.addInputPath(job,new Path("/ol_works_tt"),TextInputFormat.class,PreprocessingMapper.class);
    MultipleInputs.addInputPath(job,new Path("/ol_works1"),TextInputFormat.class,PreprocessingMapper.class);
    FileOutputFormat.setOutputPath(job, new Path("/pp"));
    job.waitForCompletion(true);    
    Scanner sc = new Scanner(System.in);    
    System.out.println("Enter the job number to run:");
    System.out.println("1: Reading Trends");
    System.out.println("2: Top Rated Books");
    //System.out.println("3: Sum rating count by author and year");
    int jobNumber = sc.nextInt();

    job = null;

    switch (jobNumber) {
        case 1:
            job = new Job();
            job.setJarByClass(Driver.class);
            job.setMapperClass(ReadingTrendMapper.class);
            job.setReducerClass(ReadingTrendReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("/pp/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/year_count"));
            job.waitForCompletion(true);
            job = new Job();            
            System.out.println("\n\nEnter Year to Predict Published Books :");
            job.getConfiguration().set("queryYear", new Integer(sc.nextInt()).toString());
            job.setJarByClass(Driver.class);
            job.setMapperClass(SalesMapper.class);
            job.setReducerClass(SalesReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/year_count/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/pred"));
            job.waitForCompletion(true);
            LineGraph.run();
            break;
        case 2:
        	job = new Job();
            job.setJarByClass(Driver.class);
            job.setMapperClass(RatingsMapper.class);
            job.setReducerClass(RatingsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("/ol_ratings"));
            FileOutputFormat.setOutputPath(job, new Path("/book_rat"));
            job.waitForCompletion(true);
            job = new Job();
            job.setJarByClass(Driver.class);
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("/book_rat/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/tt"));
            job.waitForCompletion(true);
            job = new Job();
            job.setJarByClass(Driver.class);
            //job.setMapperClass(NameiMapper.class);
            job.setReducerClass(NameReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            //job.getConfiguration().
            MultipleInputs.addInputPath(job,new Path("/tt/part-r-00000"),TextInputFormat.class,NameiMapper.class);
            MultipleInputs.addInputPath(job,new Path("/pp/part-r-00000"),TextInputFormat.class,NameiiMapper.class);
            //FileInputFormat.addInputPath(job, new Path("/tt/part-r-00000"));
            //FileInputFormat.addInputPath(job, new Path("/pp/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/tt1"));
            job.waitForCompletion(true);
            job = new Job();
            job.setJarByClass(Driver.class);
            job.setMapperClass(SortiMapper.class);
            job.setReducerClass(SortiReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("/tt1/part-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path("/top_ten"));
            job.waitForCompletion(true);
            break;        
        default:
            System.out.println("Invalid job number entered.");
            System.exit(1);
    }  
    //FileOutputFormat.setOutputPath(job, new Path(op));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
