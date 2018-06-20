package FlightDetails;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.util.*;
import java.io.*;

public class aircancel {

	public static void main(String[] args) throws Exception {

   Configuration conf = new Configuration();
   Job  job=new Job(conf, "AirCancelCode");
   job.setJarByClass(aircancel.class);
   job.setNumReduceTasks(1);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
   job.setMapperClass(CancelCodeMapper.class);
   job.setReducerClass(CancelCodeReducer.class);
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextOutputFormat.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   job.waitForCompletion(true);
}   //  MAIN


public static class CancelCodeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	   public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		   String line = value.toString();
			String[] elements = line.split(",");
			if (!elements[22].equals("CancellationCode")){
			Text tx = new Text(elements[22]);
	   //   cancelCode=line[22];
	      context.write(tx, new IntWritable(1));
	   }  // MAP
	   }
	}


	public static class CancelCodeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable sv = new IntWritable();
	   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		   int sum = 0;
		    for (IntWritable val : values) {
		      sum += val.get();
		    }
		    sv.set(sum);
					context.write(key, sv);
	   }
	}  // AirlineCancelCode


}
