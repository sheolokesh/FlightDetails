package FlightDetails;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class sorttop {
	public static class SorttopMap extends Mapper<LongWritable, Text, DoubleWritable,Text> 
	{ 
		public void map(LongWritable Key,Text Value, Context context) throws IOException, InterruptedException 
		{
		String[] elements=Value.toString().split("\t");
	    Double rf=Double.parseDouble(elements[0].toString());
		context.write(new DoubleWritable(rf) ,new Text(elements[1]));
	}
}
	
	public static class SorttopReduce extends Reducer<Text,Text,DoubleWritable,Text>
	{
		
		public void reduce(DoubleWritable Key,Iterable<Text> Values, Context context) throws IOException, InterruptedException 
		{
           // int counter = 0;
            for(Text value:Values) {
              //  if (counter == 50) {
              //      break;
              //  }
                context.write(Key, value);
            //   counter++;
            }	
			}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Sort");
		job.setJarByClass(sorttop.class); 
		job.setMapOutputKeyClass(DoubleWritable.class); 
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SorttopMap.class);
		job.setReducerClass(SorttopReduce.class);
		job.setNumReduceTasks(1);
		//job.setSortComparatorClass(DecreasingComparator.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		
	}
}
