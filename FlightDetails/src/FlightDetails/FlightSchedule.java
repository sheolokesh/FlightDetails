package FlightDetails;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FlightSchedule {

	public static class MapDemohadoop extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
	            if (key.get() == 0 && value.toString().contains("header") /*Some condition satisfying it is header*/)
	                return;
	            else {
			String line = value.toString();
			String[] elements = line.split(",");

			if (!elements[8].equals("UniqueCarrier")){
			Text tx = new Text(elements[8]);
			Text tx1 = new Text(elements[14]);
			Text tx2 = new Text(elements[15]);

			String i = "1";
				context.write(tx, new Text(i));
				if (tx1.toString().equals("0") && tx2.toString().equals("0"))
				{
				context.write(tx, new Text("onsched"+"\t"+i));
				}
	            }
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }	
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Double, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double count=0;
			double count1=0;
			double prob = 0;
			List<Text> elements=new ArrayList<Text>();
			for(Text value:values)
			{
				elements.add(new Text(value));
			}
			for(int i=0;i<elements.size();i++)
			{
				String[] elements1=elements.get(i).toString().split("\t");
				if(elements1.length==2)
				{
					count = count+Integer.parseInt(elements1[1]);
				}
				if(elements1.length==1){
					count1 = count1+Integer.parseInt(elements1[0]);
			
				}
			}
			
			prob = (double)(count/count1);
			prob=Math.round(prob*100)/100.00;
			context.write(prob, key);
		}
			
	}
	
	/*public static class SorttopMap extends Mapper<LongWritable, Text, DoubleWritable,Text> 
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
	}*/
	
			
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Insufficient args");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");
		Job job = new Job(conf, "Flight Schedule Probablity");
		job.setJarByClass(FlightSchedule.class); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Double.class); 
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapDemohadoop.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	
		/*Job job2 = new Job(conf, "Sorting");
		job2.setJarByClass(FlightSchedule.class); 
		job2.setMapOutputKeyClass(DoubleWritable.class); 
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(SorttopMap.class);
		job2.setReducerClass(SorttopReduce.class);
		job2.setNumReduceTasks(1);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path("/airprob"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.waitForCompletion(true);*/
	
	}

}
