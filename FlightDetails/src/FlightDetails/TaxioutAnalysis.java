package FlightDetails;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TaxioutAnalysis extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Taxiin");
		job.setJarByClass(TaxioutAnalysis.class);

		// configure output and input source
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// configure mapper and reducer
		job.setMapperClass(TaxioutMap.class);
		job.setReducerClass(TaxioutReduce.class);

		// configure output
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TaxioutAnalysis(), args);
		System.exit(exitCode);
	}

}

class TaxioutMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineparts = value.toString().split(",");
		if (lineparts.length >= 10) {
			if (lineparts[20].equals("TaxiOut")) {
			} else {
				if (lineparts[20].equals("NA")) {
					// do nothing.
				} else {
							if (Integer.parseInt(lineparts[20]) != 1) {
								context.write(new Text(lineparts[18]), new Text(lineparts[20]));
							}
				}
			}
		}
	}
}

class TaxioutReduce extends Reducer<Text, Text, Text,Text>{
	public void reduce(Text key,Iterable<Text> Values, Context context) throws IOException,InterruptedException
	{
		Double sum=0.0;
		long count=0;
		if(key.equals(0))
		{
			System.err.println("InMap");
		}
		for(Text value:Values)
		{
			sum=sum+Integer.parseInt(value.toString());
			count++;
		}
		sum=sum/count;
		context.write(new Text(sum.toString()), key);
		}
	}

