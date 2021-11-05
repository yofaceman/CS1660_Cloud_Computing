import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class MaxTemperatureMapper
		extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context)
		 throws IOException, InterruptedException {

		String line = value.toString();
		String year = line.substring(15,23);
		int airTemperature;
		if(line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88,92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87,92));
		}
		String quality = line.substring(92,93);
		if(airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
}
