package weather;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Que3 {



	// Map Class
	public static class MapperClass extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

			String s = value.toString();
			String date = s.substring(6, 14).replaceAll(" ", "");
			String tempHigh = s.substring(38, 45).replaceAll(" ", "");
			String tempLow = s.substring(46, 53).replaceAll(" ", "");

			double hightemp = Double.parseDouble(tempHigh);
			double lowtemp = Double.parseDouble(tempLow);

			if (hightemp != -9999.0) {
				con.write(new DoubleWritable(hightemp), new Text(date));
			}
			if (lowtemp != -9999.0) {
				con.write(new DoubleWritable(lowtemp), new Text(date));
			}

			

		}
	}

	// Reducer Class
	public static class ReducerClass extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		LinkedHashMap<DoubleWritable, Text> lowHashMap = new LinkedHashMap<DoubleWritable, Text>();
		LinkedHashMap<DoubleWritable, Text> highHashMap = new LinkedHashMap<DoubleWritable, Text>();
		int i = 0;

		public void reduce(DoubleWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			double value = key.get();
			String date = values.iterator().next().toString();

			if (i < 10) {
				lowHashMap.put(new DoubleWritable(value), new Text(date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8)));
				++i;
			}

			highHashMap.put(new DoubleWritable(value), new Text(date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8)));
			if (highHashMap.size() > 10) {
				highHashMap.remove(highHashMap.keySet().iterator().next());
			}

		}

		public void cleanup(Context con) throws IOException, InterruptedException {

			con.write(null, new Text("Top 10 Coldest Days:"));
			for (Map.Entry<DoubleWritable, Text> m : lowHashMap.entrySet()) {
				con.write(m.getKey(), m.getValue());
			}

			con.write(null, new Text("Top 10 Hottest Days:"));
			List<DoubleWritable> highKeys = new ArrayList<DoubleWritable>(
					highHashMap.keySet());
			Collections.reverse(highKeys);

			for (int i = 0; i < highKeys.size(); i++) {
				con.write(highKeys.get(i), highHashMap.get(highKeys.get(i)));
			}
		}

	}
	
	public static void main(String[] args) throws IOException,	ClassNotFoundException, InterruptedException {
Configuration cf = new Configuration();

Job j = new Job(cf, "Question 3");
j.setJarByClass(Que3.class);
j.setMapperClass(MapperClass.class);
j.setReducerClass(ReducerClass.class);
j.setOutputKeyClass(DoubleWritable.class);
j.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(j, new Path(args[0]));
FileOutputFormat.setOutputPath(j, new Path(args[1]));
System.exit(j.waitForCompletion(true) ? 0 : 1);

}

}