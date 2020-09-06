package weather;


import java.io.IOException;

import org.apache.commons.math3.analysis.function.Min;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weather.Que4.MapForWordCount;

public class Que4 {
  
  
  public static class MapForWordCount extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
      String line = value.toString();
      String[] words = line.split("\\s+");
//      for (String word: words) {
//        Text outputKey = new Text(word);
//        IntWritable outputValue = new IntWritable(1);
        
        String s=words[1];
        Text outputKey=new Text(s.substring(0,4)+"-"+s.substring(4,6)+"-"+s.substring(6,8));
        Float mint=Float.parseFloat(words[6]);
        Float maxt=Float.parseFloat(words[5]);
    	Text outputValue = new Text("Min Temp: "+mint+" \u2103 \t Max Temp: "+maxt+" \u2103 ");
        con.write(outputKey, outputValue);
      }
    }
    
  
  
  
  public static class ReduceForWordCount extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException {
//      float sum = 0.0f;
      for (Text value: values) {
//        sum = sum+value.get();
        con.write(word, value);
      }
    }
    
  }
  
  
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration c = new Configuration();
    Job j = Job.getInstance(c, "Question 4");
    j.setJarByClass(Que4.class);
    j.setMapperClass(MapForWordCount.class);
    j.setReducerClass(ReduceForWordCount.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(j, new Path(args[0]));
    FileOutputFormat.setOutputPath(j, new Path(args[1]));
    System.exit(j.waitForCompletion(true)?0:1);
  }
  

}