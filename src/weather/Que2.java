package weather;


import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weather.Que2.MapForWordCount;

public class Que2 {
  
  
  public static class MapForWordCount extends Mapper<LongWritable, Text, Text, FloatWritable> {
    
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
      String line = value.toString();
      String[] words = line.split("\\s+");
        
        String s=words[1];

        Float mint=Float.parseFloat(words[6]);
        Float maxt=Float.parseFloat(words[5]);
        FloatWritable minValue = new FloatWritable(mint);	
        FloatWritable maxValue = new FloatWritable(maxt);
        if (mint<10) {
        	con.write(new Text("Cold Day: "+s.substring(0,4)+"-"+s.substring(4,6)+"-"+s.substring(6,8)), minValue);
		}
        if (maxt>35) {
        	con.write(new Text("Hot Day: "+s.substring(0,4)+"-"+s.substring(4,6)+"-"+s.substring(6,8)), maxValue);
		}
        
      }
    }
    
  
  
  
  public static class ReduceForWordCount extends Reducer<Text, FloatWritable, Text, Text> {
    
    public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException {
//      float sum = 0.0f;
      for (FloatWritable value: values) {
////        sum = sum+value.get();
//        con.write(word, new FloatWritable(sum));
//      }
    	con.write(word, new Text(value +" \u2103"));
      }
    }
    
  }
  
  
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration c = new Configuration();
    Job j = Job.getInstance(c, "Question 2");
    j.setJarByClass(Que2.class);
    j.setMapperClass(MapForWordCount.class);
    j.setReducerClass(ReduceForWordCount.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(j, new Path(args[0]));
    FileOutputFormat.setOutputPath(j, new Path(args[1]));
    System.exit(j.waitForCompletion(true)?0:1);
  }
  

}