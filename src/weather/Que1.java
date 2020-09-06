package weather;


import java.io.IOException;

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

import weather.Que1.MapForWordCount;

public class Que1 {
  

  
  public static class MapForWordCount extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
		{
			String line= value.toString();
			String[] words=line.split("\\s+");
	        String s=words[1];
	        Text outputKey=new Text(s.substring(0,4));
	        Float t=Float.parseFloat(words[7]);
	    	FloatWritable outputValue = new FloatWritable(t);
	        con.write(outputKey, outputValue);
	      }
	    }
	
	//reducer class
	public static class ReduceForWordCount extends Reducer<Text,FloatWritable,Text,Text>
	{
		public void reduce(Text word,Iterable<FloatWritable> values,Context con)throws IOException, InterruptedException
		{
			float maxtem=0;
	      for (FloatWritable value: values) {

	    	  if(value.get()>maxtem){
	    		  maxtem=value.get();
	    	  }
        
      }
	      con.write(word, new Text(maxtem+"\u2103"));
    }
    
  }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration c= new Configuration();
		Job j=Job.getInstance(c,"Question 1");
		j.setJarByClass(Que1.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j,new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
  

}