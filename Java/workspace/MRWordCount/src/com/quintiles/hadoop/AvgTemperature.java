package com.quintiles.hadoop;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
 
public class AvgTemperature { 
 
    
      public static void main(String[] args) throws Exception { 
    	  StopWatch timer = new StopWatch();
  		timer.start();
        JobConf conf = new JobConf(AvgTemperature.class); 
        conf.setJobName("AvgTemp"); 
    
        conf.setOutputKeyClass(Text.class); 
        conf.setOutputValueClass(IntWritable.class); 
    
        conf.setMapperClass(AvgTemperatureMapper.class); 
       conf.setReducerClass(AvgTemperatureReducer.class); 
            conf.setInputFormat(TextInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class); 
    
        FileInputFormat.setInputPaths(conf, new Path(args[0])); 
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
    
        JobClient.runJob(conf); 
        timer.stop();
		System.out.println("Elapsed:"+timer.toString());
      } 
}
