package neu.finalProject;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.BasicConfigurator;

public class A_preProcess { 
    public static class myMapperA extends Mapper<LongWritable, Text, IntWritable, Text>{
		 
	    @Override                    
	    public void map(LongWritable key, Text value, Context contex) throws IOException, InterruptedException {
		    String[] movie_user_rating = value.toString().trim().split(",");
		    int userId = Integer.parseInt(movie_user_rating[1]);
		    String movieId = movie_user_rating[0];
		    String rating = movie_user_rating[2];
			contex.write(new IntWritable(userId), new Text(movieId + ":" + rating));
  	    }
    }
    
    public static class myReducerA extends Reducer<IntWritable,Text, IntWritable, Text> {
    	// ->  user1    movie1 : rating,   movie2 : rating
    	@Override
 	    public void reduce(IntWritable key, Iterable<Text> values,
 		    Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()){
            	sb.append(","+ values.iterator().next().toString());
            }
            context.write(key, new Text (sb.toString().replaceFirst(",", "")));      
 		}
    }

 	public static void main(String[] args) throws Exception {
 	    Configuration conf = new Configuration(); 
 		BasicConfigurator.configure();
 		Job job = Job.getInstance(conf);
 		
 		Path input = new Path(args[0]);
 		Path output = new Path(args[1]);
        
 		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output))
			fs.delete(output, true);
 		job.setJarByClass(A_preProcess.class);
 		job.setMapperClass(myMapperA.class);
 		job.setReducerClass(myReducerA.class);
 		
 		job.setInputFormatClass(TextInputFormat.class);		
 		job.setOutputFormatClass(TextOutputFormat.class);

 		job.setOutputKeyClass(IntWritable.class);
 		job.setOutputValueClass(Text.class);
 		
 		FileInputFormat.addInputPath(job, input); 
 		FileOutputFormat.setOutputPath(job, output);
 		
 		job.waitForCompletion(true);
 	}
}
