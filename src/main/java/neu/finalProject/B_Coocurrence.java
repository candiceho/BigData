package neu.finalProject;

import java.io.IOException;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;//!!!!!!!!!!
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;//!!!!!!!!
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class B_Coocurrence {
    public static class myMapperB extends Mapper<LongWritable, Text, Text, IntWritable>{
    	@Override                    // input key-value pair  output key-value pair
	    public void map(LongWritable key, Text value, Context contex) throws IOException, InterruptedException {
    		String line = value.toString();  		   
    	    String[] user_movie_rating = line.split("\t");
	    	//String user = user_movie_rating[0];
	    	String movieRating = user_movie_rating[1];
	    	String [] movie_rating = movieRating.split(",");
	    	for (int i = 0; i < movie_rating.length; i++){
	    		String movie1 = movie_rating[i].trim().split(":")[0];
	    		for (int j = 0; j < movie_rating.length; j++){
	    		    String movie2 = movie_rating[j].trim().split(":")[0];
	    			contex.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
	    		}
	    	}
	    }
    }
    
    public static class myReducerB extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
   	    public void reduce(Text key, Iterable<IntWritable> values,
   	    		Context context) throws IOException, InterruptedException {
            int sum = 0;
            while (values.iterator().hasNext()){
            	sum += values.iterator().next().get();
            }
            context.write(key, new IntWritable(sum));
		}
  	}

   	public static void main(String[] args) throws Exception {
   		Configuration conf = new Configuration();
   		BasicConfigurator.configure();
   		
   		Path input = new Path(args[0]);
 		Path output = new Path(args[1]);
 		
 		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output))
			fs.delete(output, true);
		
   		Job job = Job.getInstance(conf);
   		job.setJarByClass(B_Coocurrence.class);
   		job.setMapperClass(myMapperB.class);
   		job.setReducerClass(myReducerB.class);
   		
   		// job.setNumReduceTasks(3);
   		
   		job.setInputFormatClass(TextInputFormat.class);		
   		job.setOutputFormatClass(TextOutputFormat.class);
   		
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(IntWritable.class);
   		
   		//TextInputFormat.setInputPaths(job, new Path(args[0]));
   		//TextOutputFormat.setOutputPath(job, new Path(args[1])); 
   		
   		FileInputFormat.addInputPath(job, input);
   		FileOutputFormat.setOutputPath(job, output); 
   		job.waitForCompletion(true);
   	}
}

