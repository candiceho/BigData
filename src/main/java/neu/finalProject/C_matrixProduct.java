package neu.finalProject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


public class C_matrixProduct { 
	public static class myMapperC extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		Map<Integer, List<MovieRelation>> relations = new HashMap<Integer, List<MovieRelation>>();
    	Map<Integer, Integer> normalization = new HashMap<Integer,Integer>();
    	   
	    @Override
	    public void setup(Context context) throws IOException{
	    	Configuration conf= context.getConfiguration(); 
		    String filePath = conf.get("coOccurrencePath"); // driver .set()    B's output
		    Path path = new Path(filePath);
		    FileSystem fs = FileSystem.get(conf);
		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		    String line = br.readLine();
		    while (line != null){
		    	// movie1 : movie2  \t relation
			    String[] tokens = line.toString().trim().split("\t");
			    String [] movies = tokens[0].split(":");   
			    int movie1 =Integer.parseInt(movies[0]);
			    int movie2 = Integer.parseInt(movies[1]);
			    int relation = Integer.parseInt(tokens[1]);  
			    MovieRelation mr = new MovieRelation(movie1, movie2, relation);
			    if (relations.containsKey(movie1)){
			    	relations.get(movie1).add(mr);
			    }
			    else {
				    List<MovieRelation> l = new ArrayList<MovieRelation>();
				    l.add(mr);
				    relations.put(movie1, l);
			    }
			    line = br.readLine();
		    }
		    br.close();
    		   
		    // set up normalization
		    for (Map.Entry<Integer, List<MovieRelation>> entry : relations.entrySet()){
			    int sum = 0;
			    for (MovieRelation mr :entry.getValue()){
				    sum += mr.getRelation();
			    }
			    normalization.put(entry.getKey(), sum);
		    }   
	    }
	    
	    @Override                    // input key-value pair  output key-value pair
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    // user- movie-rating
		    // user:movie score      
    	    String[] tokens = value.toString().trim().split(",");
    	    int user = Integer.parseInt(tokens[1]);
    	    int movie = Integer.parseInt(tokens[0]);
    	    double rating = Double.parseDouble(tokens[2]);
            
    	    for (MovieRelation  movieR : relations.get(movie)){
    	    	double score = rating *  movieR.getRelation();
    		    // normalize
    		    score = score / normalization.get(movieR.getMovieB());// normalization
    		    DecimalFormat df = new DecimalFormat("#.00");
    		    score = Double.valueOf(df.format(score));
    		    // user - movie:score
    		    // context.write(new IntWritable(user), new Text(movieR.getMovieB() + ":" + score));
    		    // __> user:movie - score
    		    context.write(new Text(user + ":" + movieR.getMovieB()), new DoubleWritable(score));
    	    }
	    }
    }
       
	
	public static class myReducerC extends Reducer<Text, DoubleWritable, IntWritable, Text> {	   
		// input    user：movie   rating
	    // output   user     movie: rating sum
	    @Override
   	    public void reduce(Text key, Iterable<DoubleWritable> values,
   	    		Context context) throws IOException, InterruptedException {
            double sum = 0; 
            while (values.iterator().hasNext()){
            	sum += values.iterator().next().get();
            }
            String[] tokens = key.toString().split(":");
            int user = Integer.parseInt(tokens[0]);
            context.write(new IntWritable(user), new Text(tokens[1] + ":" + sum));
		}
  	}
       
   	public static void main(String[] args) throws Exception {
   		Configuration conf = new Configuration();
   		BasicConfigurator.configure();
   		conf.set("coOccurrencePath ", args[0]);
   		Job job = Job.getInstance(conf);
   		
   		Path input = new Path(args[1]);
 		Path output = new Path(args[2]);
 		
 		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output))
			fs.delete(output, true);
		
   		job.setJarByClass(C_matrixProduct.class);
   		job.setMapperClass(myMapperC.class);
   		job.setReducerClass(myReducerC.class);
   		
   		job.setInputFormatClass(TextInputFormat.class);		
   		job.setOutputFormatClass(TextOutputFormat.class);
   		
   		job.setMapOutputKeyClass(Text.class);
   		job.setMapOutputValueClass(DoubleWritable.class);
   		
   		job.setOutputKeyClass(IntWritable.class);
   		job.setOutputValueClass(Text.class);
   		
   		//TextInputFormat.setInputPaths(job, new Path(args[1])); // raw
   		//TextOutputFormat.setOutputPath(job, new Path(args[2])); 
   		
   		FileInputFormat.addInputPath(job, input);
   		FileOutputFormat.setOutputPath(job, output); 
   		job.waitForCompletion(true);
   	}
}


