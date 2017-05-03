package neu.finalProject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class D_RecomandDetail {
	 public static class myMapperD extends Mapper<LongWritable, Text, IntWritable, Text>{
		// filter watched movies
		 Map<Integer, List<Integer>> watchedHistory = new HashMap<Integer, List<Integer>>();
		 
   		 @Override
   		 public void setup(Context context) throws IOException{
   			 //read movie watched history
   			 Configuration conf= context.getConfiguration(); 
   			 String filePath = conf.get("watchedHistory"); 
   			 Path path = new Path(filePath);
   			 FileSystem fs = FileSystem.get(conf);
   			 BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
   			 String line = br.readLine();
  		  
   			 // movie user rating   
   			 while (line != null){
   				 // movie1 : movie2  \t relation
   				 int userid =Integer.parseInt(line.split(",")[1]);
   				 int  movieid= Integer.parseInt(line.split(",")[0]);
   				 if (watchedHistory.containsKey(userid)){
   					 watchedHistory.get(userid).add(movieid);
   				 }
   				 else {
   					 List<Integer> l = new ArrayList<Integer>();
   					 l.add(movieid);
   					 watchedHistory.put(userid, l);
   				 }
   				 line = br.readLine();
   			 }
   			 br.close();
  	    }
   		 
  	    @Override                    
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  		    // filtering   
  		    // user    movie : rating
  		    String[] tokens = value.toString().trim().split("\t");
	  	    int user = Integer.parseInt(tokens[0]);
	  	    int movie = Integer.parseInt((tokens[1].split(":")[0]).trim());
	  	    if (!watchedHistory.get(user).contains(movie)){
	  		    context.write(new IntWritable(user), new Text(tokens[1]));
	  	    }
  	    }
    }
	 
	// match movie_name and movie id
    public static class myReducerD extends Reducer<IntWritable, Text, IntWritable, Text> {	 
	    Map<Integer, String> movieTitle = new HashMap<Integer, String>();
    	@Override 
    	protected void setup(Context context) throws IOException{
    	    Configuration conf= context.getConfiguration(); 
		    String filePath = conf.get("movie_titles"); // driver .set()  title data
		    Path path = new Path(filePath);
		    FileSystem fs = FileSystem.get(conf);
		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		    String line = br.readLine();
		    // movieid, year, movietitle 
		    while(line != null){
		        int movie_id = Integer.parseInt(line.trim().split(",")[0]);
			    movieTitle.put( movie_id, line.trim().split(",")[2]);
			    line = br.readLine();
		    }
		    br.close();
        }        
    	 
    	@Override
 	    public void reduce(IntWritable key, Iterable<Text> values,
 		    Context context) throws IOException, InterruptedException {
    		// key -> user   value -> movie : rating
            while (values.iterator().hasNext()){
	          	String cur = values.iterator().next().toString();
	            int movieid = Integer.parseInt(cur.split(":")[0]);
	            String rating = cur.split(":")[1];
	            context.write(key, new Text(movieTitle.get(movieid) + ":" + rating));
	            // userid      movie name : rating 
            }            
 		}
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
 		conf.set("watchedHistory", args[0]);
 		conf.set("movie_titles", args[1]); 
 		Job job = Job.getInstance(conf);
 		
 		job.setJarByClass(D_RecomandDetail.class);
 		job.setMapperClass(myMapperD.class);
 		job.setReducerClass(myReducerD.class);
 		
 		job.setInputFormatClass(TextInputFormat.class);		
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		//job.setMapOutputKeyClass(IntWritable.class);
 		//job.setMapOutputValueClass(Text.class);
 		
 		job.setOutputKeyClass(IntWritable.class);
 		job.setOutputValueClass(Text.class);
 		
 		//TextInputFormat.setInputPaths(job, new Path(args[2])); // C's result 
 		//TextOutputFormat.setOutputPath(job, new Path(args[3])); 
 		
 		FileInputFormat.addInputPath(job, new Path(args[2]));
 		FileOutputFormat.setOutputPath(job, new Path(args[3])); 
 		job.waitForCompletion(true);
 	}
}
