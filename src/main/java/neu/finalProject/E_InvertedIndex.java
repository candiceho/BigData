package neu.finalProject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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


public class E_InvertedIndex { 
    public static class myMapper extends Mapper<LongWritable, Text, Text, Text>{
    	List<String> stop = new ArrayList<String>();
	    @Override   	   
	    public void setup(Context context) throws IOException{ 
	    	Configuration conf = context.getConfiguration();
    	    String filePath = conf.get("StopPath");//   /input/stopWord.txt
    	    Path path = new Path(filePath); 
    	    //Path path = new Path("hdfs:" + filePath);   // load file to local from HDFS   hdfs:/input/stopwords.txt
    	    FileSystem fs = FileSystem.get(conf);
    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    	    String line;
    	    line = br.readLine();
    	    while (line !=null){
    		    stop.add(line.toLowerCase().trim());
    		    line = br.readLine();
    	    }
	    }
    	   
	    @Override                    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {      
    	    String[] movie_info = value.toString().trim().split(",");
    	    String movieId = movie_info[0];
    	    Text id = new Text(movieId);
    	   
    	    String names = movie_info[2];
    	    StringTokenizer st = new StringTokenizer(names);
    	    while (st.hasMoreTokens()){
    		    String cur = st.nextToken().toLowerCase().trim();
    		    cur = cur.replaceAll("[^a-zA-Z]", "");
    		    if (!stop.contains(cur) && cur.trim() != null && cur.trim() != ""){
    			    context.write(new Text(cur), id);
    		    }
    	    }	    
	    }
    }    
       
    public static class myReducer extends Reducer<Text,Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values,
			 Context context) throws IOException, InterruptedException {
   	    	 // key : keyword    values : {doc1, doc1, doc1, doc2, doc2}
   	    	 // __>  key: keyword   value : doc1\doc2
   	    	 String last = null;
   	    	 StringBuilder movieList = new StringBuilder();

   	    	 for (Text value: values){
   	    		 if (last != null && value.toString().trim().equals(last)){
   	    			 //count++;
   	    			 continue;
   	    		 }
   	    		 if (last == null){
   	    			 // count ++;
   	    			 last = value.toString().trim();
   	    			 continue;
   	    		 }
   	    		   
   	    		 movieList.append(last); // if last != null && count > threshold
   	    		 movieList.append("\t");  	    		   
   	    		 //count = 1;
   	    		 last = value.toString().trim();  
   	    	 }  
   	    	   
   	    	 //if (threshold < count){
   	    		 movieList.append(last);
   	    	 //}
             if (!movieList.toString().trim().equals("") && ! key.toString().equals("")){
            	 context.write(key, new Text (movieList.toString()));
             }       
   		}// reduce
    } // Reducer

   	public static void main(String[] args) throws Exception {
   		if (args.length < 3){
   			throw new Exception("Usage: <input dir> <output dir> <stopwords dir>");
   		}
   		Configuration conf = new Configuration();
   		BasicConfigurator.configure();
   		conf.set("StopPath", args[0]);
   		Job job = Job.getInstance(conf);
   		
   		Path input = new Path(args[1]);
 		Path output = new Path(args[2]);
        
 		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output))
			fs.delete(output, true);
		
   		job.setJarByClass(E_InvertedIndex.class);
   		job.setMapperClass(myMapper.class);
   		job.setReducerClass(myReducer.class);
   		//job.setNumReduceTasks(3); // will generate 3 output files
   		
   		job.setInputFormatClass(TextInputFormat.class);		
   		job.setOutputFormatClass(TextOutputFormat.class);

   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(Text.class);
   		
   		FileInputFormat.addInputPath(job, input); 
   		FileOutputFormat.setOutputPath(job, output); // path 是一个文件夹的path 
   		
   		System.exit(job.waitForCompletion(true)? 0 : 1);
   	}
}
