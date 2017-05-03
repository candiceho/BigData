package neu.finalProject;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class G_TopK {
	static class MyMap extends Mapper<LongWritable, Text, NullWritable, Text> {  
		public static final int K = 100;  
        private TreeMap<Double, Text> averages = new TreeMap<Double, Text>();  
  
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
        	// movid   moviName : average
        	String line = value.toString();
        	String[] tokens = line.split("@");
        	double average = Double.parseDouble(tokens[2].trim());
            averages.put(average, new Text(value));  
            if (averages.size() > K)
                averages.remove(averages.firstKey());  
        }  
  
        @Override  
        protected void cleanup(Context context) throws IOException, InterruptedException {  
            for (Text value : averages.values()) {  
                context.write(NullWritable.get(), value);  
            }  
        } 
    }
  
	
    static class MyReduce extends Reducer<NullWritable, Text, NullWritable,Text> {  
        public static final int K = 100;  
        private TreeMap<Double, Text> averages = new TreeMap<Double, Text>(); 
        
        @Override  
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            for (Text value : values) {  
	        	String[] tokens = value.toString().split("@");
	            double average = Double.parseDouble(tokens[2].trim());
	            averages.put(average, new Text(value));  
	            if (averages.size() > K){
	            	averages.remove(averages.firstKey());
	            } 
            }  

	        int i = 0;
	        for (Text t : averages.descendingMap().values()) {
	        	i++;
		        // Output our ten records to the file system with a null key
		        String line = t.toString();
		        String [] tokens = line.split("@");
		        context.write(NullWritable.get(), new Text(i +". " + tokens[0] + " " + tokens[1] + " @ " + tokens[2]));
	        }
        }        
    } // reducer
	
	public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration(); 
        BasicConfigurator.configure();
        Job job = new Job(conf, "top100");
        
        Path input = new Path(args[0]);
 		Path output = new Path(args[1]);
        
 		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output))
			fs.delete(output, true);
		
        job.setJarByClass(G_TopK.class);
 		job.setMapperClass(MyMap.class);
 		job.setReducerClass(MyReduce.class);
        //job.setNumReduceTasks(3);  
        job.setOutputKeyClass(NullWritable.class);  
        job.setOutputValueClass(Text.class);
        
 		job.setInputFormatClass(TextInputFormat.class);		
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
    	FileInputFormat.addInputPath(job, input); 
 		FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
