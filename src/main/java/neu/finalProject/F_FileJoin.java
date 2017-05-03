package neu.finalProject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class F_FileJoin{
	public static class myMapperF extends Mapper<LongWritable, Text, IntWritable, Text> {
		private Map<Integer, String> movieName = new HashMap<Integer, String>();

		@Override
		protected void setup(Context context) throws IOException{
			Configuration conf= context.getConfiguration(); 
 		    String filePath = conf.get("movie_titles"); // driver .set()  title data
 		    Path path = new Path(filePath);
 		    FileSystem fs = FileSystem.get(conf); // hdfs:/
 		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
 		    // FSDataInputStream fsInput = fs.open(new Path(filePath)); // 
 		    //String line = fsInput.readLine(); // 
 		    String line = br.readLine();
 		    // movieid, year, movietitle 
 		    while(line != null){
 			    int movie_id = Integer.parseInt(line.trim().split(",")[0]);
 			    movieName.put(movie_id, line.trim().split(",")[2]);
 			    //line = br.readLine();
 			    line = br.readLine();
 		    }
 		    //br.close();
 		    br.close();
	    }

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			//int usrid = Integer.parseInt(tokens[1].trim());
			int movieid = Integer.parseInt(tokens[0].trim()); 
            String rating = tokens[2].trim();			
			if (movieName.containsKey(movieid)) {
				//oKey.set(tokens[0].trim()); // movieid
				//oValue.set(movieName.get(movieid) + ":" + rating); // moviename :rating
				context.write(new IntWritable(movieid), new Text(movieName.get(movieid) + "@" + rating));
			}
		}
	}

	
	public static class myReducerF extends Reducer<IntWritable, Text, IntWritable, Text> {
		
        @Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
        	long sum = 0;
			int count = 0;
			String moviename ="";
			for (Text value : values) {
				String cur = value.toString();
                moviename = cur.split("@")[0].trim();
                double rate = Double.parseDouble(cur.split("@")[1].trim());
                sum += rate;
                count++;
			}
			double average = (double) sum / count;
  		    DecimalFormat df = new DecimalFormat("#.00");
  		    average = Double.valueOf(df.format(average));
		    context.write(key, new Text("@" + moviename + "@" + average));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		BasicConfigurator.configure();
 		conf.set("movie_titles", args[0]); //   
 		Job job = Job.getInstance(conf); // 
 		 		
 		Path input = new Path(args[1]);
 		Path output = new Path(args[2]);
        
 		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output))
			fs.delete(output, true);
		
 		job.setJarByClass(F_FileJoin.class);
 		job.setMapperClass(myMapperF.class);
 		job.setReducerClass(myReducerF.class);
 		
 		job.setInputFormatClass(TextInputFormat.class);		
 		job.setOutputFormatClass(TextOutputFormat.class);
 		
 		job.setMapOutputKeyClass(IntWritable.class);
 		job.setMapOutputValueClass(Text.class);
 		
 		job.setOutputKeyClass(IntWritable.class);
 		job.setOutputValueClass(Text.class);
 		
 		//TextInputFormat.setInputPaths(job, new Path(args[1])); // 最初的data
 		//TextOutputFormat.setOutputPath(job, new Path(args[2])); 
 		
 		FileInputFormat.addInputPath(job, input);
 		FileOutputFormat.setOutputPath(job, output); 
 		job.waitForCompletion(true);
	}
}