import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class MRCleaning extends Configured implements Tool{
	public static void main(String[] args) 
		throws Exception { 

    	Configuration conf = new Configuration();
  		int res = ToolRunner.run(conf, new MRCleaning(), args);
  		System.exit(res);
  	}

  	public final int run(final String[] args) throws Exception{
  		Job job = new Job(super.getConf()); 
		job.setNumReduceTasks(1);
		job.setJarByClass(MRCleaning.class); 
		job.setJobName("MRCleaning");
		FileInputFormat.addInputPath(job, new Path(args[1])); 
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
    		job.setMapperClass(CleaningMapper.class);
    		job.setReducerClass(CleaningReducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		job.waitForCompletion(true);
		return 0;
  	}

}
