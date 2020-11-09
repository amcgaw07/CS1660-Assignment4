import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
  
public class Driver { 
  
    public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, 
                                  args).getRemainingArgs(); 
  
        // if less than two paths  
        // provided will show error 
        if (otherArgs.length < 2)  
        { 
            System.err.println("Error: please provide four paths"); 
            System.exit(2); 
        } 
  
        Job job = Job.getInstance(conf, "top 10"); 
        job.setJarByClass(Driver.class); 
  
        job.setMapperClass(least_5_Mapper.class); 
        job.setReducerClass(least_5_Mapper.class); 
        job.setNumReduceTasks(1);
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
  
        //FileInputFormat.addInputPath(job, new Path(otherArgs[0],otherArgs[1],otherArgs[2])); 
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]),TextInputFormat.class, least_5_Mapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, least_5_Mapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, least_5_Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 