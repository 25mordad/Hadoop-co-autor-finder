import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Exam {


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
           
            Text outputKey = new Text();
            Text outputValue = new Text();
            	
			outputKey.set(indicesAndValue[0]);
			outputValue.set(indicesAndValue[0] + "," + indicesAndValue[1]);
			context.write(outputKey, outputValue);
			System.console().writer().println("-key "+ outputKey  + " -value " + outputValue);
			
			outputKey.set(indicesAndValue[1] );
			outputValue.set(indicesAndValue[0] + "," + indicesAndValue[1]);
			context.write(outputKey, outputValue);
			System.console().writer().println("-key "+ outputKey  + " -value " + outputValue);
			
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            System.console().writer().println(" -********************* " + values);
            String result = "";
            for (Text val : values) {
                value = val.toString().split(",");
                result += Arrays.toString(value);
                System.console().writer().println(" -value " + Arrays.toString(value));
                
            }
             System.console().writer().println(" -key " + key);
             
             context.write(null, new Text(key.toString() + " ****** " + result));
            
        }
    }
 
    public static void main(String[] args) throws Exception {
	System.console().writer().println("Start+++++++++++++++++++++++++++++++++++");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Exam");
        job.setJarByClass(Exam.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}
