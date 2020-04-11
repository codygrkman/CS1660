//package CS1660;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class InvertedIndexJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: Inverted Indices <input path> <output path>");
            System.exit(-1);
        }
        // Creating a Hadoop job and assigning a job name for identification
        Job job = new Job();
        job.setJarByClass(InvertedIndexJob.class);
        job.setJobName("Inverted Indices");
        // The HDFS input and output directories to be fetched from the Dataproc job submission console.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Providing the mapper and reducer class names.
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        // Setting the job object with the data types of output key(Text) and value(IntWritable)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}

class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text docID = new Text();
    private Text word = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Reading input one line at a time and tokenizing
        String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);
        // Determine what document we are in
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        docID.set(fileName);
        // Iterating through all of the words available in that line and forming the key value pair
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, docID);
        }
    }
}

class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> results = new HashMap<String, Integer>();
        // Iterate through the values available with a key and add them together to give final result as the key and sum of its values
        for (Text value : values) {
            int count = results.containsKey(value.toString()) ? results.get(value.toString()) : 0;
            results.put(value.toString(), count + 1);
        }
        // Write to context 
        for (HashMap.Entry<String,Integer> entry : results.entrySet()) {
            context.write(key, new Text(entry.getKey() + "--" + entry.getValue()));
        }
    }
}
