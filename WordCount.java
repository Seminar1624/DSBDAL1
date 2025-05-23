/*
steps taken :-
step 1: cd hadoop/sbin
step 2: start-dfs.sh
step 3: start-yarn.sh
step 4:jps
step 5: hdfs dfs -mkdir /YourRollno
step 6: hdfs dfs -mkdir /YourRollno/Input
step 7: hdfs dfs -put 'textfilepath.txt' /YourRollno/Input
step 8: hadoop jar 'pathofjarfile' wordcount /YourRollno/Input /YourRollno/Output
step 9: hdfs dfs -cat /YourRollno/Output/*
step 10: stop-dfs.sh
step 11: stop-yarn.sh 

 if nodes not started - hdfs --daemon start namenode
  ResourceManager
 yarn --daemon start resourcemanager
 5. NodeManager
 yarn --daemon start nodemanager
 
 hdfs dfs -mkdir -p /user/te/input 
hdfs dfs -put input/* /user/te/input/ 
rm -rf build wordcount.jar   //if already exist 
mkdir build 
javac -classpath $(hadoop classpath) -d build WordCount.java 
jar -cvf wordcount.jar -C build/ 
hadoop jar wordcount.jar WordCount /user/te/input /user/te/output 
hdfs dfs -cat /user/te/output/part-r-00000
hadoop fs -get /user/te/weather_output ./weather_output_local
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                word.set(filename + "\t" + token);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}