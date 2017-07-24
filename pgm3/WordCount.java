import java.io.IOException;
import java.util.StringTokenizer;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text fileName = new Text();
        String filename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filename = fileSplit.getPath().getName();

        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int count = 0;
            int length = 0;
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                word.set(str);
                int currentIndex = Integer.parseInt(key.toString()) + length;
                fileName.set(filename + "-" + currentIndex);
                context.write(word, fileName);
                length += str.length() + 1;
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap map = new HashMap();
            HashMap occurrenceMap = new HashMap();


            for (Text val : values) {
                String value[] = val.toString().split("-");
                String fname = value[0];
                String position = value[1];
                if (map != null && map.get(fname) != null) {

                    String otherPositions = map.get(fname).toString();
                    map.put(fname, position + "," + otherPositions);
                    int occurrences = (int) occurrenceMap.get(fname);
                    occurrenceMap.put(fname, ++occurrences);

                } else {
                    map.put(fname, position);
                    occurrenceMap.put(fname, 1);

                }

            }
            String finalValue = "{";
            Set<String> set = map.keySet();
            int count =0;
            for (String name : set) {
                finalValue += "" + name + " occurrences: " + occurrenceMap.get(name) + " positions: " + map.get(name) ;
                count++;
                if(count<set.size()){
                    finalValue += " , ";
                }
            }
            finalValue += "}";
            result.set(finalValue);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
