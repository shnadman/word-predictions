import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SortResults {


    public static class MapperClass extends Mapper<Text, FloatWritable, PairTextFloatWritable, Text> {

        @Override
        public void map(Text key, FloatWritable value, Context context) throws IOException, InterruptedException {
            String[] trigram = key.toString().split(" ");
            if (trigram.length > 2) {
                context.write(new PairTextFloatWritable(new Text(trigram[0] + " " + trigram[1]), value), new Text(trigram[2]));
            }
        }
    }

    public static class ReducerClass extends Reducer<PairTextFloatWritable, Text, Text, FloatWritable> {

        @Override
        public void reduce(PairTextFloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text w3 : values) {
                context.write(new Text(key.getLeft().toString() + " " + w3.toString()), key.getRight());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[1];
        String outputPath = args[2];
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sort");

        job.setJarByClass(SortResults.class);
        job.setMapperClass(SortResults.MapperClass.class);
        job.setReducerClass(SortResults.ReducerClass.class);
        job.setMapOutputKeyClass(PairTextFloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
