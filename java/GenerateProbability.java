import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

public class GenerateProbability {

    public static class ReducerClass extends Reducer<Text, PairTextLongWritable, Text, FloatWritable> {
        private static Long N = null;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // N = Long.parseLong(context.getConfiguration().get("N", "2803960"));
            FileSystem fileSystem = FileSystem.get(URI.create("s3://dsp-ass-2/data"), context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(("s3://dsp-ass-2/data/N.txt")));
            String input = IOUtils.toString(fsDataInputStream, "UTF-8");
            fsDataInputStream.close();
            N = Long.valueOf(input);
        }


        @Override
        public void reduce(Text key, Iterable<PairTextLongWritable> values, Context context) throws IOException, InterruptedException {
            Long Nr0 = null;
            Long Nr1 = null;
            Long Tr01 = null;
            Long Tr10 = null;

            for (PairTextLongWritable value : values) {
                String set = value.getLeft().toString();
                switch (set) {
                    case ("Nr0"): {
                        Nr0 = value.getRight().get();
                        break;
                    }
                    case ("Nr1"): {
                        Nr1 = value.getRight().get();
                        break;
                    }
                    case ("Tr01"): {
                        Tr01 = value.getRight().get();
                        break;
                    }
                    case ("Tr10"): {
                        Tr10 = value.getRight().get();
                        break;
                    }
                }
            }
            if (Nr0 == null || Nr1 == null || Tr01 == null || Tr10 == null) {
                throw new IOException("Null values for 1 or more of the sets! Check your code or input");
            }
            float probability = (Tr01.floatValue() + Tr10.floatValue()) / (N.floatValue() * (Nr0.floatValue() + Nr1.floatValue()));
            context.write(key, new FloatWritable(probability));
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[1];
        String outputPath = args[2];
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Probability");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairTextLongWritable.class);
        job.setJarByClass(GenerateProbability.class);
        job.setReducerClass(GenerateProbability.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
