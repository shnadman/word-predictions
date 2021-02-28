import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class SplitDataAndCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, PairLongsWritable> {
        private final static Text A = new Text("A");
        private final static Text B = new Text("B");
        private Text trigram = new Text("");
        private String[] splittedRecord;
        private String trigramWords;


        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            splittedRecord = line.toString().split("\t");
            trigramWords = splittedRecord[0];
            if (trigramWords.split(" ").length < 3) {
                return; //Dunno if its neccesary
            }

            trigram.set(trigramWords);
            Long occurrences = Long.parseLong(splittedRecord[2]);

            if (Math.random() < 0.5) {
                context.write(trigram, new PairLongsWritable(0l, occurrences));
            } else {
                context.write(trigram, new PairLongsWritable(occurrences, 0l));
            }


        }
    }


    public static class ReducerClass extends Reducer<Text, PairLongsWritable, Text, PairLongsWritable> {

        @Override
        public void reduce(Text key, Iterable<PairLongsWritable> values, Context context) throws IOException, InterruptedException {
            long totalA = 0;
            long totalB = 0;

            for (PairLongsWritable value : values) {
                totalA += value.getLeft().get();
                totalB += value.getRight().get();
            }
            context.write(key, new PairLongsWritable(totalA, totalB));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "SplitDataCount");

        job.setJarByClass(SplitDataAndCount.class);
        job.setMapperClass(MapperClass.class);
        //job.setPartitionerClass(PartitionerClass.class);
        if (outputPath.contains("agg")) {
            job.setCombinerClass(ReducerClass.class);
        }
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairLongsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairLongsWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        boolean completionStatus = job.waitForCompletion(true);
        long N = job.getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
        writeN(conf, "s3://dsp-ass-2/data", String.valueOf(N));
        System.exit(completionStatus ? 0 : 1);

    }

    public static void writeN(Configuration conf, String bucketname, String nStr) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(URI.create(bucketname), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(bucketname + "/N.txt"));
        PrintWriter writer = new PrintWriter(fsDataOutputStream);
        writer.write(nStr);
        writer.close();
        fsDataOutputStream.close();
    }

}