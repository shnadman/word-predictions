import org.apache.hadoop.fs.Path;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CalcR {

    public static class MapperClass extends Mapper<Text, PairLongsWritable, PairTextLongWritable, PairTextLongWritable> {

        private void writeDollarR(String set, long rKey, Text ngram, long toCount, Context context) throws IOException, InterruptedException {
            String newSet = set + "$" + rKey;
//
//            ( < Nr0$139, -1 >,<ngram, count >)
//            ( < Nr0$139, 1 ><ngram, count >)
//
//            ( < Tr0$139, -1 >,<ngram, count >)
//            ( < Tr0$139, 1 ><ngram, count >)
//
//            ( < Nr0$139, -1 >,<ngram, count >)
//            ( < Nr0$139, 1 ><ngram, count >)
//
//            ( < Nr0$139, -1 >,<ngram, count >)
//            ( < Nr0$139, 1 ><ngram, count >)


            context.write(new PairTextLongWritable(newSet, -1l), new PairTextLongWritable(ngram, toCount));
            context.write(new PairTextLongWritable(newSet, 1l), new PairTextLongWritable(ngram, toCount));
        }

        @Override
        public void map(Text ngram, PairLongsWritable values, Context context) throws IOException, InterruptedException {

            //Calculating Nrs = Number of DIFFERENT ngrams appearing r time in part A and B
            long Ra = values.getLeft().get(); //frequency r of ngram in A
            long Rb = values.getRight().get();//frequency r of ngram in B

            this.writeDollarR("Nr0", Ra, ngram, Ra, context);
            this.writeDollarR("Nr1", Rb, ngram, Rb, context);
            this.writeDollarR("Tr01", Ra, ngram, Rb, context);
            this.writeDollarR("Tr10", Rb, ngram, Ra, context);
        }
    }


    public static class ReducerClass extends Reducer<PairTextLongWritable, PairTextLongWritable, Text, PairTextLongWritable> {
        Text lastSet = null;
        String realSet = null;
        Long sum = 0l;

        @Override
        public void reduce(PairTextLongWritable key, Iterable<PairTextLongWritable> values, Context context) throws IOException, InterruptedException {
            if (!key.getLeft().equals(lastSet)) {
                lastSet = key.getLeft();
                sum = 0l;
            }
            if (key.getRight().get() < 0) {
                for (PairTextLongWritable value : values) {
                    sum += value.getRight().get();
                }
            } else {
                realSet = key.getLeft().toString().split("\\$")[0];
                for (PairTextLongWritable value : values) {
                    PairTextLongWritable setAndOcc = new PairTextLongWritable(realSet, sum);
                    context.write(value.getLeft(), setAndOcc);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            sum = 0l;
        }
    }


    public static void main(String[] args) throws Exception {
        String inputPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "CalcR");

        job.setJarByClass(CalcR.class);
        job.setMapperClass(CalcR.MapperClass.class);
        //job.setPartitionerClass(CalcR.PartitionerClass.class);
        job.setReducerClass(CalcR.ReducerClass.class);
        job.setMapOutputKeyClass(PairTextLongWritable.class);
        job.setMapOutputValueClass(PairTextLongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairTextLongWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}