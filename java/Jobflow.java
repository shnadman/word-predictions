import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.LinkedList;

public class Jobflow {
    public static void main(String[] args) throws Exception {

        try{
            Configuration conf = new Configuration();
            String inputPath ="ngrams.txt";
            String outputPath ="out1";
            Job job = Job.getInstance(conf, "SplitDataCount");
            job.setJarByClass(SplitDataAndCount.class);
            job.setMapperClass(SplitDataAndCount.MapperClass.class);
            //job.setPartitionerClass(SplitDataAndCount.PartitionerClass.class);
            // job.setCombinerClass(ReducerClass.class);
            job.setReducerClass(SplitDataAndCount.ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PairTextLongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PairLongsWritable.class);
            //FileInputFormat.addInputPath(job, new Path(S3HebrewTrigrams));
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            //job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);


             inputPath ="out1";
             outputPath ="out2";

            Job job2 = Job.getInstance(conf, "CalcR");

            job2.setJarByClass(CalcR.class);
            job2.setMapperClass(CalcR.MapperClass.class);
            //job2.setPartitionerClass(CalcR.PartitionerClass.class);
            job2.setReducerClass(CalcR.ReducerClass.class);
            job2.setMapOutputKeyClass(PairTextLongWritable.class);
            job2.setMapOutputValueClass(PairTextLongWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(PairTextLongWritable.class);
            FileInputFormat.addInputPath(job2, new Path(inputPath));
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

             inputPath ="out2";
             outputPath ="out3";


            Job job3 = Job.getInstance(conf, "Probability");
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(PairTextLongWritable.class);
            job3.setJarByClass(GenerateProbability.class);
            job3.setReducerClass(GenerateProbability.ReducerClass.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(FloatWritable.class);
            FileInputFormat.addInputPath(job3, new Path(inputPath));
            FileOutputFormat.setOutputPath(job3, new Path(outputPath));
            job3.setInputFormatClass(SequenceFileInputFormat.class);
            job3.setOutputFormatClass(SequenceFileOutputFormat.class);

             inputPath ="out3";
             outputPath ="final";

           Job  job4 = Job.getInstance(conf, "Sort");

            job4.setJarByClass(SortResults.class);
            job4.setMapperClass(SortResults.MapperClass.class);
            job4.setReducerClass(SortResults.ReducerClass.class);
            job4.setMapOutputKeyClass(PairTextFloatWritable.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(FloatWritable.class);
            FileInputFormat.addInputPath(job4, new Path(inputPath));
            FileOutputFormat.setOutputPath(job4, new Path(outputPath));
            job4.setInputFormatClass(SequenceFileInputFormat.class);
            job4.setOutputFormatClass(TextOutputFormat.class);


            job.waitForCompletion(true);
            long n =job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
                    "MAP_OUTPUT_RECORDS").getValue();
            //Counter n = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
            conf.set("N", String.valueOf(n));

            //ControlledJob controlledJob1 = new ControlledJob(job,new LinkedList<ControlledJob>());
            ControlledJob controlledJob2 = new ControlledJob(job2,new LinkedList<ControlledJob>());
            ControlledJob controlledJob3 = new ControlledJob(job3,new LinkedList<ControlledJob>());
            ControlledJob controlledJob4 = new ControlledJob(job4,new LinkedList<ControlledJob>());
            //controlledJob2.addDependingJob(controlledJob1);
            controlledJob3.addDependingJob(controlledJob2);
            controlledJob4.addDependingJob(controlledJob3);
            JobControl jc = new JobControl("JC");
            jc.addJob(controlledJob2);
            jc.addJob(controlledJob3);
            jc.addJob(controlledJob4);
            jc.run();
        }

        catch (Exception e){
            System.out.println("shit went sour");

        }


    }

}
