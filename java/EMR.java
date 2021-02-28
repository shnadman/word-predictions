import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.io.*;
import java.lang.*;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class EMR {

    public static void main(String[] args) throws Exception {

//M4_XLARGE
        EmrClient mapReduce = EmrClient.builder().credentialsProvider(ProfileCredentialsProvider.builder().profileName("default").build()).build();


        List<StepConfig> steps = new LinkedList<>();
        steps.add(createStepConfig("trigrams", "out1-agg", "SplitDataAndCount"));
        steps.add(createStepConfig("out1-agg", "out2-agg", "CalcR"));
        steps.add(createStepConfig("out2-agg", "out3-agg", "GenerateProbability"));
        steps.add(createStepConfig("out3-agg", "final-agg", "SortResults"));

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(4)
                .masterInstanceType(InstanceType.M4_XLARGE.toString())
                .slaveInstanceType(InstanceType.M4_XLARGE.toString())
                .hadoopVersion("2.7.3")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build()).build();

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("ass2")
                .releaseLabel("emr-5.0.3")
                .instances(instances)
                .steps(steps)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3://dsp-ass-2/logs/").build();


        RunJobFlowResponse runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();

        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static StepConfig createStepConfig(String input, String output, String mainClass) {

        HadoopJarStepConfig hadoopJarStep = HadoopJarStepConfig.builder()
                .jar("s3://dsp-ass-2/jars/" + mainClass + ".jar") // This should be a full map reduce application.
                .mainClass(mainClass)
                .args("s3n://dsp-ass-2/data/" + input + "/", "s3n://dsp-ass-2/data/" + output + "/").build();

        StepConfig stepConfig = StepConfig.builder()
                .name("step - " + mainClass)
                .hadoopJarStep(hadoopJarStep)
                .actionOnFailure("TERMINATE_JOB_FLOW").build();
        return stepConfig;

    }


}
