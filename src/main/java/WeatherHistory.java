import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;


public class WeatherHistory {



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        org.apache.log4j.BasicConfigurator.configure();
       int start_year, end_year;
       if(args.length == 2){
            start_year = Integer.parseInt(args[0]);
            end_year = Integer.parseInt(args[1]);
        }
        else{
            start_year = 1901;
            end_year = 1910;
        }

        WeatherHistoryFileManagement.getFilesFromYearBounds(start_year, end_year);

        Configuration configuration = new Configuration();
        JobConf jobConf = new JobConf(configuration);
        Job job = Job.getInstance(jobConf, "Weather-Temperature");
        job.setJarByClass(WeatherHistory.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(WeatherHistoryMapper.class);
        job.setReducerClass(WeatherHistoryReducer.class);
        FileInputFormat.addInputPath(job, new Path("Input\\"));
        FileOutputFormat.setOutputPath(job, new Path("Output\\"));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
