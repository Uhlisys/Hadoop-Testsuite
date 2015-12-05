package com.uhlisys.hadoop.rainbow;

import com.uhlisys.hadoop.rainbow.io.PassgenInputFormat;
import com.uhlisys.hadoop.rainbow.mapreduce.HashingMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class Generator extends Configured implements Tool {

    @Override
    public int run(final String[] args) throws Exception {
        final Job job = Job.getInstance(getConf(), "Rainbow Table Generator");
        job.setJarByClass(Generator.class);

        job.setInputFormatClass(PassgenInputFormat.class);
        job.setMapperClass(HashingMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        TextOutputFormat.setOutputPath(job, new Path(args[0]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : -1;
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new Generator(), args));
    }

}
