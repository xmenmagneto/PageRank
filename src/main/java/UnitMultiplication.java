import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //transition mapper
            //input: from ID\t to1, to2, to3
            //outputKey: fromId
            //outputValue: toId = prob
            String[] fromTos = value.toString().trim().split("\t");//拆分from and to

            //edge case: fromID\t 后面没了, fromTos.length < 2
            if (fromTos.length < 2) {
                return; //面试不能这样写，
            }

            String from = fromTos[0];
            String[] tos = fromTos[1].split(",");
            for (String to: tos) {
                context.write(new Text(from), new Text(to + "=" + (double) 1 / tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> transitionUnit = new ArrayList<String>();

        }
    }

    public static void main(String[] args) throws Exception {


    }

}
