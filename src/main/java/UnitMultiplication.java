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
            //生成PR Matrix
            //id\t pr0
            //outputkey = id
            //outputValue = pr(n)
            String[] idPr = value.toString().trim().split("\t");
            context.write(new Text(idPr[0]), new Text(idPr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key = fromId
            //value = [toId1 = prob, ... , PR(n)]
            //e.g. fromId = 1; value = [2 = 1/3, 5 = 1/3, 8 = 1/3, 1]
            //outputKey: toId
            //outputValue: prob * PR(n)

            double prCell = 0;
            List<String> transitionCellList = new ArrayList<String>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionCellList.add(value.toString());
                } else {
                    prCell = Double.parseDouble(value.toString());
                }
            }

            for (String transCell : transitionCellList) {
                //transCell : toId = prob
                String toId = transCell.split("=")[0];
                double prob = Double.parseDouble(transCell.split("=")[1]);
                double subPr = prob * prCell;
                context.write(new Text(toId), new Text(String.valueOf(subPr)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //多个mapper从不同的input读数据，需要告诉每个MapReduce从哪里读取数据
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
