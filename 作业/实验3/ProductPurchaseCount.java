package Experiment3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProductPurchaseCount {

    public static class PurchaseCountMapper extends Mapper<Object, Text, Area_Number, IntWritable> {
        public static Area_Number map_value;
        public static IntWritable map_key;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            //只统计进行购买的数据
            if(Integer.parseInt(tuple[7]) != 2) return;
            String area = tuple[10];
            int productID = Integer.parseInt(tuple[1]);
            map_value = new Area_Number(area, productID);
            map_key = new IntWritable(1);
            context.write(map_value, map_key);
        }
    }

    public static class PurchaseCountReducer extends Reducer<Area_Number, IntWritable, Area_Number, IntWritable> {
        public Area_Number reduce_key;
        public IntWritable reduce_value;

        public void reduce(Area_Number key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum_count = 0;
            for (IntWritable val : value) {
                sum_count += val.get();
            }
            reduce_key = new Area_Number(key.getArea(), sum_count);
            reduce_value = new IntWritable(key.getProduct_id());
            context.write(reduce_key, reduce_value);
        }
    }

    public static void productPurchaseMain(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ProductPurchaseCount");
        job.setJarByClass(ProductPurchaseCount.class);
        job.setMapperClass(PurchaseCountMapper.class);
        job.setReducerClass(PurchaseCountReducer.class);
        //job.setCombinerClass(ConcernCountReducer.class);
        job.setMapOutputKeyClass(Area_Number.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Area_Number.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.out.println(job.waitForCompletion(true) ? "success":"failure");
    }
}
