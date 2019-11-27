package Experiment3;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static Experiment3.ProductConcernCount.productConcernMain;
import static Experiment3.ProductPurchaseCount.productPurchaseMain;

/* 第一个mapreduce仅输出了省份，关注量和产品id，但未进行排序。本程序对结果进行按省份+关注量倒序排序，并输出每个省份的前十。
 * Mapper和Reducer中利用<省份:TreeMap>的键值对来存储各省份的前十产品。
 * 其中TreeMap可以实现以<省份+关注量,产品id>为键值对的有序map，按照键Area_Number进行排序（Area_Number实现了Comparable接口)，
 * 并取出排在最前/最后的key，方便进行删除对应键值对的操作。
 * 当TreeMap存储的键值对数大于10时，取出排在队尾的（同地区关注量最少的）键值对进行删除。
 * */
public class DescSort {
    public static class DescSortMapper extends Mapper<Text, Text, Area_Number, IntWritable>{
        public static Area_Number map_key;
        public static IntWritable map_value;
        public static Map<String, TreeMap<Area_Number, IntWritable>> province_map = new HashMap<>();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            map_key = new Area_Number(key.toString());
            map_value = new IntWritable(Integer.parseInt(value.toString()));
            String area = map_key.getArea();
            if(!province_map.containsKey(area)){
                province_map.put(area, new TreeMap<>());
            }
            province_map.get(area).put(map_key, map_value);
            if(province_map.get(area).size() > 10){
                province_map.get(area).remove(province_map.get(area).lastKey());
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            for(TreeMap<Area_Number, IntWritable> v: province_map.values()){
                for(Area_Number k: v.keySet()){
                    context.write(k, v.get(k));
                }
            }
        }
    }
    public static class DescSortReducer extends Reducer<Area_Number, IntWritable, Area_Number, IntWritable>{
        public static Area_Number reduce_key;
        public static IntWritable reduce_value;
        public static Map<String, TreeMap<Area_Number, IntWritable>> province_map = new HashMap<>();
        public void reduce(Area_Number key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            reduce_key = key;
            reduce_value = value.iterator().next();
            String area = reduce_key.getArea();
            if(!province_map.containsKey(area)){
                province_map.put(area, new TreeMap<>());
            }
            province_map.get(area).put(reduce_key, reduce_value);
            if(province_map.get(area).size() > 10){
                province_map.get(area).remove(province_map.get(area).lastKey());
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            for(TreeMap<Area_Number, IntWritable> v: province_map.values()){
                for(Area_Number k: v.keySet()){
                    context.write(k, v.get(k));
                }
            }
        }
    }
    public static void main(String[] args) throws Exception{
        //0:input path for count; 1:output path for count i.e. input path for sort; 2:output path for sort 3:concern/purchase
        if(args[3].equals("concern"))  productConcernMain(args);
        else if (args[3].equals("purchase")) productPurchaseMain(args);
        else{
            System.out.println("参数输入错误！请输入 concern 或 purchase");
            System.exit(-1);
        }
        String inputPath = args[1];
        String outputPath = args[2];
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator",",");
        Job job = Job.getInstance(conf, "ProductConcernSortResult");
        job.setJarByClass(DescSort.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(DescSortMapper.class);
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
