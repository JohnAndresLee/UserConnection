package UC;

import java.io.*;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
//import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserConnection {

    static class ValuePair implements WritableComparable<ValuePair> {

        private String friend;
        private String union;//连结关系

        public String getFriend() {
            return friend;
        }

        public void setFriend(String friend) {
            this.friend = friend;
        }

        public String getUnion() {
            return union;
        }

        public void setUnion(String union) {
            this.union = union;
        }

        public ValuePair() {
            // TODO Auto-generated constructor stub
        }

        public ValuePair(String n, String u) {
            this.friend = n;
            this.union = u;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.friend = in.readUTF();
            this.union = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.friend);
            out.writeUTF(this.union);
        }

        @Override
        public int compareTo(ValuePair o) {
            if (this.friend.equals(o.getFriend())) {
                return this.union.compareTo(o.getUnion());
            } else {
                return this.friend.compareTo(o.getFriend());
            }
        }

    }


    static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

        private final Text userkey = new Text();
        private final Text uservalue = new Text();

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String input = value.toString();
            String[] s = input.split("	");
            userkey.set(s[0]);
            uservalue.set(s[1]);
            context.write(userkey, uservalue);
        }
    }

    static class Reduce1 extends Reducer<Text, Text, Text, Text>{

        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            String result1="";
            for (Text value:values){
                result1+=value.toString()+",";
            }
            Text result2 = new Text();
            result2.set(result1.substring(0,result1.length()-1));
            context.write(key, result2);
        }
    }

    static class Map2 extends Mapper<LongWritable, Text, Text, ValuePair> {

        private Text outkey = new Text();
        private ValuePair outvalue = new ValuePair();

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String input = value.toString();
            String[] sz = input.split("	");
            outkey.set(sz[0]);
            String[] sz1 = sz[1].split(",");
            for (int i = 0; i < sz1.length; i++) {
                outvalue.setFriend(sz1[i]);
                outvalue.setUnion("yes"); //yes表示key与outvalue.friend已经为好友关系
                context.write(outkey, outvalue);
            }

            for (int i = 0; i < sz1.length; i++) {
                for (int j = i + 1; j < sz1.length; j++) {
                    this.outkey.set(sz1[i]);
                    this.outvalue.setFriend(sz1[j]);
                    this.outvalue.setUnion(sz[0]); //表示key与outvalue.friend共同的好友
                    context.write(this.outkey, this.outvalue);

                    this.outkey.set(sz1[j]);
                    this.outvalue.setFriend(sz1[i]);
                    //存疑
                    this.outvalue.setUnion(sz[0]); //表示key与outvalue.friend共同的好友
                    context.write(this.outkey, this.outvalue);
                }
            }
        }
    }

    static class Reduce2 extends Reducer<Text, ValuePair, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        private static int[] quickSort(List<Integer> keys) {
            int[] indices = new int[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                indices[i] = i;
            }
            quickSort2(keys, 0, keys.size()-1, indices);
            return indices;
        }

        private static void quickSort2(List<Integer> keys, int begin, int end, int[] indices) {
            if (begin >= 0 && begin < keys.size() && end >= 0 && end < keys.size() && begin < end) {
                int i = begin, j = end;
                int vot = keys.get(i);
                int temp = indices[i];
                while (i != j) {
                    while(i < j && keys.get(j) >= vot) j--;
                    if(i < j) {
                        keys.set(i, keys.get(j));
                        indices[i] = indices[j];
                        i++;
                    }
                    while(i < j && keys.get(i) <= vot)  i++;
                    if(i < j) {
                        keys.set(j, keys.get(i));
                        indices[j] = indices[i];
                        j--;
                    }
                }
                keys.set(i, vot);
                indices[i] = temp;
                quickSort2(keys, begin, j-1, indices);
                quickSort2(keys, i+1, end, indices);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<ValuePair> values,
                              Context context) throws IOException, InterruptedException {
            String s_tmp = key.toString();
            s_tmp += ':';
            this.outKey.set(s_tmp);
            Multimap<String, String> map = HashMultimap.create();
            for (ValuePair v : values) {
                map.put(v.getFriend(), v.getUnion());
            }

            StringBuilder outString = new StringBuilder();
            List<String> outString_tmp = new ArrayList<>();
            Set<String> keys = map.keySet();
            List<Integer> l = new ArrayList();
            for(String s : keys){
                StringBuilder outStr = new StringBuilder();
                boolean isok = true;
                int acc = 0;
                Collection<String> v = map.get(s);
                outStr.append(s + "(");
                String tmp = new String();
                for(String u : v){
                    if(u.equals("yes")){
                        isok = false;
                        break;
                    }
                    tmp += u + ",";
                }
                if(tmp.length()>=2){
                    tmp = tmp.substring(0, tmp.length()-1);
                }
                if(isok){
                    outStr.append(v.size() + ":" + "[" + tmp.trim() + "]), ");
                    outString_tmp.add(String.valueOf(outStr));
                    l.add(v.size());
                }
            }

            l.toArray();
            int[] idx = quickSort(l);
            for (int i = idx.length-1; i >= 0; i--) {
                outString.append(outString_tmp.get(idx[i]));
            }

            String aa = outString.toString();
            if (aa.length()>=2){
                outValue.set(aa.substring(0, aa.length()-2));
            }
            context.write(outKey, outValue);

        }

    }

    private static String inputPath = "/user/hadoop/hw6_input";
    private static String tempDir = "/user/hadoop/out-friend-tmp";
    private static String outputDir = "/user/hadoop/out-friend";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "UserConnection");
        job.setJarByClass(UserConnection.class);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(inputPath);
        if (fs.exists(inPath)) {
            FileInputFormat.addInputPath(job, inPath);
        }

        Path tempPath = new Path(tempDir);
        fs.delete(tempPath, true);
        FileOutputFormat.setOutputPath(job, tempPath);

        if(job.waitForCompletion(true))
        {
            Job job2 = new Job(conf, "UserConnection2");
            job2.setJarByClass(UserConnection.class);

            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(ValuePair.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            Path inputPath = new Path(tempDir);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job2, inputPath);
            }

            Path outputPath = new Path(outputDir);
            fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job2, outputPath);

            if(job2.waitForCompletion(true))//删除中间文件
                fs.delete(tempPath);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
