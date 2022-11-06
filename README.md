## UserConnection

### 问题

目前有大量社交网络网站（Facebook，LinkedIn等），采用双向好友关系，它们有一个共同的特性，就是可以推荐联系人。基本思想是：如果A是B的好友，A又是C的好友，但是B和C不认识，那么社交网络系统会推荐B与C联系或者推荐C与B联系。请设计MapReduce程序实现好友推荐。

要求1：用伪代码描述程序设计思想；

要求2：提交源程序和运行结果（样例的结果，完整数据集或其子集的结果）。

### 输入&输出

（1）输入：输入数据为文本文件，每一行分别包括用户ID（user_id）和它的直接好友ID（friend_id）。user_id和friend_id用‘TAB’分隔。
（2）输出：输出格式要求如下：每一行为USER: F (M: [I1, I2, I3, ….]), …  其中F是推荐给USER的一个好友，M是共同好友数量；I1, I2, I3,…是共同好友的ID。

### 问题分析

本实验将该问题的解决分为两个MapReduce阶段：

（1）MapReduce得到每个用户对应的直接好友列表

（2）MapReduce得到推荐给每个用户的用户列表（二度朋友）

### 阶段一

设置key为每个用户，value为每个用户的直接好友列表

为了方便表示，采用text存储直接好友列表，以","隔开

```java
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
```

### 阶段二

思路：

key依然为user，将value设计成与当前user有关联关系的人及关联关系的说明，例如：

- key："1"  value：<"2", "yes"> 表示"1"与"2"已经是好友关系
- key："2" value：<"3", "1">表示"2"与"3"共同的好友为"1"

对reduce后的key的value做归类，比如把key为"2"的键值对中所有第一个值为"3"的value归为一组，这一组value中的第二个值若没有"yes"出现，则把"3"推荐给"2"，若有"yes"出现，说明"2"与"3"已为好友关系。

该部分的主体for循环如下所述：

```java
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
```

### 排序输出

虽然题目说明中并没有要求根据推荐用户数量进行排序，但是在期望的输出中可以看到每个用户的推荐顺序是按照推荐个数降序排序的，因此本实验中采用了快速排序

```java
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
```

### Value数据类型的自定义

Value类型的自定义关键在于三个函数的构建：readFields, write, compareTo.

序列化在分布式环境的两大作用：进程间通信，永久存储。

自定义value的数据类型需要实现Writable接口才能实现序列化，自定义的key则必须实现WritableComparable接口（WritableComparable继承自Writable和Comparable接口）

```java
static class ValuePair implements WritableComparable<ValuePair> {
    private String friend;
    private String union;

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
```


### 实验结果（以sample为例展示）

命令行执行jar文件（由于程序内置了输入输出路径，故不在命令行中体现）

![截图 2022-11-06 18-27-41](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/%E6%88%AA%E5%9B%BE%202022-11-06%2018-27-41.png)

输出的结果如下所示：

![截图 2022-11-06 18-28-40](https://cdn.jsdelivr.net/gh/JohnAndresLee/websitepicture/%E6%88%AA%E5%9B%BE%202022-11-06%2018-28-40.png)

稍大样本的结果由于过大，因此放在了云盘上：

https://box.nju.edu.cn/d/34931e6e151a4d56acde/
