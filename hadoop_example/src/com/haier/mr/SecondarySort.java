 package com.haier.mr;
/*     */ 
/*     */ import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/*     */ 
/*     */ public class SecondarySort
/*     */ {
/*     */   public static void main(String[] args)
/*     */     throws Exception
/*     */   {
/* 211 */     Configuration conf = new Configuration();
/* 212 */     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
/* 213 */     if (otherArgs.length != 2) {
/* 214 */       System.err.println("Usage: secondarysrot <in> <out>");
/* 215 */       System.exit(2);
/*     */     }
/* 217 */     Job job = new Job(conf, "secondary sort");
/* 218 */     job.setJarByClass(SecondarySort.class);
/* 219 */     job.setMapperClass(MapClass.class);
/* 220 */     job.setReducerClass(Reduce.class);
/*     */ 
/* 223 */     job.setPartitionerClass(FirstPartitioner.class);
/* 224 */     job.setGroupingComparatorClass(FirstGroupingComparator.class);
/*     */ 
/* 227 */     job.setMapOutputKeyClass(IntPair.class);
/* 228 */     job.setMapOutputValueClass(IntWritable.class);
/*     */ 
/* 231 */     job.setOutputKeyClass(Text.class);
/* 232 */     job.setOutputValueClass(IntWritable.class);
/*     */ 
/* 234 */     FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
/* 235 */     FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
/* 236 */     System.exit(job.waitForCompletion(true) ? 0 : 1);
/*     */   }
/*     */ 
/*     */   public static class Reduce extends Reducer<SecondarySort.IntPair, IntWritable, Text, IntWritable>
/*     */   {
/* 194 */     private static final Text SEPARATOR = new Text("------------------------------------------------");
/*     */ 
/* 196 */     private final Text first = new Text();
/*     */ 
/*     */     public void reduce(SecondarySort.IntPair key, Iterable<IntWritable> values, Reducer<SecondarySort.IntPair, IntWritable, Text, IntWritable>.Context context)
/*     */       throws IOException, InterruptedException
/*     */     {
/* 202 */       context.write(SEPARATOR, null);
/* 203 */       this.first.set(Integer.toString(key.getFirst()));
/* 204 */       for (IntWritable value : values)
/* 205 */         context.write(this.first, value);
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class MapClass extends Mapper<LongWritable, Text, SecondarySort.IntPair, IntWritable>
/*     */   {
/* 168 */     private final SecondarySort.IntPair key = new SecondarySort.IntPair();
/* 169 */     private final IntWritable value = new IntWritable();
/*     */ 
/*     */     public void map(LongWritable inKey, Text inValue, Mapper<LongWritable, Text, SecondarySort.IntPair, IntWritable>.Context context)
/*     */       throws IOException, InterruptedException
/*     */     {
/* 174 */       StringTokenizer itr = new StringTokenizer(inValue.toString());
/* 175 */       int left = 0;
/* 176 */       int right = 0;
/* 177 */       if (itr.hasMoreTokens()) {
/* 178 */         left = Integer.parseInt(itr.nextToken());
/* 179 */         if (itr.hasMoreTokens()) {
/* 180 */           right = Integer.parseInt(itr.nextToken());
/*     */         }
/* 182 */         this.key.set(left, right);
/* 183 */         this.value.set(right);
/* 184 */         context.write(this.key, this.value);
/*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class FirstGroupingComparator
/*     */     implements RawComparator<SecondarySort.IntPair>
/*     */   {
/*     */     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
/*     */     {
/* 149 */       return WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
/*     */     }
/*     */ 
/*     */     public int compare(SecondarySort.IntPair o1, SecondarySort.IntPair o2)
/*     */     {
/* 155 */       int l = o1.getFirst();
/* 156 */       int r = o2.getFirst();
/* 157 */       return l < r ? -1 : l == r ? 0 : 1;
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class FirstPartitioner extends Partitioner<SecondarySort.IntPair, IntWritable>
/*     */   {
/*     */     public int getPartition(SecondarySort.IntPair key, IntWritable value, int numPartitions)
/*     */     {
/* 137 */       return Math.abs(key.getFirst() * 127) % numPartitions;
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class IntPair
/*     */     implements WritableComparable<IntPair>
/*     */   {
/*     */     private int first;
/*     */     private int second;
/*     */ 
/*     */     public IntPair()
/*     */     {
/*  59 */       this.first = 0;
/*  60 */       this.second = 0;
/*     */     }
/*     */ 
/*     */     public void set(int left, int right)
/*     */     {
/*  66 */       this.first = left;
/*  67 */       this.second = right;
/*     */     }
/*     */     public int getFirst() {
/*  70 */       return this.first;
/*     */     }
/*     */     public int getSecond() {
/*  73 */       return this.second;
/*     */     }
/*     */ 
/*     */     public void readFields(DataInput in)
/*     */       throws IOException
/*     */     {
/*  81 */       this.first = (in.readInt() + -2147483648);
/*  82 */       this.second = (in.readInt() + -2147483648);
/*     */     }
/*     */ 
/*     */     public void write(DataOutput out) throws IOException {
/*  86 */       out.writeInt(this.first - -2147483648);
/*  87 */       out.writeInt(this.second - -2147483648);
/*     */     }
/*     */ 
/*     */     public int hashCode() {
/*  91 */       return this.first * 157 + this.second;
/*     */     }
/*     */ 
/*     */     public boolean equals(Object right) {
/*  95 */       if ((right instanceof IntPair)) {
/*  96 */         IntPair r = (IntPair)right;
/*  97 */         return (r.first == this.first) && (r.second == this.second);
/*     */       }
/*  99 */       return false;
/*     */     }
/*     */ 
/*     */     public int compareTo(IntPair o)
/*     */     {
/* 120 */       if (this.first != o.first)
/* 121 */         return this.first < o.first ? -1 : 1;
/* 122 */       if (this.second != o.second) {
/* 123 */         return this.second < o.second ? -1 : 1;
/*     */       }
/* 125 */       return 0;
/*     */     }
/*     */ 
/*     */     static
/*     */     {
/* 115 */       WritableComparator.define(IntPair.class, new Comparator());
/*     */     }
/*     */ 
/*     */     public static class Comparator extends WritableComparator
/*     */     {
/*     */       public Comparator()
/*     */       {
/* 105 */         super();
/*     */       }
/*     */ 
/*     */       public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
/*     */       {
/* 110 */         return compareBytes(b1, s1, l1, b2, s2, l2);
/*     */       }
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.SecondarySort
 * JD-Core Version:    0.6.0
 */