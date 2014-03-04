package com.haier.mr;
/*     */ 
/*     */ import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class MultiFileWordCount extends Configured
/*     */   implements Tool
/*     */ {
/*     */   private void printUsage()
/*     */   {
/* 228 */     System.out.println("Usage : multifilewc <input_dir> <output>");
/*     */   }
/*     */ 
/*     */   public int run(String[] args) throws Exception
/*     */   {
/* 233 */     if (args.length < 2) {
/* 234 */       printUsage();
/* 235 */       return 1;
/*     */     }
/*     */ 
/* 238 */     JobConf job = new JobConf(getConf(), MultiFileWordCount.class);
/* 239 */     job.setJobName("MultiFileWordCount");
/*     */ 
/* 242 */     job.setInputFormat(MyInputFormat.class);
/*     */ 
/* 245 */     job.setOutputKeyClass(Text.class);
/*     */ 
/* 247 */     job.setOutputValueClass(LongWritable.class);
/*     */ 
/* 250 */     job.setMapperClass(MapClass.class);
/*     */ 
/* 252 */     job.setCombinerClass(LongSumReducer.class);
/* 253 */     job.setReducerClass(LongSumReducer.class);
/*     */ 
/* 255 */     FileInputFormat.addInputPaths(job, args[0]);
/* 256 */     FileOutputFormat.setOutputPath(job, new Path(args[1]));
/*     */ 
/* 258 */     JobClient.runJob(job);
/*     */ 
/* 260 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args) throws Exception {
/* 264 */     int ret = ToolRunner.run(new MultiFileWordCount(), args);
/* 265 */     System.exit(ret);
/*     */   }
/*     */ 
/*     */   public static class MapClass extends MapReduceBase
/*     */     implements Mapper<MultiFileWordCount.WordOffset, Text, Text, LongWritable>
/*     */   {
/* 210 */     private static final LongWritable one = new LongWritable(1L);
/* 211 */     private Text word = new Text();
/*     */ 
/*     */     public void map(MultiFileWordCount.WordOffset key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 217 */       String line = value.toString();
/* 218 */       StringTokenizer itr = new StringTokenizer(line);
/* 219 */       while (itr.hasMoreTokens()) {
/* 220 */         this.word.set(itr.nextToken());
/* 221 */         output.collect(this.word, one);
/*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class MultiFileLineRecordReader
/*     */     implements RecordReader<MultiFileWordCount.WordOffset, Text>
/*     */   {
/*     */     private MultiFileSplit split;
/*     */     private long offset;
/*     */     private long totLength;
/*     */     private FileSystem fs;
/* 128 */     private int count = 0;
/*     */     private Path[] paths;
/*     */     private FSDataInputStream currentStream;
/*     */     private BufferedReader currentReader;
/*     */ 
/*     */     public MultiFileLineRecordReader(Configuration conf, MultiFileSplit split)
/*     */       throws IOException
/*     */     {
/* 137 */       this.split = split;
/* 138 */       this.fs = FileSystem.get(conf);
/* 139 */       this.paths = split.getPaths();
/* 140 */       this.totLength = split.getLength();
/* 141 */       this.offset = 0L;
/*     */ 
/* 144 */       Path file = this.paths[this.count];
/* 145 */       this.currentStream = this.fs.open(file);
/* 146 */       this.currentReader = new BufferedReader(new InputStreamReader(this.currentStream));
/*     */     }
/*     */     public void close() throws IOException {
/*     */     }
/*     */ 
/*     */     public long getPos() throws IOException {
/* 152 */       long currentOffset = this.currentStream == null ? 0L : this.currentStream.getPos();
/* 153 */       return this.offset + currentOffset;
/*     */     }
/*     */ 
/*     */     public float getProgress() throws IOException {
/* 157 */       return (float)getPos() / (float)this.totLength;
/*     */     }
/*     */ 
/*     */     public boolean next(MultiFileWordCount.WordOffset key, Text value) throws IOException {
/* 161 */       if (this.count >= this.split.getNumPaths()) {
/* 162 */         return false;
/*     */       }
/*     */ 
/*     */       String line;
/*     */       do
/*     */       {
/* 170 */         line = this.currentReader.readLine();
/* 171 */         if (line != null)
/*     */           continue;
/* 173 */         this.currentReader.close();
/* 174 */         this.offset += this.split.getLength(this.count);
/*     */ 
/* 176 */         if (++this.count >= this.split.getNumPaths()) {
/* 177 */           return false;
/*     */         }
/*     */ 
/* 180 */         Path file = this.paths[this.count];
/* 181 */         this.currentStream = this.fs.open(file);
/* 182 */         this.currentReader = new BufferedReader(new InputStreamReader(this.currentStream));
/* 183 */         MultiFileWordCount.WordOffset.access$002(key, file.getName());
/*     */       }
/* 185 */       while (line == null);
/*     */ 
/* 187 */       MultiFileWordCount.WordOffset.access$102(key, this.currentStream.getPos());
/* 188 */       value.set(line);
/*     */ 
/* 190 */       return true;
/*     */     }
/*     */ 
/*     */     public MultiFileWordCount.WordOffset createKey() {
/* 194 */       MultiFileWordCount.WordOffset wo = new MultiFileWordCount.WordOffset();
/* 195 */       MultiFileWordCount.WordOffset.access$002(wo, this.paths[0].toString());
/* 196 */       return wo;
/*     */     }
/*     */ 
/*     */     public Text createValue() {
/* 200 */       return new Text();
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class MyInputFormat extends MultiFileInputFormat<MultiFileWordCount.WordOffset, Text>
/*     */   {
/*     */     public RecordReader<MultiFileWordCount.WordOffset, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 112 */       return new MultiFileWordCount.MultiFileLineRecordReader(job, (MultiFileSplit)split);
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class WordOffset
/*     */     implements WritableComparable
/*     */   {
/*     */     private long offset;
/*     */     private String fileName;
/*     */ 
/*     */     public void readFields(DataInput in)
/*     */       throws IOException
/*     */     {
/*  69 */       this.offset = in.readLong();
/*  70 */       this.fileName = Text.readString(in);
/*     */     }
/*     */ 
/*     */     public void write(DataOutput out) throws IOException {
/*  74 */       out.writeLong(this.offset);
/*  75 */       Text.writeString(out, this.fileName);
/*     */     }
/*     */ 
/*     */     public int compareTo(Object o) {
/*  79 */       WordOffset that = (WordOffset)o;
/*     */ 
/*  81 */       int f = this.fileName.compareTo(that.fileName);
/*  82 */       if (f == 0) {
/*  83 */         return (int)Math.signum(this.offset - that.offset);
/*     */       }
/*  85 */       return f;
/*     */     }
/*     */ 
/*     */     public boolean equals(Object obj) {
/*  89 */       if ((obj instanceof WordOffset))
/*  90 */         return compareTo(obj) == 0;
/*  91 */       return false;
/*     */     }
/*     */ 
/*     */     public int hashCode() {
/*  95 */       if (!$assertionsDisabled) throw new AssertionError("hashCode not designed");
/*  96 */       return 42;
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.MultiFileWordCount
 * JD-Core Version:    0.6.0
 */