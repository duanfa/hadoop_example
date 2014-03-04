/*     */ package com.haier.mr.terasort;
/*     */ 
/*     */ import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class TeraValidate extends Configured
/*     */   implements Tool
/*     */ {
/*  53 */   private static final Text error = new Text("error");
/*     */ 
/*     */   public int run(String[] args)
/*     */     throws Exception
/*     */   {
/* 131 */     JobConf job = (JobConf)getConf();
/* 132 */     TeraInputFormat.setInputPaths(job, new Path[] { new Path(args[0]) });
/* 133 */     FileOutputFormat.setOutputPath(job, new Path(args[1]));
/* 134 */     job.setJobName("TeraValidate");
/* 135 */     job.setJarByClass(TeraValidate.class);
/* 136 */     job.setMapperClass(ValidateMapper.class);
/* 137 */     job.setReducerClass(ValidateReducer.class);
/* 138 */     job.setOutputKeyClass(Text.class);
/* 139 */     job.setOutputValueClass(Text.class);
/*     */ 
/* 141 */     job.setNumReduceTasks(1);
/*     */ 
/* 143 */     job.setLong("mapred.min.split.size", 9223372036854775807L);
/* 144 */     job.setInputFormat(TeraInputFormat.class);
/* 145 */     JobClient.runJob(job);
/* 146 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */     throws Exception
/*     */   {
/* 153 */     int res = ToolRunner.run(new JobConf(), new TeraValidate(), args);
/* 154 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   static class ValidateReducer extends MapReduceBase
/*     */     implements Reducer<Text, Text, Text, Text>
/*     */   {
/* 101 */     private boolean firstKey = true;
/* 102 */     private Text lastKey = new Text();
/* 103 */     private Text lastValue = new Text();
/*     */ 
/*     */     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
/*     */     {
/* 107 */       if (TeraValidate.error.equals(key)) {
/* 108 */         while (values.hasNext()) {
/* 109 */           output.collect(key, values.next());
/*     */         }
/*     */       }
/* 112 */       Text value = (Text)values.next();
/* 113 */       if (this.firstKey) {
/* 114 */         this.firstKey = false;
/*     */       }
/* 116 */       else if (value.compareTo(this.lastValue) < 0) {
/* 117 */         output.collect(TeraValidate.error, new Text("misordered keys last: " + this.lastKey + " '" + this.lastValue + "' current: " + key + " '" + value + "'"));
/*     */       }
/*     */ 
/* 123 */       this.lastKey.set(key);
/* 124 */       this.lastValue.set(value);
/*     */     }
/*     */   }
/*     */ 
/*     */   static class ValidateMapper extends MapReduceBase
/*     */     implements Mapper<Text, Text, Text, Text>
/*     */   {
/*     */     private Text lastKey;
/*     */     private OutputCollector<Text, Text> output;
/*     */     private String filename;
/*     */ 
/*     */     private String getFilename(FileSplit split)
/*     */     {
/*  67 */       return split.getPath().getName();
/*     */     }
/*     */ 
/*     */     public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
/*     */     {
/*  72 */       if (this.lastKey == null) {
/*  73 */         this.filename = getFilename((FileSplit)reporter.getInputSplit());
/*  74 */         output.collect(new Text(this.filename + ":begin"), key);
/*  75 */         this.lastKey = new Text();
/*  76 */         this.output = output;
/*     */       }
/*  78 */       else if (key.compareTo(this.lastKey) < 0) {
/*  79 */         output.collect(TeraValidate.error, new Text("misorder in " + this.filename + " last: '" + this.lastKey + "' current: '" + key + "'"));
/*     */       }
/*     */ 
/*  84 */       this.lastKey.set(key);
/*     */     }
/*     */ 
/*     */     public void close() throws IOException {
/*  88 */       if (this.lastKey != null)
/*  89 */         this.output.collect(new Text(this.filename + ":end"), this.lastKey);
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.terasort.TeraValidate
 * JD-Core Version:    0.6.0
 */