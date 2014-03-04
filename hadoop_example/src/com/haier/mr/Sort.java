/*     */ package com.haier.mr;
/*     */ 
/*     */ import java.io.PrintStream;
/*     */ import java.net.URI;
/*     */ import java.util.ArrayList;
/*     */ import java.util.Date;
/*     */ import java.util.List;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.conf.Configured;
/*     */ import org.apache.hadoop.filecache.DistributedCache;
/*     */ import org.apache.hadoop.fs.Path;
/*     */ import org.apache.hadoop.io.BytesWritable;
/*     */ import org.apache.hadoop.io.Writable;
/*     */ import org.apache.hadoop.io.WritableComparable;
/*     */ import org.apache.hadoop.mapred.ClusterStatus;
/*     */ import org.apache.hadoop.mapred.FileInputFormat;
/*     */ import org.apache.hadoop.mapred.FileOutputFormat;
/*     */ import org.apache.hadoop.mapred.InputFormat;
/*     */ import org.apache.hadoop.mapred.JobClient;
/*     */ import org.apache.hadoop.mapred.JobConf;
/*     */ import org.apache.hadoop.mapred.OutputFormat;
/*     */ import org.apache.hadoop.mapred.RunningJob;
/*     */ import org.apache.hadoop.mapred.SequenceFileInputFormat;
/*     */ import org.apache.hadoop.mapred.SequenceFileOutputFormat;
/*     */ import org.apache.hadoop.mapred.lib.IdentityMapper;
/*     */ import org.apache.hadoop.mapred.lib.IdentityReducer;
/*     */ import org.apache.hadoop.mapred.lib.InputSampler;
/*     */ import org.apache.hadoop.mapred.lib.InputSampler.RandomSampler;
/*     */ import org.apache.hadoop.mapred.lib.InputSampler.Sampler;
/*     */ import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
/*     */ import org.apache.hadoop.util.Tool;
/*     */ import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class Sort<K, V> extends Configured
/*     */   implements Tool
/*     */ {
/*  54 */   private RunningJob jobResult = null;
/*     */ 
/*     */   static int printUsage() {
/*  57 */     System.out.println("sort [-m <maps>] [-r <reduces>] [-inFormat <input format class>] [-outFormat <output format class>] [-outKey <output key class>] [-outValue <output value class>] [-totalOrder <pcnt> <num samples> <max splits>] <input> <output>");
/*     */ 
/*  64 */     ToolRunner.printGenericCommandUsage(System.out);
/*  65 */     return -1;
/*     */   }
/*     */ 
/*     */   public int run(String[] args)
/*     */     throws Exception
/*     */   {
/*  76 */     JobConf jobConf = new JobConf(getConf(), Sort.class);
/*  77 */     jobConf.setJobName("sorter");
/*     */ 
/*  79 */     jobConf.setMapperClass(IdentityMapper.class);
/*  80 */     jobConf.setReducerClass(IdentityReducer.class);
/*     */ 
/*  82 */     JobClient client = new JobClient(jobConf);
/*  83 */     ClusterStatus cluster = client.getClusterStatus();
/*  84 */     int num_reduces = (int)(cluster.getMaxReduceTasks() * 0.9D);
/*  85 */     String sort_reduces = jobConf.get("test.sort.reduces_per_host");
/*  86 */     if (sort_reduces != null) {
/*  87 */       num_reduces = cluster.getTaskTrackers() * Integer.parseInt(sort_reduces);
/*     */     }
/*     */ 
/*  90 */     Class inputFormatClass = SequenceFileInputFormat.class;
/*     */ 
/*  92 */     Class outputFormatClass = SequenceFileOutputFormat.class;
/*     */ 
/*  94 */     Class outputKeyClass = BytesWritable.class;
/*  95 */     Class outputValueClass = BytesWritable.class;
/*  96 */     List otherArgs = new ArrayList();
/*  97 */     InputSampler.Sampler sampler = null;
/*  98 */     for (int i = 0; i < args.length; i++) {
/*     */       try {
/* 100 */         if ("-m".equals(args[i])) {
/* 101 */           i++; jobConf.setNumMapTasks(Integer.parseInt(args[i]));
/* 102 */         } else if ("-r".equals(args[i])) {
/* 103 */           i++; num_reduces = Integer.parseInt(args[i]);
/* 104 */         } else if ("-inFormat".equals(args[i])) {
/* 105 */           i++; inputFormatClass = Class.forName(args[i]).asSubclass(InputFormat.class);
/*     */         }
/* 107 */         else if ("-outFormat".equals(args[i])) {
/* 108 */           i++; outputFormatClass = Class.forName(args[i]).asSubclass(OutputFormat.class);
/*     */         }
/* 110 */         else if ("-outKey".equals(args[i])) {
/* 111 */           i++; outputKeyClass = Class.forName(args[i]).asSubclass(WritableComparable.class);
/*     */         }
/* 113 */         else if ("-outValue".equals(args[i])) {
/* 114 */           i++; outputValueClass = Class.forName(args[i]).asSubclass(Writable.class);
/*     */         }
/* 116 */         else if ("-totalOrder".equals(args[i])) {
/* 117 */           i++; double pcnt = Double.parseDouble(args[i]);
/* 118 */           i++; int numSamples = Integer.parseInt(args[i]);
/* 119 */           i++; int maxSplits = Integer.parseInt(args[i]);
/* 120 */           if (0 >= maxSplits) maxSplits = 2147483647;
/* 121 */           sampler = new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
/*     */         }
/*     */         else {
/* 124 */           otherArgs.add(args[i]);
/*     */         }
/*     */       } catch (NumberFormatException except) {
/* 127 */         System.out.println("ERROR: Integer expected instead of " + args[i]);
/* 128 */         return printUsage();
/*     */       } catch (ArrayIndexOutOfBoundsException except) {
/* 130 */         System.out.println("ERROR: Required parameter missing from " + args[(i - 1)]);
/*     */ 
/* 132 */         return printUsage();
/*     */       }
/*     */ 
/*     */     }
/*     */ 
/* 137 */     jobConf.setNumReduceTasks(num_reduces);
/*     */ 
/* 139 */     jobConf.setInputFormat(inputFormatClass);
/* 140 */     jobConf.setOutputFormat(outputFormatClass);
/*     */ 
/* 142 */     jobConf.setOutputKeyClass(outputKeyClass);
/* 143 */     jobConf.setOutputValueClass(outputValueClass);
/*     */ 
/* 146 */     if (otherArgs.size() != 2) {
/* 147 */       System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 2.");
/*     */ 
/* 149 */       return printUsage();
/*     */     }
/* 151 */     FileInputFormat.setInputPaths(jobConf, (String)otherArgs.get(0));
/* 152 */     FileOutputFormat.setOutputPath(jobConf, new Path((String)otherArgs.get(1)));
/*     */ 
/* 154 */     if (sampler != null) {
/* 155 */       System.out.println("Sampling input to effect total-order sort...");
/* 156 */       jobConf.setPartitionerClass(TotalOrderPartitioner.class);
/* 157 */       Path inputDir = FileInputFormat.getInputPaths(jobConf)[0];
/* 158 */       inputDir = inputDir.makeQualified(inputDir.getFileSystem(jobConf));
/* 159 */       Path partitionFile = new Path(inputDir, "_sortPartitioning");
/* 160 */       TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
/* 161 */       InputSampler.writePartitionFile(jobConf, sampler);
/* 162 */       URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning");
/*     */ 
/* 164 */       DistributedCache.addCacheFile(partitionUri, jobConf);
/* 165 */       DistributedCache.createSymlink(jobConf);
/*     */     }
/*     */ 
/* 168 */     System.out.println("Running on " + cluster.getTaskTrackers() + " nodes to sort from " + FileInputFormat.getInputPaths(jobConf)[0] + " into " + FileOutputFormat.getOutputPath(jobConf) + " with " + num_reduces + " reduces.");
/*     */ 
/* 174 */     Date startTime = new Date();
/* 175 */     System.out.println("Job started: " + startTime);
/* 176 */     this.jobResult = JobClient.runJob(jobConf);
/* 177 */     Date end_time = new Date();
/* 178 */     System.out.println("Job ended: " + end_time);
/* 179 */     System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000L + " seconds.");
/*     */ 
/* 181 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */     throws Exception
/*     */   {
/* 187 */     int res = ToolRunner.run(new Configuration(), new Sort(), args);
/* 188 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   public RunningJob getResult()
/*     */   {
/* 196 */     return this.jobResult;
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.Sort
 * JD-Core Version:    0.6.0
 */