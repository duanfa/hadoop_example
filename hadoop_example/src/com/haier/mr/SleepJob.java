/*     */  package com.haier.mr;
/*     */ 
/*     */ import java.io.DataInput;
/*     */ import java.io.DataOutput;
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.util.Iterator;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.conf.Configured;
/*     */ import org.apache.hadoop.fs.Path;
/*     */ import org.apache.hadoop.io.IntWritable;
/*     */ import org.apache.hadoop.io.NullWritable;
/*     */ import org.apache.hadoop.mapred.FileInputFormat;
/*     */ import org.apache.hadoop.mapred.InputFormat;
/*     */ import org.apache.hadoop.mapred.InputSplit;
/*     */ import org.apache.hadoop.mapred.JobClient;
/*     */ import org.apache.hadoop.mapred.JobConf;
/*     */ import org.apache.hadoop.mapred.Mapper;
/*     */ import org.apache.hadoop.mapred.OutputCollector;
/*     */ import org.apache.hadoop.mapred.Partitioner;
/*     */ import org.apache.hadoop.mapred.RecordReader;
/*     */ import org.apache.hadoop.mapred.Reducer;
/*     */ import org.apache.hadoop.mapred.Reporter;
/*     */ import org.apache.hadoop.mapred.lib.NullOutputFormat;
/*     */ import org.apache.hadoop.util.Tool;
/*     */ import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class SleepJob extends Configured
/*     */   implements Tool, Mapper<IntWritable, IntWritable, IntWritable, NullWritable>, Reducer<IntWritable, NullWritable, NullWritable, NullWritable>, Partitioner<IntWritable, NullWritable>
/*     */ {
/*     */   private long mapSleepDuration;
/*     */   private long reduceSleepDuration;
/*     */   private int mapSleepCount;
/*     */   private int reduceSleepCount;
/*     */   private int count;
/*     */ 
/*     */   public SleepJob()
/*     */   {
/*  50 */     this.mapSleepDuration = 100L;
/*  51 */     this.reduceSleepDuration = 100L;
/*  52 */     this.mapSleepCount = 1;
/*  53 */     this.reduceSleepCount = 1;
/*  54 */     this.count = 0;
/*     */   }
/*     */   public int getPartition(IntWritable k, NullWritable v, int numPartitions) {
/*  57 */     return k.get() % numPartitions;
/*     */   }
/*     */ 
/*     */   public void map(IntWritable key, IntWritable value, OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
/*     */     throws IOException
/*     */   {
/*     */     try
/*     */     {
/* 117 */       reporter.setStatus("Sleeping... (" + this.mapSleepDuration * (this.mapSleepCount - this.count) + ") ms left");
/*     */ 
/* 119 */       Thread.sleep(this.mapSleepDuration);
/*     */     }
/*     */     catch (InterruptedException ex) {
/* 122 */       throw ((IOException)new IOException("Interrupted while sleeping").initCause(ex));
/*     */     }
/*     */ 
/* 125 */     this.count += 1;
/*     */ 
/* 128 */     int k = key.get();
/* 129 */     for (int i = 0; i < value.get(); i++)
/* 130 */       output.collect(new IntWritable(k + i), NullWritable.get());
/*     */   }
/*     */ 
/*     */   public void reduce(IntWritable key, Iterator<NullWritable> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
/*     */     throws IOException
/*     */   {
/*     */     try
/*     */     {
/* 138 */       reporter.setStatus("Sleeping... (" + this.reduceSleepDuration * (this.reduceSleepCount - this.count) + ") ms left");
/*     */ 
/* 140 */       Thread.sleep(this.reduceSleepDuration);
/*     */     }
/*     */     catch (InterruptedException ex)
/*     */     {
/* 144 */       throw ((IOException)new IOException("Interrupted while sleeping").initCause(ex));
/*     */     }
/*     */ 
/* 147 */     this.count += 1;
/*     */   }
/*     */ 
/*     */   public void configure(JobConf job) {
/* 151 */     this.mapSleepCount = job.getInt("sleep.job.map.sleep.count", this.mapSleepCount);
/*     */ 
/* 153 */     this.reduceSleepCount = job.getInt("sleep.job.reduce.sleep.count", this.reduceSleepCount);
/*     */ 
/* 155 */     this.mapSleepDuration = (job.getLong("sleep.job.map.sleep.time", 100L) / this.mapSleepCount);
/*     */ 
/* 157 */     this.reduceSleepDuration = (job.getLong("sleep.job.reduce.sleep.time", 100L) / this.reduceSleepCount);
/*     */   }
/*     */ 
/*     */   public void close() throws IOException
/*     */   {
/*     */   }
/*     */ 
/*     */   public static void main(String[] args) throws Exception {
/* 165 */     int res = ToolRunner.run(new Configuration(), new SleepJob(), args);
/* 166 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   public int run(int numMapper, int numReducer, long mapSleepTime, int mapSleepCount, long reduceSleepTime, int reduceSleepCount)
/*     */     throws IOException
/*     */   {
/* 172 */     JobConf job = setupJobConf(numMapper, numReducer, mapSleepTime, mapSleepCount, reduceSleepTime, reduceSleepCount);
/*     */ 
/* 174 */     JobClient.runJob(job);
/* 175 */     return 0;
/*     */   }
/*     */ 
/*     */   public JobConf setupJobConf(int numMapper, int numReducer, long mapSleepTime, int mapSleepCount, long reduceSleepTime, int reduceSleepCount)
/*     */   {
/* 181 */     JobConf job = new JobConf(getConf(), SleepJob.class);
/* 182 */     job.setNumMapTasks(numMapper);
/* 183 */     job.setNumReduceTasks(numReducer);
/* 184 */     job.setMapperClass(SleepJob.class);
/* 185 */     job.setMapOutputKeyClass(IntWritable.class);
/* 186 */     job.setMapOutputValueClass(NullWritable.class);
/* 187 */     job.setReducerClass(SleepJob.class);
/* 188 */     job.setOutputFormat(NullOutputFormat.class);
/* 189 */     job.setInputFormat(SleepInputFormat.class);
/* 190 */     job.setPartitionerClass(SleepJob.class);
/* 191 */     job.setSpeculativeExecution(false);
/* 192 */     job.setJobName("Sleep job");
/* 193 */     FileInputFormat.addInputPath(job, new Path("ignored"));
/* 194 */     job.setLong("sleep.job.map.sleep.time", mapSleepTime);
/* 195 */     job.setLong("sleep.job.reduce.sleep.time", reduceSleepTime);
/* 196 */     job.setInt("sleep.job.map.sleep.count", mapSleepCount);
/* 197 */     job.setInt("sleep.job.reduce.sleep.count", reduceSleepCount);
/* 198 */     return job;
/*     */   }
/*     */ 
/*     */   public int run(String[] args) throws Exception
/*     */   {
/* 203 */     if (args.length < 1) {
/* 204 */       System.err.println("SleepJob [-m numMapper] [-r numReducer] [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)] [-recordt recordSleepTime (msec)]");
/*     */ 
/* 207 */       ToolRunner.printGenericCommandUsage(System.err);
/* 208 */       return -1;
/*     */     }
/*     */ 
/* 211 */     int numMapper = 1; int numReducer = 1;
/* 212 */     long mapSleepTime = 100L; long reduceSleepTime = 100L; long recSleepTime = 100L;
/* 213 */     int mapSleepCount = 1; int reduceSleepCount = 1;
/*     */ 
/* 215 */     for (int i = 0; i < args.length; i++) {
/* 216 */       if (args[i].equals("-m")) {
/* 217 */         i++; numMapper = Integer.parseInt(args[i]);
/*     */       }
/* 219 */       else if (args[i].equals("-r")) {
/* 220 */         i++; numReducer = Integer.parseInt(args[i]);
/*     */       }
/* 222 */       else if (args[i].equals("-mt")) {
/* 223 */         i++; mapSleepTime = Long.parseLong(args[i]);
/*     */       }
/* 225 */       else if (args[i].equals("-rt")) {
/* 226 */         i++; reduceSleepTime = Long.parseLong(args[i]);
/*     */       }
/* 228 */       else if (args[i].equals("-recordt")) {
/* 229 */         i++; recSleepTime = Long.parseLong(args[i]);
/*     */       }
/*     */ 
/*     */     }
/*     */ 
/* 234 */     mapSleepCount = (int)Math.ceil(mapSleepTime / recSleepTime);
/* 235 */     reduceSleepCount = (int)Math.ceil(reduceSleepTime / recSleepTime);
/*     */ 
/* 237 */     return run(numMapper, numReducer, mapSleepTime, mapSleepCount, reduceSleepTime, reduceSleepCount);
/*     */   }
/*     */ 
/*     */   public static class SleepInputFormat extends Configured
/*     */     implements InputFormat<IntWritable, IntWritable>
/*     */   {
/*     */     public InputSplit[] getSplits(JobConf conf, int numSplits)
/*     */     {
/*  70 */       InputSplit[] ret = new InputSplit[numSplits];
/*  71 */       for (int i = 0; i < numSplits; i++) {
/*  72 */         ret[i] = new SleepJob.EmptySplit();
/*     */       }
/*  74 */       return ret;
/*     */     }
/*     */ 
/*     */     public RecordReader<IntWritable, IntWritable> getRecordReader(InputSplit ignored, JobConf conf, Reporter reporter) throws IOException
/*     */     {
/*  79 */       int count = conf.getInt("sleep.job.map.sleep.count", 1);
/*  80 */       if (count < 0) throw new IOException("Invalid map count: " + count);
/*  81 */       int redcount = conf.getInt("sleep.job.reduce.sleep.count", 1);
/*  82 */       if (redcount < 0)
/*  83 */         throw new IOException("Invalid reduce count: " + redcount);
/*  84 */       int emitPerMapTask = redcount * conf.getNumReduceTasks();
/*  85 */       return new RecordReader(emitPerMapTask, count) {
/*  86 */         private int records = 0;
/*  87 */         private int emitCount = 0;
/*     */ 
/*     */         public boolean next(IntWritable key, IntWritable value) throws IOException
/*     */         {
/*  91 */           key.set(this.emitCount);
/*  92 */           int emit = this.val$emitPerMapTask / this.val$count;
/*  93 */           if (this.val$emitPerMapTask % this.val$count > this.records) {
/*  94 */             emit++;
/*     */           }
/*  96 */           this.emitCount += emit;
/*  97 */           value.set(emit);
/*  98 */           return this.records++ < this.val$count;
/*     */         }
/* 100 */         public IntWritable createKey() { return new IntWritable(); } 
/* 101 */         public IntWritable createValue() { return new IntWritable(); } 
/* 102 */         public long getPos() throws IOException { return this.records; } 
/*     */         public void close() throws IOException {
/*     */         }
/* 105 */         public float getProgress() throws IOException { return this.records / this.val$count;
/*     */         }
/*     */       };
/*     */     }
/*     */   }
/*     */ 
/*     */   public static class EmptySplit
/*     */     implements InputSplit
/*     */   {
/*     */     public void write(DataOutput out)
/*     */       throws IOException
/*     */     {
/*     */     }
/*     */ 
/*     */     public void readFields(DataInput in)
/*     */       throws IOException
/*     */     {
/*     */     }
/*     */ 
/*     */     public long getLength()
/*     */     {
/*  63 */       return 0L; } 
/*  64 */     public String[] getLocations() { return new String[0];
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.SleepJob
 * JD-Core Version:    0.6.0
 */