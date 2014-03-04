 package com.haier.mr;
/*     */ 
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.util.Date;
/*     */ import java.util.Random;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.conf.Configured;
/*     */ import org.apache.hadoop.fs.Path;
/*     */ import org.apache.hadoop.io.BytesWritable;
/*     */ import org.apache.hadoop.io.Text;
/*     */ import org.apache.hadoop.io.Writable;
/*     */ import org.apache.hadoop.io.WritableComparable;
/*     */ import org.apache.hadoop.mapred.ClusterStatus;
/*     */ import org.apache.hadoop.mapred.FileOutputFormat;
/*     */ import org.apache.hadoop.mapred.FileSplit;
/*     */ import org.apache.hadoop.mapred.InputFormat;
/*     */ import org.apache.hadoop.mapred.InputSplit;
/*     */ import org.apache.hadoop.mapred.JobClient;
/*     */ import org.apache.hadoop.mapred.JobConf;
/*     */ import org.apache.hadoop.mapred.MapReduceBase;
/*     */ import org.apache.hadoop.mapred.Mapper;
/*     */ import org.apache.hadoop.mapred.OutputCollector;
/*     */ import org.apache.hadoop.mapred.RecordReader;
/*     */ import org.apache.hadoop.mapred.Reporter;
/*     */ import org.apache.hadoop.mapred.SequenceFileOutputFormat;
/*     */ import org.apache.hadoop.mapred.lib.IdentityReducer;
/*     */ import org.apache.hadoop.util.Tool;
/*     */ import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class RandomWriter extends Configured
/*     */   implements Tool
/*     */ {
/*     */   public int run(String[] args)
/*     */     throws Exception
/*     */   {
/* 226 */     if (args.length == 0) {
/* 227 */       System.out.println("Usage: writer <out-dir>");
/* 228 */       ToolRunner.printGenericCommandUsage(System.out);
/* 229 */       return -1;
/*     */     }
/*     */ 
/* 232 */     Path outDir = new Path(args[0]);
/* 233 */     JobConf job = new JobConf(getConf());
/*     */ 
/* 235 */     job.setJarByClass(RandomWriter.class);
/* 236 */     job.setJobName("random-writer");
/* 237 */     FileOutputFormat.setOutputPath(job, outDir);
/*     */ 
/* 239 */     job.setOutputKeyClass(BytesWritable.class);
/* 240 */     job.setOutputValueClass(BytesWritable.class);
/*     */ 
/* 242 */     job.setInputFormat(RandomInputFormat.class);
/* 243 */     job.setMapperClass(Map.class);
/* 244 */     job.setReducerClass(IdentityReducer.class);
/* 245 */     job.setOutputFormat(SequenceFileOutputFormat.class);
/*     */ 
/* 247 */     JobClient client = new JobClient(job);
/* 248 */     ClusterStatus cluster = client.getClusterStatus();
/* 249 */     int numMapsPerHost = job.getInt("test.randomwriter.maps_per_host", 10);
/* 250 */     long numBytesToWritePerMap = job.getLong("test.randomwrite.bytes_per_map", 1073741824L);
/*     */ 
/* 252 */     if (numBytesToWritePerMap == 0L) {
/* 253 */       System.err.println("Cannot have test.randomwrite.bytes_per_map set to 0");
/* 254 */       return -2;
/*     */     }
/* 256 */     long totalBytesToWrite = job.getLong("test.randomwrite.total_bytes", numMapsPerHost * numBytesToWritePerMap * cluster.getTaskTrackers());
/*     */ 
/* 258 */     int numMaps = (int)(totalBytesToWrite / numBytesToWritePerMap);
/* 259 */     if ((numMaps == 0) && (totalBytesToWrite > 0L)) {
/* 260 */       numMaps = 1;
/* 261 */       job.setLong("test.randomwrite.bytes_per_map", totalBytesToWrite);
/*     */     }
/*     */ 
/* 264 */     job.setNumMapTasks(numMaps);
/* 265 */     System.out.println("Running " + numMaps + " maps.");
/*     */ 
/* 268 */     job.setNumReduceTasks(0);
/*     */ 
/* 270 */     Date startTime = new Date();
/* 271 */     System.out.println("Job started: " + startTime);
/* 272 */     JobClient.runJob(job);
/* 273 */     Date endTime = new Date();
/* 274 */     System.out.println("Job ended: " + endTime);
/* 275 */     System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) / 1000L + " seconds.");
/*     */ 
/* 279 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args) throws Exception {
/* 283 */     int res = ToolRunner.run(new Configuration(), new RandomWriter(), args);
/* 284 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   static class Map extends MapReduceBase
/*     */     implements Mapper<WritableComparable, Writable, BytesWritable, BytesWritable>
/*     */   {
/*     */     private long numBytesToWrite;
/*     */     private int minKeySize;
/*     */     private int keySizeRange;
/*     */     private int minValueSize;
/*     */     private int valueSizeRange;
/* 161 */     private Random random = new Random();
/* 162 */     private BytesWritable randomKey = new BytesWritable();
/* 163 */     private BytesWritable randomValue = new BytesWritable();
/*     */ 
/*     */     private void randomizeBytes(byte[] data, int offset, int length) {
/* 166 */       for (int i = offset + length - 1; i >= offset; i--)
/* 167 */         data[i] = (byte)this.random.nextInt(256);
/*     */     }
/*     */ 
/*     */     public void map(WritableComparable key, Writable value, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 178 */       int itemCount = 0;
/* 179 */       while (this.numBytesToWrite > 0L) {
/* 180 */         int keyLength = this.minKeySize + (this.keySizeRange != 0 ? this.random.nextInt(this.keySizeRange) : 0);
/*     */ 
/* 182 */         this.randomKey.setSize(keyLength);
/* 183 */         randomizeBytes(this.randomKey.getBytes(), 0, this.randomKey.getLength());
/* 184 */         int valueLength = this.minValueSize + (this.valueSizeRange != 0 ? this.random.nextInt(this.valueSizeRange) : 0);
/*     */ 
/* 186 */         this.randomValue.setSize(valueLength);
/* 187 */         randomizeBytes(this.randomValue.getBytes(), 0, this.randomValue.getLength());
/* 188 */         output.collect(this.randomKey, this.randomValue);
/* 189 */         this.numBytesToWrite -= keyLength + valueLength;
/* 190 */         reporter.incrCounter(RandomWriter.Counters.BYTES_WRITTEN, keyLength + valueLength);
/* 191 */         reporter.incrCounter(RandomWriter.Counters.RECORDS_WRITTEN, 1L);
/* 192 */         itemCount++; if (itemCount % 200 == 0) {
/* 193 */           reporter.setStatus("wrote record " + itemCount + ". " + this.numBytesToWrite + " bytes left.");
/*     */         }
/*     */       }
/*     */ 
/* 197 */       reporter.setStatus("done with " + itemCount + " records.");
/*     */     }
/*     */ 
/*     */     public void configure(JobConf job)
/*     */     {
/* 206 */       this.numBytesToWrite = job.getLong("test.randomwrite.bytes_per_map", 1073741824L);
/*     */ 
/* 208 */       this.minKeySize = job.getInt("test.randomwrite.min_key", 10);
/* 209 */       this.keySizeRange = (job.getInt("test.randomwrite.max_key", 1000) - this.minKeySize);
/*     */ 
/* 211 */       this.minValueSize = job.getInt("test.randomwrite.min_value", 0);
/* 212 */       this.valueSizeRange = (job.getInt("test.randomwrite.max_value", 20000) - this.minValueSize);
/*     */     }
/*     */   }
/*     */ 
/*     */   static class RandomInputFormat
/*     */     implements InputFormat<Text, Text>
/*     */   {
/*     */     public InputSplit[] getSplits(JobConf job, int numSplits)
/*     */       throws IOException
/*     */     {
/* 104 */       InputSplit[] result = new InputSplit[numSplits];
/* 105 */       Path outDir = FileOutputFormat.getOutputPath(job);
/* 106 */       for (int i = 0; i < result.length; i++) {
/* 107 */         result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i), 0L, 1L, (String[])null);
/*     */       }
/*     */ 
/* 110 */       return result;
/*     */     }
/*     */ 
/*     */     public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 148 */       return new RandomRecordReader(((FileSplit)split).getPath());
/*     */     }
/*     */ 
/*     */     static class RandomRecordReader
/*     */       implements RecordReader<Text, Text>
/*     */     {
/*     */       Path name;
/*     */ 
/*     */       public RandomRecordReader(Path p)
/*     */       {
/* 120 */         this.name = p;
/*     */       }
/*     */       public boolean next(Text key, Text value) {
/* 123 */         if (this.name != null) {
/* 124 */           key.set(this.name.getName());
/* 125 */           this.name = null;
/* 126 */           return true;
/*     */         }
/* 128 */         return false;
/*     */       }
/*     */       public Text createKey() {
/* 131 */         return new Text();
/*     */       }
/*     */       public Text createValue() {
/* 134 */         return new Text();
/*     */       }
/*     */       public long getPos() {
/* 137 */         return 0L;
/*     */       }
/*     */       public void close() {
/*     */       }
/* 141 */       public float getProgress() { return 0.0F;
/*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   static enum Counters
/*     */   {
/*  90 */     RECORDS_WRITTEN, BYTES_WRITTEN;
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.RandomWriter
 * JD-Core Version:    0.6.0
 */