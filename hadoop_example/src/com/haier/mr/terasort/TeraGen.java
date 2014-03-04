/*     */ package com.haier.mr.terasort;
/*     */ 
/*     */ import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class TeraGen extends Configured
/*     */   implements Tool
/*     */ {
/*     */   static long getNumberOfRows(JobConf job)
/*     */   {
/* 179 */     return job.getLong("terasort.num-rows", 0L);
/*     */   }
/*     */ 
/*     */   static void setNumberOfRows(JobConf job, long numRows) {
/* 183 */     job.setLong("terasort.num-rows", numRows);
/*     */   }
/*     */ 
/*     */   public int run(String[] args)
/*     */     throws IOException
/*     */   {
/* 341 */     JobConf job = (JobConf)getConf();
/* 342 */     setNumberOfRows(job, Long.parseLong(args[0]));
/* 343 */     FileOutputFormat.setOutputPath(job, new Path(args[1]));
/* 344 */     job.setJobName("TeraGen");
/* 345 */     job.setJarByClass(TeraGen.class);
/* 346 */     job.setMapperClass(SortGenMapper.class);
/* 347 */     job.setNumReduceTasks(0);
/* 348 */     job.setOutputKeyClass(Text.class);
/* 349 */     job.setOutputValueClass(Text.class);
/* 350 */     job.setInputFormat(RangeInputFormat.class);
/* 351 */     job.setOutputFormat(TeraOutputFormat.class);
/* 352 */     JobClient.runJob(job);
/* 353 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args) throws Exception {
/* 357 */     int res = ToolRunner.run(new JobConf(), new TeraGen(), args);
/* 358 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   public static class SortGenMapper extends MapReduceBase
/*     */     implements Mapper<LongWritable, NullWritable, Text, Text>
/*     */   {
/*     */     private Text key;
/*     */     private Text value;
/*     */     private TeraGen.RandomGenerator rand;
/*     */     private byte[] keyBytes;
/*     */     private byte[] spaces;
/*     */     private byte[][] filler;
/*     */ 
/*     */     public SortGenMapper()
/*     */     {
/* 261 */       this.key = new Text();
/* 262 */       this.value = new Text();
/*     */ 
/* 264 */       this.keyBytes = new byte[12];
/* 265 */       this.spaces = "          ".getBytes();
/* 266 */       this.filler = new byte[26][];
/*     */ 
/* 268 */       for (int i = 0; i < 26; i++) {
/* 269 */         this.filler[i] = new byte[10];
/* 270 */         for (int j = 0; j < 10; j++)
/* 271 */           this.filler[i][j] = (byte)(65 + i);
/*     */       }
/*     */     }
/*     */ 
/*     */     private void addKey()
/*     */     {
/* 281 */       for (int i = 0; i < 3; i++) {
/* 282 */         long temp = this.rand.next() / 52L;
/* 283 */         this.keyBytes[(3 + 4 * i)] = (byte)(int)(32L + temp % 95L);
/* 284 */         temp /= 95L;
/* 285 */         this.keyBytes[(2 + 4 * i)] = (byte)(int)(32L + temp % 95L);
/* 286 */         temp /= 95L;
/* 287 */         this.keyBytes[(1 + 4 * i)] = (byte)(int)(32L + temp % 95L);
/* 288 */         temp /= 95L;
/* 289 */         this.keyBytes[(4 * i)] = (byte)(int)(32L + temp % 95L);
/*     */       }
/* 291 */       this.key.set(this.keyBytes, 0, 10);
/*     */     }
/*     */ 
/*     */     private void addRowId(long rowId)
/*     */     {
/* 299 */       byte[] rowid = Integer.toString((int)rowId).getBytes();
/* 300 */       int padSpace = 10 - rowid.length;
/* 301 */       if (padSpace > 0) {
/* 302 */         this.value.append(this.spaces, 0, 10 - rowid.length);
/*     */       }
/* 304 */       this.value.append(rowid, 0, Math.min(rowid.length, 10));
/*     */     }
/*     */ 
/*     */     private void addFiller(long rowId)
/*     */     {
/* 313 */       int base = (int)(rowId * 8L % 26L);
/* 314 */       for (int i = 0; i < 7; i++) {
/* 315 */         this.value.append(this.filler[((base + i) % 26)], 0, 10);
/*     */       }
/* 317 */       this.value.append(this.filler[((base + 7) % 26)], 0, 8);
/*     */     }
/*     */ 
/*     */     public void map(LongWritable row, NullWritable ignored, OutputCollector<Text, Text> output, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 323 */       long rowId = row.get();
/* 324 */       if (this.rand == null)
/*     */       {
/* 326 */         this.rand = new TeraGen.RandomGenerator(rowId * 3L);
/*     */       }
/* 328 */       addKey();
/* 329 */       this.value.clear();
/* 330 */       addRowId(rowId);
/* 331 */       addFiller(rowId);
/* 332 */       output.collect(this.key, this.value);
/*     */     }
/*     */   }
/*     */ 
/*     */   static class RandomGenerator
/*     */   {
/* 187 */     private long seed = 0L;
/*     */     private static final long mask32 = 4294967295L;
/*     */     private static final int seedSkip = 134217728;
/* 198 */     private static final long[] seeds = { 0L, 4160749568L, 4026531840L, 3892314112L, 3758096384L, 3623878656L, 3489660928L, 3355443200L, 3221225472L, 3087007744L, 2952790016L, 2818572288L, 2684354560L, 2550136832L, 2415919104L, 2281701376L, 2147483648L, 2013265920L, 1879048192L, 1744830464L, 1610612736L, 1476395008L, 1342177280L, 1207959552L, 1073741824L, 939524096L, 805306368L, 671088640L, 536870912L, 402653184L, 268435456L, 134217728L };
/*     */ 
/*     */     RandomGenerator(long initalIteration)
/*     */     {
/* 237 */       int baseIndex = (int)((initalIteration & 0xFFFFFFFF) / 134217728L);
/* 238 */       this.seed = seeds[baseIndex];
/* 239 */       for (int i = 0; i < initalIteration % 134217728L; i++)
/* 240 */         next();
/*     */     }
/*     */ 
/*     */     RandomGenerator()
/*     */     {
/* 245 */       this(0L);
/*     */     }
/*     */ 
/*     */     long next() {
/* 249 */       this.seed = (this.seed * 3141592621L + 663896637L & 0xFFFFFFFF);
/* 250 */       return this.seed;
/*     */     }
/*     */   }
/*     */ 
/*     */   static class RangeInputFormat
/*     */     implements InputFormat<LongWritable, NullWritable>
/*     */   {
/*     */     public RecordReader<LongWritable, NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 152 */       return new RangeRecordReader((RangeInputSplit)split);
/*     */     }
/*     */ 
/*     */     public InputSplit[] getSplits(JobConf job, int numSplits)
/*     */     {
/* 161 */       long totalRows = TeraGen.getNumberOfRows(job);
/* 162 */       long rowsPerSplit = totalRows / numSplits;
/* 163 */       System.out.println("Generating " + totalRows + " using " + numSplits + " maps with step of " + rowsPerSplit);
/*     */ 
/* 165 */       InputSplit[] splits = new InputSplit[numSplits];
/* 166 */       long currentRow = 0L;
/* 167 */       for (int split = 0; split < numSplits - 1; split++) {
/* 168 */         splits[split] = new RangeInputSplit(currentRow, rowsPerSplit);
/* 169 */         currentRow += rowsPerSplit;
/*     */       }
/* 171 */       splits[(numSplits - 1)] = new RangeInputSplit(currentRow, totalRows - currentRow);
/*     */ 
/* 173 */       return splits;
/*     */     }
/*     */ 
/*     */     static class RangeRecordReader
/*     */       implements RecordReader<LongWritable, NullWritable>
/*     */     {
/*     */       long startRow;
/*     */       long finishedRows;
/*     */       long totalRows;
/*     */ 
/*     */       public RangeRecordReader(TeraGen.RangeInputFormat.RangeInputSplit split)
/*     */       {
/* 111 */         this.startRow = split.firstRow;
/* 112 */         this.finishedRows = 0L;
/* 113 */         this.totalRows = split.rowCount;
/*     */       }
/*     */ 
/*     */       public void close() throws IOException
/*     */       {
/*     */       }
/*     */ 
/*     */       public LongWritable createKey() {
/* 121 */         return new LongWritable();
/*     */       }
/*     */ 
/*     */       public NullWritable createValue() {
/* 125 */         return NullWritable.get();
/*     */       }
/*     */ 
/*     */       public long getPos() throws IOException {
/* 129 */         return this.finishedRows;
/*     */       }
/*     */ 
/*     */       public float getProgress() throws IOException {
/* 133 */         return (float)this.finishedRows / (float)this.totalRows;
/*     */       }
/*     */ 
/*     */       public boolean next(LongWritable key, NullWritable value)
/*     */       {
/* 138 */         if (this.finishedRows < this.totalRows) {
/* 139 */           key.set(this.startRow + this.finishedRows);
/* 140 */           this.finishedRows += 1L;
/* 141 */           return true;
/*     */         }
/* 143 */         return false;
/*     */       }
/*     */     }
/*     */ 
/*     */     static class RangeInputSplit
/*     */       implements InputSplit
/*     */     {
/*     */       long firstRow;
/*     */       long rowCount;
/*     */ 
/*     */       public RangeInputSplit()
/*     */       {
/*     */       }
/*     */ 
/*     */       public RangeInputSplit(long offset, long length)
/*     */       {
/*  78 */         this.firstRow = offset;
/*  79 */         this.rowCount = length;
/*     */       }
/*     */ 
/*     */       public long getLength() throws IOException {
/*  83 */         return 0L;
/*     */       }
/*     */ 
/*     */       public String[] getLocations() throws IOException {
/*  87 */         return new String[0];
/*     */       }
/*     */ 
/*     */       public void readFields(DataInput in) throws IOException {
/*  91 */         this.firstRow = WritableUtils.readVLong(in);
/*  92 */         this.rowCount = WritableUtils.readVLong(in);
/*     */       }
/*     */ 
/*     */       public void write(DataOutput out) throws IOException {
/*  96 */         WritableUtils.writeVLong(out, this.firstRow);
/*  97 */         WritableUtils.writeVLong(out, this.rowCount);
/*     */       }
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.terasort.TeraGen
 * JD-Core Version:    0.6.0
 */