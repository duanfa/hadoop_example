/*     */ package com.haier.mr.terasort;
/*     */ 
/*     */ import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
/*     */ 
/*     */ public class TeraInputFormat extends FileInputFormat<Text, Text>
/*     */ {
/*     */   static final String PARTITION_FILENAME = "_partition.lst";
/*     */   static final String SAMPLE_SIZE = "terasort.partitions.sample";
/*  50 */   private static JobConf lastConf = null;
/*  51 */   private static InputSplit[] lastResult = null;
/*     */ 
/*     */   public static void writePartitionFile(JobConf conf, Path partFile)
/*     */     throws IOException
/*     */   {
/* 110 */     TeraInputFormat inFormat = new TeraInputFormat();
/* 111 */     TextSampler sampler = new TextSampler();
/* 112 */     Text key = new Text();
/* 113 */     Text value = new Text();
/* 114 */     int partitions = conf.getNumReduceTasks();
/* 115 */     long sampleSize = conf.getLong("terasort.partitions.sample", 100000L);
/* 116 */     InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
/* 117 */     int samples = Math.min(10, splits.length);
/* 118 */     long recordsPerSample = sampleSize / samples;
/* 119 */     int sampleStep = splits.length / samples;
/* 120 */     long records = 0L;
/*     */ 
/* 122 */     for (int i = 0; i < samples; i++) {
/* 123 */       RecordReader reader = inFormat.getRecordReader(splits[(sampleStep * i)], conf, null);
/*     */ 
/* 125 */       while (reader.next(key, value)) {
/* 126 */         sampler.addKey(key);
/* 127 */         records += 1L;
/* 128 */         if ((i + 1) * recordsPerSample <= records) {
/* 129 */           break;
/*     */         }
/*     */       }
/*     */     }
/* 133 */     FileSystem outFs = partFile.getFileSystem(conf);
/* 134 */     if (outFs.exists(partFile)) {
/* 135 */       outFs.delete(partFile, false);
/*     */     }
/* 137 */     SequenceFile.Writer writer = SequenceFile.createWriter(outFs, conf, partFile, Text.class, NullWritable.class);
/*     */ 
/* 140 */     NullWritable nullValue = NullWritable.get();
/* 141 */     for (Text split : sampler.createPartitions(partitions)) {
/* 142 */       writer.append(split, nullValue);
/*     */     }
/* 144 */     writer.close();
/*     */   }
/*     */ 
/*     */   public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
/*     */     throws IOException
/*     */   {
/* 200 */     return new TeraRecordReader(job, (FileSplit)split);
/*     */   }
/*     */ 
/*     */   public InputSplit[] getSplits(JobConf conf, int splits) throws IOException
/*     */   {
/* 205 */     if (conf == lastConf) {
/* 206 */       return lastResult;
/*     */     }
/* 208 */     lastConf = conf;
/* 209 */     lastResult = super.getSplits(conf, splits);
/* 210 */     return lastResult;
/*     */   }
/*     */ 
/*     */   static class TeraRecordReader
/*     */     implements RecordReader<Text, Text>
/*     */   {
/*     */     private LineRecordReader in;
/* 149 */     private LongWritable junk = new LongWritable();
/* 150 */     private Text line = new Text();
/* 151 */     private static int KEY_LENGTH = 10;
/*     */ 
/*     */     public TeraRecordReader(Configuration job, FileSplit split) throws IOException
/*     */     {
/* 155 */       this.in = new LineRecordReader(job, split);
/*     */     }
/*     */ 
/*     */     public void close() throws IOException {
/* 159 */       this.in.close();
/*     */     }
/*     */ 
/*     */     public Text createKey() {
/* 163 */       return new Text();
/*     */     }
/*     */ 
/*     */     public Text createValue() {
/* 167 */       return new Text();
/*     */     }
/*     */ 
/*     */     public long getPos() throws IOException {
/* 171 */       return this.in.getPos();
/*     */     }
/*     */ 
/*     */     public float getProgress() throws IOException {
/* 175 */       return this.in.getProgress();
/*     */     }
/*     */ 
/*     */     public boolean next(Text key, Text value) throws IOException {
/* 179 */       if (this.in.next(this.junk, this.line)) {
/* 180 */         if (this.line.getLength() < KEY_LENGTH) {
/* 181 */           key.set(this.line);
/* 182 */           value.clear();
/*     */         } else {
/* 184 */           byte[] bytes = this.line.getBytes();
/* 185 */           key.set(bytes, 0, KEY_LENGTH);
/* 186 */           value.set(bytes, KEY_LENGTH, this.line.getLength() - KEY_LENGTH);
/*     */         }
/* 188 */         return true;
/*     */       }
/* 190 */       return false;
/*     */     }
/*     */   }
/*     */ 
/*     */   static class TextSampler
/*     */     implements IndexedSortable
/*     */   {
/*  54 */     private ArrayList<Text> records = new ArrayList();
/*     */ 
/*     */     public int compare(int i, int j) {
/*  57 */       Text left = (Text)this.records.get(i);
/*  58 */       Text right = (Text)this.records.get(j);
/*  59 */       return left.compareTo(right);
/*     */     }
/*     */ 
/*     */     public void swap(int i, int j) {
/*  63 */       Text left = (Text)this.records.get(i);
/*  64 */       Text right = (Text)this.records.get(j);
/*  65 */       this.records.set(j, left);
/*  66 */       this.records.set(i, right);
/*     */     }
/*     */ 
/*     */     public void addKey(Text key) {
/*  70 */       this.records.add(new Text(key));
/*     */     }
/*     */ 
/*     */     Text[] createPartitions(int numPartitions)
/*     */     {
/*  81 */       int numRecords = this.records.size();
/*  82 */       System.out.println("Making " + numPartitions + " from " + numRecords + " records");
/*     */ 
/*  84 */       if (numPartitions > numRecords) {
/*  85 */         throw new IllegalArgumentException("Requested more partitions than input keys (" + numPartitions + " > " + numRecords + ")");
/*     */       }
/*     */ 
/*  89 */       new QuickSort().sort(this, 0, this.records.size());
/*  90 */       float stepSize = numRecords / numPartitions;
/*  91 */       System.out.println("Step size is " + stepSize);
/*  92 */       Text[] result = new Text[numPartitions - 1];
/*  93 */       for (int i = 1; i < numPartitions; i++) {
/*  94 */         result[(i - 1)] = ((Text)this.records.get(Math.round(stepSize * i)));
/*     */       }
/*  96 */       return result;
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.terasort.TeraInputFormat
 * JD-Core Version:    0.6.0
 */