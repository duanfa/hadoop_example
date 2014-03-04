/*     */ package com.haier.mr.terasort;
/*     */ 
/*     */ import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class TeraSort extends Configured
/*     */   implements Tool
/*     */ {
/*  51 */   private static final Log LOG = LogFactory.getLog(TeraSort.class);
/*     */ 
/*     */   public int run(String[] args)
/*     */     throws Exception
/*     */   {
/* 227 */     LOG.info("starting");
/* 228 */     JobConf job = (JobConf)getConf();
/* 229 */     Path inputDir = new Path(args[0]);
/* 230 */     inputDir = inputDir.makeQualified(inputDir.getFileSystem(job));
/* 231 */     Path partitionFile = new Path(inputDir, "_partition.lst");
/* 232 */     URI partitionUri = new URI(partitionFile.toString() + "#" + "_partition.lst");
/*     */ 
/* 234 */     TeraInputFormat.setInputPaths(job, new Path[] { new Path(args[0]) });
/* 235 */     FileOutputFormat.setOutputPath(job, new Path(args[1]));
/* 236 */     job.setJobName("TeraSort");
/* 237 */     job.setJarByClass(TeraSort.class);
/* 238 */     job.setOutputKeyClass(Text.class);
/* 239 */     job.setOutputValueClass(Text.class);
/* 240 */     job.setInputFormat(TeraInputFormat.class);
/* 241 */     job.setOutputFormat(TeraOutputFormat.class);
/* 242 */     job.setPartitionerClass(TotalOrderPartitioner.class);
/* 243 */     TeraInputFormat.writePartitionFile(job, partitionFile);
/* 244 */     DistributedCache.addCacheFile(partitionUri, job);
/* 245 */     DistributedCache.createSymlink(job);
/* 246 */     job.setInt("dfs.replication", 1);
/* 247 */     TeraOutputFormat.setFinalSync(job, true);
/* 248 */     JobClient.runJob(job);
/* 249 */     LOG.info("done");
/* 250 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */     throws Exception
/*     */   {
/* 257 */     int res = ToolRunner.run(new JobConf(), new TeraSort(), args);
/* 258 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   static class TotalOrderPartitioner
/*     */     implements Partitioner<Text, Text>
/*     */   {
/*     */     private TrieNode trie;
/*     */     private Text[] splitPoints;
/*     */ 
/*     */     private static Text[] readPartitions(FileSystem fs, Path p, JobConf job)
/*     */       throws IOException
/*     */     {
/* 153 */       SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, job);
/* 154 */       List parts = new ArrayList();
/* 155 */       Text key = new Text();
/* 156 */       NullWritable value = NullWritable.get();
/* 157 */       while (reader.next(key, value)) {
/* 158 */         parts.add(key);
/* 159 */         key = new Text();
/*     */       }
/* 161 */       reader.close();
/* 162 */       return (Text[])parts.toArray(new Text[parts.size()]);
/*     */     }
/*     */ 
/*     */     private static TrieNode buildTrie(Text[] splits, int lower, int upper, Text prefix, int maxDepth)
/*     */     {
/* 177 */       int depth = prefix.getLength();
/* 178 */       if ((depth >= maxDepth) || (lower == upper)) {
/* 179 */         return new LeafTrieNode(depth, splits, lower, upper);
/*     */       }
/* 181 */       InnerTrieNode result = new InnerTrieNode(depth);
/* 182 */       Text trial = new Text(prefix);
/*     */ 
/* 184 */       trial.append(new byte[1], 0, 1);
/* 185 */       int currentBound = lower;
/* 186 */       for (int ch = 0; ch < 255; ch++) {
/* 187 */         trial.getBytes()[depth] = (byte)(ch + 1);
/* 188 */         lower = currentBound;
/* 189 */         while ((currentBound < upper) && 
/* 190 */           (splits[currentBound].compareTo(trial) < 0))
/*     */         {
/* 193 */           currentBound++;
/*     */         }
/* 195 */         trial.getBytes()[depth] = (byte)ch;
/* 196 */         result.child[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
/*     */       }
/*     */ 
/* 200 */       trial.getBytes()[depth] = 127;
/* 201 */       result.child['ÿ'] = buildTrie(splits, currentBound, upper, trial, maxDepth);
/*     */ 
/* 203 */       return result;
/*     */     }
/*     */ 
/*     */     public void configure(JobConf job) {
/*     */       try {
/* 208 */         FileSystem fs = FileSystem.getLocal(job);
/* 209 */         Path partFile = new Path("_partition.lst");
/* 210 */         this.splitPoints = readPartitions(fs, partFile, job);
/* 211 */         this.trie = buildTrie(this.splitPoints, 0, this.splitPoints.length, new Text(), 2);
/*     */       } catch (IOException ie) {
/* 213 */         throw new IllegalArgumentException("can't read paritions file", ie);
/*     */       }
/*     */     }
/*     */ 
/*     */     public int getPartition(Text key, Text value, int numPartitions)
/*     */     {
/* 221 */       return this.trie.findPartition(key);
/*     */     }
/*     */ 
/*     */     static class LeafTrieNode extends TeraSort.TotalOrderPartitioner.TrieNode
/*     */     {
/*     */       int lower;
/*     */       int upper;
/*     */       Text[] splitPoints;
/*     */ 
/*     */       LeafTrieNode(int level, Text[] splitPoints, int lower, int upper)
/*     */       {
/* 119 */         super(upper);
/* 120 */         this.splitPoints = splitPoints;
/* 121 */         this.lower = lower;
/* 122 */         this.upper = upper;
/*     */       }
/*     */       int findPartition(Text key) {
/* 125 */         for (int i = this.lower; i < this.upper; i++) {
/* 126 */           if (this.splitPoints[i].compareTo(key) >= 0) {
/* 127 */             return i;
/*     */           }
/*     */         }
/* 130 */         return this.upper;
/*     */       }
/*     */       void print(PrintStream strm) throws IOException {
/* 133 */         for (int i = 0; i < 2 * getLevel(); i++) {
/* 134 */           strm.print(' ');
/*     */         }
/* 136 */         strm.print(this.lower);
/* 137 */         strm.print(", ");
/* 138 */         strm.println(this.upper);
/*     */       }
/*     */     }
/*     */ 
/*     */     static class InnerTrieNode extends TeraSort.TotalOrderPartitioner.TrieNode
/*     */     {
/*  81 */       private TeraSort.TotalOrderPartitioner.TrieNode[] child = new TeraSort.TotalOrderPartitioner.TrieNode[256];
/*     */ 
/*     */       InnerTrieNode(int level) {
/*  84 */         super(level);
/*     */       }
/*     */       int findPartition(Text key) {
/*  87 */         int level = getLevel();
/*  88 */         if (key.getLength() <= level) {
/*  89 */           return this.child[0].findPartition(key);
/*     */         }
/*  91 */         return this.child[key.getBytes()[level]].findPartition(key);
/*     */       }
/*     */       void setChild(int idx, TeraSort.TotalOrderPartitioner.TrieNode child) {
/*  94 */         this.child[idx] = child;
/*     */       }
/*     */       void print(PrintStream strm) throws IOException {
/*  97 */         for (int ch = 0; ch < 255; ch++) {
/*  98 */           for (int i = 0; i < 2 * getLevel(); i++) {
/*  99 */             strm.print(' ');
/*     */           }
/* 101 */           strm.print(ch);
/* 102 */           strm.println(" ->");
/* 103 */           if (this.child[ch] != null)
/* 104 */             this.child[ch].print(strm);
/*     */         }
/*     */       }
/*     */     }
/*     */ 
/*     */     static abstract class TrieNode
/*     */     {
/*     */       private int level;
/*     */ 
/*     */       TrieNode(int level)
/*     */       {
/*  67 */         this.level = level; } 
/*     */       abstract int findPartition(Text paramText);
/*     */ 
/*     */       abstract void print(PrintStream paramPrintStream) throws IOException;
/*     */ 
/*  72 */       int getLevel() { return this.level;
/*     */       }
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.terasort.TeraSort
 * JD-Core Version:    0.6.0
 */