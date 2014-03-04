/*     */ package com.haier.mr.dancing;
/*     */ 
/*     */ import java.io.BufferedOutputStream;
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.util.List;
/*     */ import java.util.StringTokenizer;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.conf.Configured;
/*     */ import org.apache.hadoop.fs.FileSystem;
/*     */ import org.apache.hadoop.fs.Path;
/*     */ import org.apache.hadoop.io.Text;
/*     */ import org.apache.hadoop.io.WritableComparable;
/*     */ import org.apache.hadoop.mapred.FileInputFormat;
/*     */ import org.apache.hadoop.mapred.FileOutputFormat;
/*     */ import org.apache.hadoop.mapred.JobClient;
/*     */ import org.apache.hadoop.mapred.JobConf;
/*     */ import org.apache.hadoop.mapred.MapReduceBase;
/*     */ import org.apache.hadoop.mapred.Mapper;
/*     */ import org.apache.hadoop.mapred.OutputCollector;
/*     */ import org.apache.hadoop.mapred.Reporter;
/*     */ import org.apache.hadoop.mapred.lib.IdentityReducer;
/*     */ import org.apache.hadoop.util.ReflectionUtils;
/*     */ import org.apache.hadoop.util.StringUtils;
/*     */ import org.apache.hadoop.util.Tool;
/*     */ import org.apache.hadoop.util.ToolRunner;
/*     */ 
/*     */ public class DistributedPentomino extends Configured
/*     */   implements Tool
/*     */ {
/*     */   private static void createInputDirectory(FileSystem fs, Path dir, Pentomino pent, int depth)
/*     */     throws IOException
/*     */   {
/* 131 */     fs.mkdirs(dir);
/* 132 */     List<int[]> splits = pent.getSplits(depth);
/* 133 */     PrintStream file = new PrintStream(new BufferedOutputStream(fs.create(new Path(dir, "part1")), 65536));
/*     */ 
/* 136 */     for (int[] prefix : splits) {
/* 137 */       for (int i = 0; i < prefix.length; i++) {
/* 138 */         if (i != 0) {
/* 139 */           file.print(',');
/*     */         }
/* 141 */         file.print(prefix[i]);
/*     */       }
/* 143 */       file.print('\n');
/*     */     }
/* 145 */     file.close();
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */     throws Exception
/*     */   {
/* 154 */     int res = ToolRunner.run(new Configuration(), new DistributedPentomino(), args);
/* 155 */     System.exit(res);
/*     */   }
/*     */ 
/*     */   public int run(String[] args) throws Exception
/*     */   {
/* 160 */     int depth = 5;
/* 161 */     int width = 9;
/* 162 */     int height = 10;
/*     */ 
/* 164 */     if (args.length == 0) {
/* 165 */       System.out.println("pentomino <output>");
/* 166 */       ToolRunner.printGenericCommandUsage(System.out);
/* 167 */       return -1;
/*     */     }
/*     */ 
/* 170 */     JobConf conf = new JobConf(getConf());
/* 171 */     width = conf.getInt("pent.width", width);
/* 172 */     height = conf.getInt("pent.height", height);
/* 173 */     depth = conf.getInt("pent.depth", depth);
/* 174 */     Class pentClass = conf.getClass("pent.class", OneSidedPentomino.class, Pentomino.class);
/*     */ 
/* 176 */     Path output = new Path(args[0]);
/* 177 */     Path input = new Path(output + "_input");
/* 178 */     FileSystem fileSys = FileSystem.get(conf);
/*     */     try {
/* 180 */       FileInputFormat.setInputPaths(conf, new Path[] { input });
/* 181 */       FileOutputFormat.setOutputPath(conf, output);
/* 182 */       conf.setJarByClass(PentMap.class);
/*     */ 
/* 184 */       conf.setJobName("dancingElephant");
/* 185 */       Pentomino pent = (Pentomino)ReflectionUtils.newInstance(pentClass, conf);
/* 186 */       pent.initialize(width, height);
/* 187 */       createInputDirectory(fileSys, input, pent, depth);
/*     */ 
/* 190 */       conf.setOutputKeyClass(Text.class);
/*     */ 
/* 192 */       conf.setOutputValueClass(Text.class);
/*     */ 
/* 194 */       conf.setMapperClass(PentMap.class);
/* 195 */       conf.setReducerClass(IdentityReducer.class);
/*     */ 
/* 197 */       conf.setNumMapTasks(2000);
/* 198 */       conf.setNumReduceTasks(1);
/*     */ 
/* 200 */       JobClient.runJob(conf);
/*     */     } finally {
/* 202 */       fileSys.delete(input, true);
/*     */     }
/* 204 */     return 0;
/*     */   }
/*     */ 
/*     */   public static class PentMap extends MapReduceBase
/*     */     implements Mapper<WritableComparable, Text, Text, Text>
/*     */   {
/*     */     private int width;
/*     */     private int height;
/*     */     private int depth;
/*     */     private Pentomino pent;
/*     */     private Text prefixString;
/*     */     private OutputCollector<Text, Text> output;
/*     */     private Reporter reporter;
/*     */ 
/*     */     public void map(WritableComparable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
/*     */       throws IOException
/*     */     {
/*  91 */       this.output = output;
/*  92 */       this.reporter = reporter;
/*  93 */       this.prefixString = value;
/*  94 */       StringTokenizer itr = new StringTokenizer(this.prefixString.toString(), ",");
/*  95 */       int[] prefix = new int[this.depth];
/*  96 */       int idx = 0;
/*  97 */       while (itr.hasMoreTokens()) {
/*  98 */         String num = itr.nextToken();
/*  99 */         prefix[(idx++)] = Integer.parseInt(num);
/*     */       }
/* 101 */       this.pent.solve(prefix);
/*     */     }
/*     */ 
/*     */     public void configure(JobConf conf)
/*     */     {
/* 106 */       this.depth = conf.getInt("pent.depth", -1);
/* 107 */       this.width = conf.getInt("pent.width", -1);
/* 108 */       this.height = conf.getInt("pent.height", -1);
/* 109 */       this.pent = ((Pentomino)ReflectionUtils.newInstance(conf.getClass("pent.class", OneSidedPentomino.class), conf));
/*     */ 
/* 113 */       this.pent.initialize(this.width, this.height);
/* 114 */       this.pent.setPrinter(new SolutionCatcher());
/*     */     }
/*     */ 
/*     */     class SolutionCatcher
/*     */       implements DancingLinks.SolutionAcceptor<Pentomino.ColumnName>
/*     */     {
/*     */       SolutionCatcher()
/*     */       {
/*     */       }
/*     */ 
/*     */       public void solution(List<List<Pentomino.ColumnName>> answer)
/*     */       {
/*  73 */         String board = Pentomino.stringifySolution(DistributedPentomino.PentMap.this.width, DistributedPentomino.PentMap.this.height, answer);
/*     */         try {
/*  75 */           DistributedPentomino.PentMap.this.output.collect(DistributedPentomino.PentMap.this.prefixString, new Text("\n" + board));
/*  76 */           DistributedPentomino.PentMap.this.reporter.incrCounter(DistributedPentomino.PentMap.this.pent.getCategory(answer), 1L);
/*     */         } catch (IOException e) {
/*  78 */           System.err.println(StringUtils.stringifyException(e));
/*     */         }
/*     */       }
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.dancing.DistributedPentomino
 * JD-Core Version:    0.6.0
 */