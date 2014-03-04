/*    */ package com.haier.mr.terasort;
/*    */ 
/*    */ import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
/*    */ 
/*    */ public class TeraOutputFormat extends TextOutputFormat<Text, Text>
/*    */ {
/*    */   static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";
/*    */ 
/*    */   public static void setFinalSync(JobConf conf, boolean newValue)
/*    */   {
/* 43 */     conf.setBoolean("terasort.final.sync", newValue);
/*    */   }
/*    */ 
/*    */   public static boolean getFinalSync(JobConf conf)
/*    */   {
/* 50 */     return conf.getBoolean("terasort.final.sync", false);
/*    */   }
/*    */ 
/*    */   public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
/*    */     throws IOException
/*    */   {
/* 83 */     Path dir = getWorkOutputPath(job);
/* 84 */     FileSystem fs = dir.getFileSystem(job);
/* 85 */     FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
/* 86 */     return new TeraRecordWriter(fileOut, job);
/*    */   }
/*    */ 
/*    */   static class TeraRecordWriter extends TextOutputFormat.LineRecordWriter<Text, Text>
/*    */   {
/* 54 */     private static final byte[] newLine = "\r\n".getBytes();
/* 55 */     private boolean finalSync = false;
/*    */ 
/*    */     public TeraRecordWriter(DataOutputStream out, JobConf conf)
/*    */     {
/* 59 */       super(out);
/* 60 */       this.finalSync = TeraOutputFormat.getFinalSync(conf);
/*    */     }
/*    */ 
/*    */     public synchronized void write(Text key, Text value) throws IOException
/*    */     {
/* 65 */       this.out.write(key.getBytes(), 0, key.getLength());
/* 66 */       this.out.write(value.getBytes(), 0, value.getLength());
/* 67 */       this.out.write(newLine, 0, newLine.length);
/*    */     }
/*    */ 
/*    */     public void close() throws IOException {
/* 71 */       if (this.finalSync) {
/* 72 */         ((FSDataOutputStream)this.out).sync();
/*    */       }
/* 74 */       super.close(null);
/*    */     }
/*    */   }
/*    */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.terasort.TeraOutputFormat
 * JD-Core Version:    0.6.0
 */