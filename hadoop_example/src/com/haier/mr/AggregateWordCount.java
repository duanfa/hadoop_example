package com.haier.mr;
/*    */ 
/*    */ import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorBaseDescriptor;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorJob;
/*    */ 
/*    */ public class AggregateWordCount
/*    */ {
/*    */   public static void main(String[] args)
/*    */     throws IOException
/*    */   {
/* 71 */     JobConf conf = ValueAggregatorJob.createValueAggregatorJob(args, new Class[] { WordCountPlugInClass.class });
/*    */ 
/* 74 */     JobClient.runJob(conf);
/*    */   }
/*    */ 
/*    */   public static class WordCountPlugInClass extends ValueAggregatorBaseDescriptor
/*    */   {
/*    */     public ArrayList<Map.Entry<Text, Text>> generateKeyValPairs(Object key, Object val)
/*    */     {
/* 48 */       String countType = "LongValueSum";
/* 49 */       ArrayList retv = new ArrayList();
/* 50 */       String line = val.toString();
/* 51 */       StringTokenizer itr = new StringTokenizer(line);
/* 52 */       while (itr.hasMoreTokens()) {
/* 53 */         Map.Entry e = generateEntry(countType, itr.nextToken(), ONE);
/* 54 */         if (e != null) {
/* 55 */           retv.add(e);
/*    */         }
/*    */       }
/* 58 */       return retv;
/*    */     }
/*    */   }
/*    */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.AggregateWordCount
 * JD-Core Version:    0.6.0
 */