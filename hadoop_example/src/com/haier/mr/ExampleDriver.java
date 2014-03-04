package com.haier.mr;
/*    */ 
/*    */ import org.apache.hadoop.examples.dancing.DistributedPentomino;
/*    */ import org.apache.hadoop.examples.dancing.Sudoku;
/*    */ import org.apache.hadoop.examples.terasort.TeraGen;
/*    */ import org.apache.hadoop.examples.terasort.TeraSort;
/*    */ import org.apache.hadoop.examples.terasort.TeraValidate;
/*    */ import org.apache.hadoop.util.ProgramDriver;
/*    */ 
/*    */ public class ExampleDriver
/*    */ {
/*    */   public static void main(String[] argv)
/*    */   {
/* 35 */     int exitCode = -1;
/* 36 */     ProgramDriver pgd = new ProgramDriver();
/*    */     try {
/* 38 */       pgd.addClass("wordcount", WordCount.class, "A map/reduce program that counts the words in the input files.");
/*    */ 
/* 40 */       pgd.addClass("aggregatewordcount", AggregateWordCount.class, "An Aggregate based map/reduce program that counts the words in the input files.");
/*    */ 
/* 42 */       pgd.addClass("aggregatewordhist", AggregateWordHistogram.class, "An Aggregate based map/reduce program that computes the histogram of the words in the input files.");
/*    */ 
/* 44 */       pgd.addClass("grep", Grep.class, "A map/reduce program that counts the matches of a regex in the input.");
/*    */ 
/* 46 */       pgd.addClass("randomwriter", RandomWriter.class, "A map/reduce program that writes 10GB of random data per node.");
/*    */ 
/* 48 */       pgd.addClass("randomtextwriter", RandomTextWriter.class, "A map/reduce program that writes 10GB of random textual data per node.");
/*    */ 
/* 50 */       pgd.addClass("sort", Sort.class, "A map/reduce program that sorts the data written by the random writer.");
/* 51 */       pgd.addClass("pi", PiEstimator.class, "A map/reduce program that estimates Pi using monte-carlo method.");
/* 52 */       pgd.addClass("pentomino", DistributedPentomino.class, "A map/reduce tile laying program to find solutions to pentomino problems.");
/*    */ 
/* 54 */       pgd.addClass("secondarysort", SecondarySort.class, "An example defining a secondary sort to the reduce.");
/*    */ 
/* 56 */       pgd.addClass("sudoku", Sudoku.class, "A sudoku solver.");
/* 57 */       pgd.addClass("sleep", SleepJob.class, "A job that sleeps at each map and reduce task.");
/* 58 */       pgd.addClass("join", Join.class, "A job that effects a join over sorted, equally partitioned datasets");
/* 59 */       pgd.addClass("multifilewc", MultiFileWordCount.class, "A job that counts words from several files.");
/* 60 */       pgd.addClass("dbcount", DBCountPageView.class, "An example job that count the pageview counts from a database.");
/* 61 */       pgd.addClass("teragen", TeraGen.class, "Generate data for the terasort");
/* 62 */       pgd.addClass("terasort", TeraSort.class, "Run the terasort");
/* 63 */       pgd.addClass("teravalidate", TeraValidate.class, "Checking results of terasort");
/* 64 */       pgd.driver(argv);
/*    */ 
/* 67 */       exitCode = 0;
/*    */     }
/*    */     catch (Throwable e) {
/* 70 */       e.printStackTrace();
/*    */     }
/*    */ 
/* 73 */     System.exit(exitCode);
/*    */   }
/*    */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.ExampleDriver
 * JD-Core Version:    0.6.0
 */