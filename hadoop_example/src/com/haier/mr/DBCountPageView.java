package com.haier.mr;
/*     */ 
/*     */ import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.Server;
/*     */ 
/*     */ public class DBCountPageView extends Configured
/*     */   implements Tool
/*     */ {
/*  74 */   private static final Log LOG = LogFactory.getLog(DBCountPageView.class);
/*     */   private Connection connection;
/*     */   private boolean initialized;
/*  79 */   private static final String[] AccessFieldNames = { "url", "referrer", "time" };
/*  80 */   private static final String[] PageviewFieldNames = { "url", "pageview" };
/*     */   private static final String DB_URL = "jdbc:hsqldb:hsql://localhost/URLAccess";
/*     */   private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";
/*     */   private Server server;
/*     */ 
/*     */   public DBCountPageView()
/*     */   {
/*  77 */     this.initialized = false;
/*     */   }
/*     */ 
/*     */   private void startHsqldbServer()
/*     */   {
/*  88 */     this.server = new Server();
/*  89 */     this.server.setDatabasePath(0, System.getProperty("test.build.data", ".") + "/URLAccess");
/*     */ 
/*  91 */     this.server.setDatabaseName(0, "URLAccess");
/*  92 */     this.server.start();
/*     */   }
/*     */ 
/*     */   private void createConnection(String driverClassName, String url)
/*     */     throws Exception
/*     */   {
/*  98 */     Class.forName(driverClassName);
/*  99 */     this.connection = DriverManager.getConnection(url);
/* 100 */     this.connection.setAutoCommit(false);
/*     */   }
/*     */ 
/*     */   private void shutdown() {
/*     */     try {
/* 105 */       this.connection.commit();
/* 106 */       this.connection.close();
/*     */     } catch (Throwable ex) {
/* 108 */       LOG.warn("Exception occurred while closing connection :" + StringUtils.stringifyException(ex));
/*     */     }
/*     */     finally {
/*     */       try {
/* 112 */         if (this.server != null)
/* 113 */           this.server.shutdown();
/*     */       }
/*     */       catch (Throwable ex) {
/* 116 */         LOG.warn("Exception occurred while shutting down HSQLDB :" + StringUtils.stringifyException(ex));
/*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   private void initialize(String driverClassName, String url)
/*     */     throws Exception
/*     */   {
/* 124 */     if (!this.initialized) {
/* 125 */       if (driverClassName.equals("org.hsqldb.jdbcDriver")) {
/* 126 */         startHsqldbServer();
/*     */       }
/* 128 */       createConnection(driverClassName, url);
/* 129 */       dropTables();
/* 130 */       createTables();
/* 131 */       populateAccess();
/* 132 */       this.initialized = true;
/*     */     }
/*     */   }
/*     */ 
/*     */   private void dropTables() {
/* 137 */     String dropAccess = "DROP TABLE Access";
/* 138 */     String dropPageview = "DROP TABLE Pageview";
/*     */     try
/*     */     {
/* 141 */       Statement st = this.connection.createStatement();
/* 142 */       st.executeUpdate(dropAccess);
/* 143 */       st.executeUpdate(dropPageview);
/* 144 */       this.connection.commit();
/* 145 */       st.close();
/*     */     }
/*     */     catch (SQLException ex)
/*     */     {
/*     */     }
/*     */   }
/*     */ 
/*     */   private void createTables() throws SQLException {
/* 153 */     String createAccess = "CREATE TABLE Access(url      VARCHAR(100) NOT NULL, referrer VARCHAR(100), time     BIGINT NOT NULL,  PRIMARY KEY (url, time))";
/*     */ 
/* 160 */     String createPageview = "CREATE TABLE Pageview(url      VARCHAR(100) NOT NULL, pageview     BIGINT NOT NULL,  PRIMARY KEY (url))";
/*     */ 
/* 166 */     Statement st = this.connection.createStatement();
/*     */     try {
/* 168 */       st.executeUpdate(createAccess);
/* 169 */       st.executeUpdate(createPageview);
/* 170 */       this.connection.commit();
/*     */     } finally {
/* 172 */       st.close();
/*     */     }
/*     */   }
/*     */ 
/*     */   private void populateAccess()
/*     */     throws SQLException
/*     */   {
/* 181 */     PreparedStatement statement = null;
/*     */     try {
/* 183 */       statement = this.connection.prepareStatement("INSERT INTO Access(url, referrer, time) VALUES (?, ?, ?)");
/*     */ 
/* 187 */       Random random = new Random();
/*     */ 
/* 189 */       int time = random.nextInt(50) + 50;
/*     */ 
/* 191 */       int PROBABILITY_PRECISION = 100;
/* 192 */       int NEW_PAGE_PROBABILITY = 15;
/*     */ 
/* 196 */       String[] pages = { "/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j" };
/*     */ 
/* 198 */       int[][] linkMatrix = { { 1, 5, 7 }, { 0, 7, 4, 6 }, { 0, 1, 7, 8 }, { 0, 2, 4, 6, 7, 9 }, { 0, 1 }, { 0, 3, 5, 9 }, { 0 }, { 0, 1, 3 }, { 0, 2, 6 }, { 0, 2, 6 } };
/*     */ 
/* 202 */       int currentPage = random.nextInt(pages.length);
/* 203 */       String referrer = null;
/*     */ 
/* 205 */       for (int i = 0; i < time; i++)
/*     */       {
/* 207 */         statement.setString(1, pages[currentPage]);
/* 208 */         statement.setString(2, referrer);
/* 209 */         statement.setLong(3, i);
/* 210 */         statement.execute();
/*     */ 
/* 212 */         int action = random.nextInt(100);
/*     */ 
/* 215 */         if (action < 15) {
/* 216 */           currentPage = random.nextInt(pages.length);
/* 217 */           referrer = null;
/*     */         }
/*     */         else {
/* 220 */           referrer = pages[currentPage];
/* 221 */           action = random.nextInt(linkMatrix[currentPage].length);
/* 222 */           currentPage = linkMatrix[currentPage][action];
/*     */         }
/*     */       }
/*     */ 
/* 226 */       this.connection.commit();
/*     */     }
/*     */     catch (SQLException ex) {
/* 229 */       this.connection.rollback();
/* 230 */       throw ex;
/*     */     } finally {
/* 232 */       if (statement != null)
/* 233 */         statement.close();
/*     */     }
/*     */   }
/*     */ 
/*     */   private boolean verify()
/*     */     throws SQLException
/*     */   {
/* 241 */     String countAccessQuery = "SELECT COUNT(*) FROM Access";
/* 242 */     String sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
/* 243 */     Statement st = null;
/* 244 */     ResultSet rs = null;
/*     */     try {
/* 246 */       st = this.connection.createStatement();
/* 247 */       rs = st.executeQuery(countAccessQuery);
/* 248 */       rs.next();
/* 249 */       long totalPageview = rs.getLong(1);
/*     */ 
/* 251 */       rs = st.executeQuery(sumPageviewQuery);
/* 252 */       rs.next();
/* 253 */       long sumPageview = rs.getLong(1);
/*     */ 
/* 255 */       LOG.info("totalPageview=" + totalPageview);
/* 256 */       LOG.info("sumPageview=" + sumPageview);
/*     */ 
/* 258 */       boolean i = (totalPageview == sumPageview) && (totalPageview != 0L) ? true : false;
/*     */       return i;
/*     */     }
/*     */     finally
/*     */     {
/* 260 */       if (st != null)
/* 261 */         st.close();
/* 262 */       if (rs != null)
/* 263 */         rs.close(); 
/* 263 */     }
/*     */   }
/*     */ 
/*     */   public int run(String[] args)
/*     */     throws Exception
/*     */   {
/* 379 */     String driverClassName = "org.hsqldb.jdbcDriver";
/* 380 */     String url = "jdbc:hsqldb:hsql://localhost/URLAccess";
/*     */ 
/* 382 */     if (args.length > 1) {
/* 383 */       driverClassName = args[0];
/* 384 */       url = args[1];
/*     */     }
/*     */ 
/* 387 */     initialize(driverClassName, url);
/*     */ 
/* 389 */     JobConf job = new JobConf(getConf(), DBCountPageView.class);
/*     */ 
/* 391 */     job.setJobName("Count Pageviews of URLs");
/*     */ 
/* 393 */     job.setMapperClass(PageviewMapper.class);
/* 394 */     job.setCombinerClass(LongSumReducer.class);
/* 395 */     job.setReducerClass(PageviewReducer.class);
/*     */ 
/* 397 */     DBConfiguration.configureDB(job, driverClassName, url);
/*     */ 
/* 399 */     DBInputFormat.setInput(job, AccessRecord.class, "Access", null, "url", AccessFieldNames);
/*     */ 
/* 402 */     DBOutputFormat.setOutput(job, "Pageview", PageviewFieldNames);
/*     */ 
/* 404 */     job.setMapOutputKeyClass(Text.class);
/* 405 */     job.setMapOutputValueClass(LongWritable.class);
/*     */ 
/* 407 */     job.setOutputKeyClass(PageviewRecord.class);
/* 408 */     job.setOutputValueClass(NullWritable.class);
/*     */     try
/*     */     {
/* 411 */       JobClient.runJob(job);
/*     */ 
/* 413 */       boolean correct = verify();
/* 414 */       if (!correct)
/* 415 */         throw new RuntimeException("Evaluation was not correct!");
/*     */     }
/*     */     finally {
/* 418 */       shutdown();
/*     */     }
/* 420 */     return 0;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args) throws Exception {
/* 424 */     int ret = ToolRunner.run(new DBCountPageView(), args);
/* 425 */     System.exit(ret);
/*     */   }
/*     */ 
/*     */   static class PageviewReducer extends MapReduceBase
/*     */     implements Reducer<Text, LongWritable, DBCountPageView.PageviewRecord, NullWritable>
/*     */   {
/* 361 */     NullWritable n = NullWritable.get();
/*     */ 
/*     */     public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<DBCountPageView.PageviewRecord, NullWritable> output, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 367 */       long sum = 0L;
/* 368 */       while (values.hasNext()) {
/* 369 */         sum += ((LongWritable)values.next()).get();
/*     */       }
/* 371 */       output.collect(new DBCountPageView.PageviewRecord(key.toString(), sum), this.n);
/*     */     }
/*     */   }
/*     */ 
/*     */   static class PageviewMapper extends MapReduceBase
/*     */     implements Mapper<LongWritable, DBCountPageView.AccessRecord, Text, LongWritable>
/*     */   {
/* 343 */     LongWritable ONE = new LongWritable(1L);
/*     */ 
/*     */     public void map(LongWritable key, DBCountPageView.AccessRecord value, OutputCollector<Text, LongWritable> output, Reporter reporter)
/*     */       throws IOException
/*     */     {
/* 349 */       Text oKey = new Text(value.url);
/* 350 */       output.collect(oKey, this.ONE);
/*     */     }
/*     */   }
/*     */ 
/*     */   static class PageviewRecord
/*     */     implements Writable, DBWritable
/*     */   {
/*     */     String url;
/*     */     long pageview;
/*     */ 
/*     */     public PageviewRecord(String url, long pageview)
/*     */     {
/* 306 */       this.url = url;
/* 307 */       this.pageview = pageview;
/*     */     }
/*     */ 
/*     */     public void readFields(DataInput in) throws IOException
/*     */     {
/* 312 */       this.url = Text.readString(in);
/* 313 */       this.pageview = in.readLong();
/*     */     }
/*     */ 
/*     */     public void write(DataOutput out) throws IOException {
/* 317 */       Text.writeString(out, this.url);
/* 318 */       out.writeLong(this.pageview);
/*     */     }
/*     */ 
/*     */     public void readFields(ResultSet resultSet) throws SQLException {
/* 322 */       this.url = resultSet.getString(1);
/* 323 */       this.pageview = resultSet.getLong(2);
/*     */     }
/*     */ 
/*     */     public void write(PreparedStatement statement) throws SQLException {
/* 327 */       statement.setString(1, this.url);
/* 328 */       statement.setLong(2, this.pageview);
/*     */     }
/*     */ 
/*     */     public String toString() {
/* 332 */       return this.url + " " + this.pageview;
/*     */     }
/*     */   }
/*     */ 
/*     */   static class AccessRecord
/*     */     implements Writable, DBWritable
/*     */   {
/*     */     String url;
/*     */     String referrer;
/*     */     long time;
/*     */ 
/*     */     public void readFields(DataInput in)
/*     */       throws IOException
/*     */     {
/* 275 */       this.url = Text.readString(in);
/* 276 */       this.referrer = Text.readString(in);
/* 277 */       this.time = in.readLong();
/*     */     }
/*     */ 
/*     */     public void write(DataOutput out) throws IOException
/*     */     {
/* 282 */       Text.writeString(out, this.url);
/* 283 */       Text.writeString(out, this.referrer);
/* 284 */       out.writeLong(this.time);
/*     */     }
/*     */ 
/*     */     public void readFields(ResultSet resultSet) throws SQLException
/*     */     {
/* 289 */       this.url = resultSet.getString(1);
/* 290 */       this.referrer = resultSet.getString(2);
/* 291 */       this.time = resultSet.getLong(3);
/*     */     }
/*     */ 
/*     */     public void write(PreparedStatement statement) throws SQLException {
/* 295 */       statement.setString(1, this.url);
/* 296 */       statement.setString(2, this.referrer);
/* 297 */       statement.setLong(3, this.time);
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.DBCountPageView
 * JD-Core Version:    0.6.0
 */