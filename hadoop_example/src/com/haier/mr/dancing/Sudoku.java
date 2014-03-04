/*     */ package com.haier.mr.dancing;
/*     */ 
/*     */ import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
/*     */ 
/*     */ public class Sudoku
/*     */ {
/*     */   private int[][] board;
/*     */   private int size;
/*     */   private int squareXSize;
/*     */   private int squareYSize;
/*     */ 
/*     */   static String stringifySolution(int size, List<List<ColumnName>> solution)
/*     */   {
/*  66 */     int[][] picture = new int[size][size];
/*  67 */     StringBuffer result = new StringBuffer();
/*     */ 
/*  70 */     for (List<ColumnName> row : solution) {
/*  71 */       int x = -1;
/*  72 */       int y = -1;
/*  73 */       int num = -1;
/*  74 */       for (ColumnName item : row) {
/*  75 */         if ((item instanceof ColumnConstraint)) {
/*  76 */           x = ((ColumnConstraint)item).column;
/*  77 */           num = ((ColumnConstraint)item).num;
/*  78 */         } else if ((item instanceof RowConstraint)) {
/*  79 */           y = ((RowConstraint)item).row;
/*     */         }
/*     */       }
/*  82 */       picture[y][x] = num;
/*     */     }
/*     */ 
/*  85 */     for (int y = 0; y < size; y++) {
/*  86 */       for (int x = 0; x < size; x++) {
/*  87 */         result.append(picture[y][x]);
/*  88 */         result.append(" ");
/*     */       }
/*  90 */       result.append("\n");
/*     */     }
/*  92 */     return result.toString();
/*     */   }
/*     */ 
/*     */   public Sudoku(InputStream stream)
/*     */     throws IOException
/*     */   {
/* 136 */     BufferedReader file = new BufferedReader(new InputStreamReader(stream));
/* 137 */     String line = file.readLine();
/* 138 */     List result = new ArrayList();
/* 139 */     while (line != null) {
/* 140 */       StringTokenizer tokenizer = new StringTokenizer(line);
/* 141 */       int size = tokenizer.countTokens();
/* 142 */       int[] col = new int[size];
/* 143 */       int y = 0;
/* 144 */       while (tokenizer.hasMoreElements()) {
/* 145 */         String word = tokenizer.nextToken();
/* 146 */         if ("?".equals(word))
/* 147 */           col[y] = -1;
/*     */         else {
/* 149 */           col[y] = Integer.parseInt(word);
/*     */         }
/* 151 */         y++;
/*     */       }
/* 153 */       result.add(col);
/* 154 */       line = file.readLine();
/*     */     }
/* 156 */     this.size = result.size();
/* 157 */     this.board = ((int[][])(int[][])result.toArray(new int[this.size][]));
/* 158 */     this.squareYSize = (int)Math.sqrt(this.size);
/* 159 */     this.squareXSize = (this.size / this.squareYSize);
/* 160 */     file.close();
/*     */   }
/*     */ 
/*     */   private boolean[] generateRow(boolean[] rowValues, int x, int y, int num)
/*     */   {
/* 235 */     for (int i = 0; i < rowValues.length; i++) {
/* 236 */       rowValues[i] = false;
/*     */     }
/*     */ 
/* 239 */     int xBox = x / this.squareXSize;
/* 240 */     int yBox = y / this.squareYSize;
/*     */ 
/* 242 */     rowValues[(x * this.size + num - 1)] = true;
/*     */ 
/* 244 */     rowValues[(this.size * this.size + y * this.size + num - 1)] = true;
/*     */ 
/* 246 */     rowValues[(2 * this.size * this.size + (xBox * this.squareXSize + yBox) * this.size + num - 1)] = true;
/*     */ 
/* 248 */     rowValues[(3 * this.size * this.size + this.size * x + y)] = true;
/* 249 */     return rowValues;
/*     */   }
/*     */ 
/*     */   private DancingLinks<ColumnName> makeModel() {
/* 253 */     DancingLinks model = new DancingLinks();
/*     */ 
/* 255 */     for (int x = 0; x < this.size; x++) {
/* 256 */       for (int num = 1; num <= this.size; num++) {
/* 257 */         model.addColumn(new ColumnConstraint(num, x));
/*     */       }
/*     */     }
/*     */ 
/* 261 */     for (int y = 0; y < this.size; y++) {
/* 262 */       for (int num = 1; num <= this.size; num++) {
/* 263 */         model.addColumn(new RowConstraint(num, y));
/*     */       }
/*     */     }
/*     */ 
/* 267 */     for (int x = 0; x < this.squareYSize; x++) {
/* 268 */       for (int y = 0; y < this.squareXSize; y++) {
/* 269 */         for (int num = 1; num <= this.size; num++) {
/* 270 */           model.addColumn(new SquareConstraint(num, x, y));
/*     */         }
/*     */       }
/*     */     }
/*     */ 
/* 275 */     for (int x = 0; x < this.size; x++) {
/* 276 */       for (int y = 0; y < this.size; y++) {
/* 277 */         model.addColumn(new CellConstraint(x, y));
/*     */       }
/*     */     }
/* 280 */     boolean[] rowValues = new boolean[this.size * this.size * 4];
/* 281 */     for (int x = 0; x < this.size; x++) {
/* 282 */       for (int y = 0; y < this.size; y++) {
/* 283 */         if (this.board[y][x] == -1)
/*     */         {
/* 285 */           for (int num = 1; num <= this.size; num++) {
/* 286 */             model.addRow(generateRow(rowValues, x, y, num));
/*     */           }
/*     */         }
/*     */         else {
/* 290 */           model.addRow(generateRow(rowValues, x, y, this.board[y][x]));
/*     */         }
/*     */       }
/*     */     }
/* 294 */     return model;
/*     */   }
/*     */ 
/*     */   public void solve() {
/* 298 */     DancingLinks model = makeModel();
/* 299 */     int results = model.solve(new SolutionPrinter(this.size));
/* 300 */     System.out.println("Found " + results + " solutions");
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */     throws IOException
/*     */   {
/* 308 */     if (args.length == 0) {
/* 309 */       System.out.println("Include a puzzle on the command line.");
/*     */     }
/* 311 */     for (int i = 0; i < args.length; i++) {
/* 312 */       Sudoku problem = new Sudoku(new FileInputStream(args[i]));
/* 313 */       System.out.println("Solving " + args[i]);
/* 314 */       problem.solve();
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class CellConstraint
/*     */     implements Sudoku.ColumnName
/*     */   {
/*     */     int x;
/*     */     int y;
/*     */ 
/*     */     CellConstraint(int x, int y)
/*     */     {
/* 215 */       this.x = x;
/* 216 */       this.y = y;
/*     */     }
/*     */ 
/*     */     public String toString()
/*     */     {
/* 221 */       return "cell " + this.x + "," + this.y;
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class SquareConstraint
/*     */     implements Sudoku.ColumnName
/*     */   {
/*     */     int num;
/*     */     int x;
/*     */     int y;
/*     */ 
/*     */     SquareConstraint(int num, int x, int y)
/*     */     {
/* 198 */       this.num = num;
/* 199 */       this.x = x;
/* 200 */       this.y = y;
/*     */     }
/*     */ 
/*     */     public String toString()
/*     */     {
/* 206 */       return this.num + " in square " + this.x + "," + this.y;
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class RowConstraint
/*     */     implements Sudoku.ColumnName
/*     */   {
/*     */     int num;
/*     */     int row;
/*     */ 
/*     */     RowConstraint(int num, int row)
/*     */     {
/* 183 */       this.num = num;
/* 184 */       this.row = row;
/*     */     }
/*     */ 
/*     */     public String toString()
/*     */     {
/* 189 */       return this.num + " in row " + this.row;
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class ColumnConstraint
/*     */     implements Sudoku.ColumnName
/*     */   {
/*     */     int num;
/*     */     int column;
/*     */ 
/*     */     ColumnConstraint(int num, int column)
/*     */     {
/* 168 */       this.num = num;
/* 169 */       this.column = column;
/*     */     }
/*     */ 
/*     */     public String toString()
/*     */     {
/* 174 */       return this.num + " in column " + this.column;
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class SolutionPrinter
/*     */     implements DancingLinks.SolutionAcceptor<Sudoku.ColumnName>
/*     */   {
/*     */     int size;
/*     */ 
/*     */     public SolutionPrinter(int size)
/*     */     {
/* 104 */       this.size = size;
/*     */     }
/*     */ 
/*     */     void rawWrite(List solution)
/*     */     {
/* 113 */       for (Iterator itr = solution.iterator(); itr.hasNext(); ) {
/* 114 */         Iterator subitr = ((List)itr.next()).iterator();
/* 115 */         while (subitr.hasNext()) {
/* 116 */           System.out.print(subitr.next().toString() + " ");
/*     */         }
/* 118 */         System.out.println();
/*     */       }
/*     */     }
/*     */ 
/*     */     public void solution(List<List<Sudoku.ColumnName>> names) {
/* 123 */       System.out.println(Sudoku.stringifySolution(this.size, names));
/*     */     }
/*     */   }
/*     */ 
/*     */   protected static abstract interface ColumnName
/*     */   {
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.dancing.Sudoku
 * JD-Core Version:    0.6.0
 */