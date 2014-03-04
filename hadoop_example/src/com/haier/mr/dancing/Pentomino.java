/*     */ package com.haier.mr.dancing;
/*     */ 
/*     */ import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
/*     */ 
/*     */ public class Pentomino
/*     */ {
/*     */   protected int width;
/*     */   protected int height;
/* 249 */   protected List<Piece> pieces = new ArrayList();
/*     */ 
/* 254 */   protected static final int[] oneRotation = { 0 };
/*     */ 
/* 259 */   protected static final int[] twoRotations = { 0, 1 };
/*     */ 
/* 264 */   protected static final int[] fourRotations = { 0, 1, 2, 3 };
/*     */ 
/* 346 */   private DancingLinks<ColumnName> dancer = new DancingLinks();
/*     */   private DancingLinks.SolutionAcceptor<ColumnName> printer;
/*     */ 
/*     */   public static String stringifySolution(int width, int height, List<List<ColumnName>> solution)
/*     */   {
/* 140 */     String[][] picture = new String[height][width];
/* 141 */     StringBuffer result = new StringBuffer();
/*     */ 
/* 143 */     for (List<ColumnName> row : solution)
/*     */     {
/* 145 */       Piece piece = null;
/* 146 */       for (ColumnName item : row) {
/* 147 */         if ((item instanceof Piece)) {
/* 148 */           piece = (Piece)item;
/* 149 */           break;
/*     */         }
/*     */       }
/*     */ 
/* 153 */       for (ColumnName item : row)
/* 154 */         if ((item instanceof Point)) {
/* 155 */           Point p = (Point)item;
/* 156 */           picture[p.y][p.x] = piece.getName();
/*     */         }
/*     */     }
/*     */     Piece piece;
/* 161 */     for (int y = 0; y < picture.length; y++) {
/* 162 */       for (int x = 0; x < picture[y].length; x++) {
/* 163 */         result.append(picture[y][x]);
/*     */       }
/* 165 */       result.append("\n");
/*     */     }
/* 167 */     return result.toString();
/*     */   }
/*     */ 
/*     */   public SolutionCategory getCategory(List<List<ColumnName>> names)
/*     */   {
/* 179 */     Piece xPiece = null;
/*     */ 
/* 181 */     for (Piece p : this.pieces) {
/* 182 */       if ("x".equals(p.name)) {
/* 183 */         xPiece = p;
/* 184 */         break;
/*     */       }
/*     */     }
/*     */ 
/* 188 */     for (List<ColumnName> row : names) {
/* 189 */       if (row.contains(xPiece))
/*     */       {
/* 191 */         int low_x = this.width;
/* 192 */         int high_x = 0;
/* 193 */         int low_y = this.height;
/* 194 */         int high_y = 0;
/* 195 */         for (ColumnName col : row) {
/* 196 */           if ((col instanceof Point)) {
/* 197 */             int x = ((Point)col).x;
/* 198 */             int y = ((Point)col).y;
/* 199 */             if (x < low_x) {
/* 200 */               low_x = x;
/*     */             }
/* 202 */             if (x > high_x) {
/* 203 */               high_x = x;
/*     */             }
/* 205 */             if (y < low_y) {
/* 206 */               low_y = y;
/*     */             }
/* 208 */             if (y > high_y) {
/* 209 */               high_y = y;
/*     */             }
/*     */           }
/*     */         }
/* 213 */         boolean mid_x = low_x + high_x == this.width - 1;
/* 214 */         boolean mid_y = low_y + high_y == this.height - 1;
/* 215 */         if ((mid_x) && (mid_y))
/* 216 */           return SolutionCategory.CENTER;
/* 217 */         if (mid_x)
/* 218 */           return SolutionCategory.MID_X;
/* 219 */         if (!mid_y) break;
/* 220 */         return SolutionCategory.MID_Y;
/*     */       }
/*     */ 
/*     */     }
/*     */ 
/* 225 */     return SolutionCategory.UPPER_LEFT;
/*     */   }
/*     */ 
/*     */   protected void initializePieces()
/*     */   {
/* 270 */     this.pieces.add(new Piece("x", " x /xxx/ x ", false, oneRotation));
/* 271 */     this.pieces.add(new Piece("v", "x  /x  /xxx", false, fourRotations));
/* 272 */     this.pieces.add(new Piece("t", "xxx/ x / x ", false, fourRotations));
/* 273 */     this.pieces.add(new Piece("w", "  x/ xx/xx ", false, fourRotations));
/* 274 */     this.pieces.add(new Piece("u", "x x/xxx", false, fourRotations));
/* 275 */     this.pieces.add(new Piece("i", "xxxxx", false, twoRotations));
/* 276 */     this.pieces.add(new Piece("f", " xx/xx / x ", true, fourRotations));
/* 277 */     this.pieces.add(new Piece("p", "xx/xx/x ", true, fourRotations));
/* 278 */     this.pieces.add(new Piece("z", "xx / x / xx", true, twoRotations));
/* 279 */     this.pieces.add(new Piece("n", "xx  / xxx", true, fourRotations));
/* 280 */     this.pieces.add(new Piece("y", "  x /xxxx", true, fourRotations));
/* 281 */     this.pieces.add(new Piece("l", "   x/xxxx", true, fourRotations));
/*     */   }
/*     */ 
/*     */   private static boolean isSide(int offset, int shapeSize, int board)
/*     */   {
/* 294 */     return 2 * offset + shapeSize <= board;
/*     */   }
/*     */ 
/*     */   private static void generateRows(DancingLinks dancer, Piece piece, int width, int height, boolean flip, boolean[] row, boolean upperLeft)
/*     */   {
/* 318 */     int[] rotations = piece.getRotations();
/* 319 */     for (int rotIndex = 0; rotIndex < rotations.length; rotIndex++)
/*     */     {
/* 321 */       boolean[][] shape = piece.getShape(flip, rotations[rotIndex]);
/*     */ 
/* 323 */       for (int x = 0; x < width; x++)
/* 324 */         for (int y = 0; y < height; y++) {
/* 325 */           if ((y + shape.length > height) || (x + shape[0].length > width) || ((upperLeft) && ((!isSide(x, shape[0].length, width)) || (!isSide(y, shape.length, height)))))
/*     */           {
/*     */             continue;
/*     */           }
/*     */ 
/* 330 */           for (int idx = 0; idx < width * height; idx++) {
/* 331 */             row[idx] = false;
/*     */           }
/*     */ 
/* 334 */           for (int subY = 0; subY < shape.length; subY++) {
/* 335 */             for (int subX = 0; subX < shape[0].length; subX++) {
/* 336 */               row[((y + subY) * width + x + subX)] = shape[subY][subX];
/*     */             }
/*     */           }
/* 339 */           dancer.addRow(row);
/*     */         }
/*     */     }
/*     */   }
/*     */ 
/*     */   public Pentomino(int width, int height)
/*     */   {
/* 350 */     initializePieces();
/*     */ 
/* 359 */     initialize(width, height);
/*     */   }
/*     */ 
/*     */   public Pentomino()
/*     */   {
/* 350 */     initializePieces();
/*     */   }
/*     */ 
/*     */   void initialize(int width, int height)
/*     */   {
/* 369 */     this.width = width;
/* 370 */     this.height = height;
/* 371 */     for (int y = 0; y < height; y++) {
/* 372 */       for (int x = 0; x < width; x++) {
/* 373 */         this.dancer.addColumn(new Point(x, y));
/*     */       }
/*     */     }
/* 376 */     int pieceBase = this.dancer.getNumberColumns();
/* 377 */     for (Piece p : this.pieces) {
/* 378 */       this.dancer.addColumn(p);
/*     */     }
/* 380 */     boolean[] row = new boolean[this.dancer.getNumberColumns()];
/* 381 */     for (int idx = 0; idx < this.pieces.size(); idx++) {
/* 382 */       Piece piece = (Piece)this.pieces.get(idx);
/* 383 */       row[(idx + pieceBase)] = true;
/* 384 */       generateRows(this.dancer, piece, width, height, false, row, idx == 0);
/* 385 */       if (piece.getFlippable()) {
/* 386 */         generateRows(this.dancer, piece, width, height, true, row, idx == 0);
/*     */       }
/* 388 */       row[(idx + pieceBase)] = false;
/*     */     }
/* 390 */     this.printer = new SolutionPrinter(width, height);
/*     */   }
/*     */ 
/*     */   public List<int[]> getSplits(int depth)
/*     */   {
/* 399 */     return this.dancer.split(depth);
/*     */   }
/*     */ 
/*     */   public int solve(int[] split)
/*     */   {
/* 410 */     return this.dancer.solve(split, this.printer);
/*     */   }
/*     */ 
/*     */   public int solve()
/*     */   {
/* 418 */     return this.dancer.solve(this.printer);
/*     */   }
/*     */ 
/*     */   public void setPrinter(DancingLinks.SolutionAcceptor<ColumnName> printer)
/*     */   {
/* 427 */     this.printer = printer;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args)
/*     */   {
/* 434 */     int width = 6;
/* 435 */     int height = 10;
/* 436 */     Pentomino model = new Pentomino(width, height);
/* 437 */     List splits = model.getSplits(2);
/* 438 */     for (Iterator splitItr = splits.iterator(); splitItr.hasNext(); ) {
/* 439 */       int[] choices = (int[])(int[])splitItr.next();
/* 440 */       System.out.print("split:");
/* 441 */       for (int i = 0; i < choices.length; i++) {
/* 442 */         System.out.print(" " + choices[i]);
/*     */       }
/* 444 */       System.out.println();
/*     */ 
/* 446 */       System.out.println(model.solve(choices) + " solutions found.");
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class SolutionPrinter
/*     */     implements DancingLinks.SolutionAcceptor<Pentomino.ColumnName>
/*     */   {
/*     */     int width;
/*     */     int height;
/*     */ 
/*     */     public SolutionPrinter(int width, int height)
/*     */     {
/* 237 */       this.width = width;
/* 238 */       this.height = height;
/*     */     }
/*     */ 
/*     */     public void solution(List<List<Pentomino.ColumnName>> names) {
/* 242 */       System.out.println(Pentomino.stringifySolution(this.width, this.height, names));
/*     */     }
/*     */   }
/*     */ 
/*     */   public static enum SolutionCategory
/*     */   {
/* 170 */     UPPER_LEFT, MID_X, MID_Y, CENTER;
/*     */   }
/*     */ 
/*     */   static class Point
/*     */     implements Pentomino.ColumnName
/*     */   {
/*     */     int x;
/*     */     int y;
/*     */ 
/*     */     Point(int x, int y)
/*     */     {
/* 124 */       this.x = x;
/* 125 */       this.y = y;
/*     */     }
/*     */   }
/*     */ 
/*     */   protected static class Piece
/*     */     implements Pentomino.ColumnName
/*     */   {
/*     */     private String name;
/*     */     private boolean[][] shape;
/*     */     private int[] rotations;
/*     */     private boolean flippable;
/*     */ 
/*     */     public Piece(String name, String shape, boolean flippable, int[] rotations)
/*     */     {
/*  44 */       this.name = name;
/*  45 */       this.rotations = rotations;
/*  46 */       this.flippable = flippable;
/*  47 */       StringTokenizer parser = new StringTokenizer(shape, "/");
/*  48 */       List lines = new ArrayList();
/*  49 */       while (parser.hasMoreTokens()) {
/*  50 */         String token = parser.nextToken();
/*  51 */         boolean[] line = new boolean[token.length()];
/*  52 */         for (int i = 0; i < line.length; i++) {
/*  53 */           line[i] = (token.charAt(i) == 'x' ? true : false);
/*     */         }
/*  55 */         lines.add(line);
/*     */       }
/*  57 */       this.shape = new boolean[lines.size()][];
/*  58 */       for (int i = 0; i < lines.size(); i++)
/*  59 */         this.shape[i] = ((boolean[])(boolean[])lines.get(i));
/*     */     }
/*     */ 
/*     */     public String getName()
/*     */     {
/*  64 */       return this.name;
/*     */     }
/*     */ 
/*     */     public int[] getRotations() {
/*  68 */       return this.rotations;
/*     */     }
/*     */ 
/*     */     public boolean getFlippable() {
/*  72 */       return this.flippable;
/*     */     }
/*     */ 
/*     */     private int doFlip(boolean flip, int x, int max) {
/*  76 */       if (flip) {
/*  77 */         return max - x - 1;
/*     */       }
/*  79 */       return x;
/*     */     }
/*     */ 
/*     */     public boolean[][] getShape(boolean flip, int rotate)
/*     */     {
/*     */       boolean[][] result;
/*  85 */       if (rotate % 2 == 0) {
/*  86 */         int height = this.shape.length;
/*  87 */         int width = this.shape[0].length;
/*  88 */         result = new boolean[height][];
/*  89 */         boolean flipX = rotate == 2;
/*  90 */         boolean flipY = flip ^ rotate == 2;
/*  91 */         for (int y = 0; y < height; y++) {
/*  92 */           result[y] = new boolean[width];
/*  93 */           for (int x = 0; x < width; x++)
/*  94 */             result[y][x] = this.shape[doFlip(flipY, y, height)][doFlip(flipX, x, width)];
/*     */         }
/*     */       }
/*     */       else
/*     */       {
/*  99 */         int height = this.shape[0].length;
/* 100 */         int width = this.shape.length;
/* 101 */         result = new boolean[height][];
/* 102 */         boolean flipX = rotate == 3;
/* 103 */         boolean flipY = flip ^ rotate == 1;
/* 104 */         for (int y = 0; y < height; y++) {
/* 105 */           result[y] = new boolean[width];
/* 106 */           for (int x = 0; x < width; x++) {
/* 107 */             result[y][x] = this.shape[doFlip(flipX, x, width)][doFlip(flipY, y, height)];
/*     */           }
/*     */         }
/*     */       }
/*     */ 
/* 112 */       return result;
/*     */     }
/*     */   }
/*     */ 
/*     */   protected static abstract interface ColumnName
/*     */   {
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.dancing.Pentomino
 * JD-Core Version:    0.6.0
 */