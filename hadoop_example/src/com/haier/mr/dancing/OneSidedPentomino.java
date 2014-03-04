/*    */ package com.haier.mr.dancing;
/*    */ 
/*    */ /*    */ 
/*    */ public class OneSidedPentomino extends Pentomino
/*    */ {
/*    */   public OneSidedPentomino()
/*    */   {
/*    */   }
/*    */ 
/*    */   public OneSidedPentomino(int width, int height)
/*    */   {
/* 32 */     super(width, height);
/*    */   }
/*    */ 
/*    */   protected void initializePieces()
/*    */   {
/* 40 */     this.pieces.add(new Pentomino.Piece("x", " x /xxx/ x ", false, oneRotation));
/* 41 */     this.pieces.add(new Pentomino.Piece("v", "x  /x  /xxx", false, fourRotations));
/* 42 */     this.pieces.add(new Pentomino.Piece("t", "xxx/ x / x ", false, fourRotations));
/* 43 */     this.pieces.add(new Pentomino.Piece("w", "  x/ xx/xx ", false, fourRotations));
/* 44 */     this.pieces.add(new Pentomino.Piece("u", "x x/xxx", false, fourRotations));
/* 45 */     this.pieces.add(new Pentomino.Piece("i", "xxxxx", false, twoRotations));
/* 46 */     this.pieces.add(new Pentomino.Piece("f", " xx/xx / x ", false, fourRotations));
/* 47 */     this.pieces.add(new Pentomino.Piece("p", "xx/xx/x ", false, fourRotations));
/* 48 */     this.pieces.add(new Pentomino.Piece("z", "xx / x / xx", false, twoRotations));
/* 49 */     this.pieces.add(new Pentomino.Piece("n", "xx  / xxx", false, fourRotations));
/* 50 */     this.pieces.add(new Pentomino.Piece("y", "  x /xxxx", false, fourRotations));
/* 51 */     this.pieces.add(new Pentomino.Piece("l", "   x/xxxx", false, fourRotations));
/* 52 */     this.pieces.add(new Pentomino.Piece("F", "xx / xx/ x ", false, fourRotations));
/* 53 */     this.pieces.add(new Pentomino.Piece("P", "xx/xx/ x", false, fourRotations));
/* 54 */     this.pieces.add(new Pentomino.Piece("Z", " xx/ x /xx ", false, twoRotations));
/* 55 */     this.pieces.add(new Pentomino.Piece("N", "  xx/xxx ", false, fourRotations));
/* 56 */     this.pieces.add(new Pentomino.Piece("Y", " x  /xxxx", false, fourRotations));
/* 57 */     this.pieces.add(new Pentomino.Piece("L", "x   /xxxx", false, fourRotations));
/*    */   }
/*    */ 
/*    */   public static void main(String[] args)
/*    */   {
/* 65 */     Pentomino model = new OneSidedPentomino(3, 30);
/* 66 */     int solutions = model.solve();
/* 67 */     System.out.println(solutions + " solutions found.");
/*    */   }
/*    */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.dancing.OneSidedPentomino
 * JD-Core Version:    0.6.0
 */