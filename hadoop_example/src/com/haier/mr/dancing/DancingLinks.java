/*     */ package com.haier.mr.dancing;
/*     */ 
/*     */ import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/*     */ 
/*     */ public class DancingLinks<ColumnName>
/*     */ {
/*  38 */   private static final Log LOG = LogFactory.getLog(DancingLinks.class.getName());
/*     */   private ColumnHeader<ColumnName> head;
/*     */   private List<ColumnHeader<ColumnName>> columns;
/*     */ 
/*     */   public DancingLinks()
/*     */   {
/* 100 */     this.head = new ColumnHeader(null, 0);
/* 101 */     this.head.left = this.head;
/* 102 */     this.head.right = this.head;
/* 103 */     this.head.up = this.head;
/* 104 */     this.head.down = this.head;
/* 105 */     this.columns = new ArrayList(200);
/*     */   }
/*     */ 
/*     */   public void addColumn(ColumnName name, boolean primary)
/*     */   {
/* 115 */     ColumnHeader top = new ColumnHeader(name, 0);
/* 116 */     top.up = top;
/* 117 */     top.down = top;
/* 118 */     if (primary) {
/* 119 */       Node tail = this.head.left;
/* 120 */       tail.right = top;
/* 121 */       top.left = tail;
/* 122 */       top.right = this.head;
/* 123 */       this.head.left = top;
/*     */     } else {
/* 125 */       top.left = top;
/* 126 */       top.right = top;
/*     */     }
/* 128 */     this.columns.add(top);
/*     */   }
/*     */ 
/*     */   public void addColumn(ColumnName name)
/*     */   {
/* 136 */     addColumn(name, true);
/*     */   }
/*     */ 
/*     */   public int getNumberColumns()
/*     */   {
/* 144 */     return this.columns.size();
/*     */   }
/*     */ 
/*     */   public String getColumnName(int index)
/*     */   {
/* 153 */     return ((ColumnHeader)this.columns.get(index)).name.toString();
/*     */   }
/*     */ 
/*     */   public void addRow(boolean[] values)
/*     */   {
/* 161 */     Node prev = null;
/* 162 */     for (int i = 0; i < values.length; i++)
/* 163 */       if (values[i] != false) {
/* 164 */         ColumnHeader top = (ColumnHeader)this.columns.get(i);
/* 165 */         top.size += 1;
/* 166 */         Node bottom = top.up;
/* 167 */         Node node = new Node(null, null, bottom, top, top);
/*     */ 
/* 169 */         bottom.down = node;
/* 170 */         top.up = node;
/* 171 */         if (prev != null) {
/* 172 */           Node front = prev.right;
/* 173 */           node.left = prev;
/* 174 */           node.right = front;
/* 175 */           prev.right = node;
/* 176 */           front.left = node;
/*     */         } else {
/* 178 */           node.left = node;
/* 179 */           node.right = node;
/*     */         }
/* 181 */         prev = node;
/*     */       }
/*     */   }
/*     */ 
/*     */   private ColumnHeader<ColumnName> findBestColumn()
/*     */   {
/* 204 */     int lowSize = 2147483647;
/* 205 */     ColumnHeader result = null;
/* 206 */     ColumnHeader current = (ColumnHeader)this.head.right;
/* 207 */     while (current != this.head) {
/* 208 */       if (current.size < lowSize) {
/* 209 */         lowSize = current.size;
/* 210 */         result = current;
/*     */       }
/* 212 */       current = (ColumnHeader)current.right;
/*     */     }
/* 214 */     return result;
/*     */   }
/*     */ 
/*     */   private void coverColumn(ColumnHeader<ColumnName> col)
/*     */   {
/* 222 */     LOG.debug("cover " + col.head.name);
/*     */ 
/* 224 */     col.right.left = col.left;
/* 225 */     col.left.right = col.right;
/* 226 */     Node row = col.down;
/* 227 */     while (row != col) {
/* 228 */       Node node = row.right;
/* 229 */       while (node != row) {
/* 230 */         node.down.up = node.up;
/* 231 */         node.up.down = node.down;
/* 232 */         node.head.size -= 1;
/* 233 */         node = node.right;
/*     */       }
/* 235 */       row = row.down;
/*     */     }
/*     */   }
/*     */ 
/*     */   private void uncoverColumn(ColumnHeader<ColumnName> col)
/*     */   {
/* 244 */     LOG.debug("uncover " + col.head.name);
/* 245 */     Node row = col.up;
/* 246 */     while (row != col) {
/* 247 */       Node node = row.left;
/* 248 */       while (node != row) {
/* 249 */         node.head.size += 1;
/* 250 */         node.down.up = node;
/* 251 */         node.up.down = node;
/* 252 */         node = node.left;
/*     */       }
/* 254 */       row = row.up;
/*     */     }
/* 256 */     col.right.left = col;
/* 257 */     col.left.right = col;
/*     */   }
/*     */ 
/*     */   private List<ColumnName> getRowName(Node<ColumnName> row)
/*     */   {
/* 267 */     List result = new ArrayList();
/* 268 */     result.add(row.head.name);
/* 269 */     Node node = row.right;
/* 270 */     while (node != row) {
/* 271 */       result.add(node.head.name);
/* 272 */       node = node.right;
/*     */     }
/* 274 */     return result;
/*     */   }
/*     */ 
/*     */   private int search(List<Node<ColumnName>> partial, SolutionAcceptor<ColumnName> output)
/*     */   {
/* 285 */     int results = 0;
/* 286 */     if (this.head.right == this.head) {
/* 287 */       List result = new ArrayList(partial.size());
/* 288 */       for (Node row : partial) {
/* 289 */         result.add(getRowName(row));
/*     */       }
/* 291 */       output.solution(result);
/* 292 */       results++;
/*     */     } else {
/* 294 */       ColumnHeader col = findBestColumn();
/* 295 */       if (col.size > 0) {
/* 296 */         coverColumn(col);
/* 297 */         Node row = col.down;
/* 298 */         while (row != col) {
/* 299 */           partial.add(row);
/* 300 */           Node node = row.right;
/* 301 */           while (node != row) {
/* 302 */             coverColumn(node.head);
/* 303 */             node = node.right;
/*     */           }
/* 305 */           results += search(partial, output);
/* 306 */           partial.remove(partial.size() - 1);
/* 307 */           node = row.left;
/* 308 */           while (node != row) {
/* 309 */             uncoverColumn(node.head);
/* 310 */             node = node.left;
/*     */           }
/* 312 */           row = row.down;
/*     */         }
/* 314 */         uncoverColumn(col);
/*     */       }
/*     */     }
/* 317 */     return results;
/*     */   }
/*     */ 
/*     */   private void searchPrefixes(int depth, int[] choices, List<int[]> prefixes)
/*     */   {
/* 329 */     if (depth == 0) {
/* 330 */       prefixes.add(choices.clone());
/*     */     } else {
/* 332 */       ColumnHeader col = findBestColumn();
/* 333 */       if (col.size > 0) {
/* 334 */         coverColumn(col);
/* 335 */         Node row = col.down;
/* 336 */         int rowId = 0;
/* 337 */         while (row != col) {
/* 338 */           Node node = row.right;
/* 339 */           while (node != row) {
/* 340 */             coverColumn(node.head);
/* 341 */             node = node.right;
/*     */           }
/* 343 */           choices[(choices.length - depth)] = rowId;
/* 344 */           searchPrefixes(depth - 1, choices, prefixes);
/* 345 */           node = row.left;
/* 346 */           while (node != row) {
/* 347 */             uncoverColumn(node.head);
/* 348 */             node = node.left;
/*     */           }
/* 350 */           row = row.down;
/* 351 */           rowId++;
/*     */         }
/* 353 */         uncoverColumn(col);
/*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   public List<int[]> split(int depth)
/*     */   {
/* 364 */     int[] choices = new int[depth];
/* 365 */     List result = new ArrayList(100000);
/* 366 */     searchPrefixes(depth, choices, result);
/* 367 */     return result;
/*     */   }
/*     */ 
/*     */   private Node<ColumnName> advance(int goalRow)
/*     */   {
/* 376 */     ColumnHeader col = findBestColumn();
/* 377 */     if (col.size > 0) {
/* 378 */       coverColumn(col);
/* 379 */       Node row = col.down;
/* 380 */       int id = 0;
/* 381 */       while (row != col) {
/* 382 */         if (id == goalRow) {
/* 383 */           Node node = row.right;
/* 384 */           while (node != row) {
/* 385 */             coverColumn(node.head);
/* 386 */             node = node.right;
/*     */           }
/* 388 */           return row;
/*     */         }
/* 390 */         id++;
/* 391 */         row = row.down;
/*     */       }
/*     */     }
/* 394 */     return null;
/*     */   }
/*     */ 
/*     */   private void rollback(Node<ColumnName> row)
/*     */   {
/* 402 */     Node node = row.left;
/* 403 */     while (node != row) {
/* 404 */       uncoverColumn(node.head);
/* 405 */       node = node.left;
/*     */     }
/* 407 */     uncoverColumn(row.head);
/*     */   }
/*     */ 
/*     */   public int solve(int[] prefix, SolutionAcceptor<ColumnName> output)
/*     */   {
/* 418 */     List choices = new ArrayList();
/* 419 */     for (int i = 0; i < prefix.length; i++) {
/* 420 */       choices.add(advance(prefix[i]));
/*     */     }
/* 422 */     int result = search(choices, output);
/* 423 */     for (int i = prefix.length - 1; i >= 0; i--) {
/* 424 */       rollback((Node)choices.get(i));
/*     */     }
/* 426 */     return result;
/*     */   }
/*     */ 
/*     */   public int solve(SolutionAcceptor<ColumnName> output)
/*     */   {
/* 435 */     return search(new ArrayList(), output);
/*     */   }
/*     */ 
/*     */   public static abstract interface SolutionAcceptor<ColumnName>
/*     */   {
/*     */     public abstract void solution(List<List<ColumnName>> paramList);
/*     */   }
/*     */ 
/*     */   private static class ColumnHeader<ColumnName> extends DancingLinks.Node<ColumnName>
/*     */   {
/*     */     ColumnName name;
/*     */     int size;
/*     */ 
/*     */     ColumnHeader(ColumnName n, int s)
/*     */     {
/*  78 */       this.name = n;
/*  79 */       this.size = s;
/*  80 */       this.head = this;
/*     */     }
/*     */ 
/*     */     ColumnHeader() {
/*  84 */       this(null, 0);
/*     */     }
/*     */   }
/*     */ 
/*     */   private static class Node<ColumnName>
/*     */   {
/*     */     Node<ColumnName> left;
/*     */     Node<ColumnName> right;
/*     */     Node<ColumnName> up;
/*     */     Node<ColumnName> down;
/*     */     DancingLinks.ColumnHeader<ColumnName> head;
/*     */ 
/*     */     Node(Node<ColumnName> l, Node<ColumnName> r, Node<ColumnName> u, Node<ColumnName> d, DancingLinks.ColumnHeader<ColumnName> h)
/*     */     {
/*  55 */       this.left = l;
/*  56 */       this.right = r;
/*  57 */       this.up = u;
/*  58 */       this.down = d;
/*  59 */       this.head = h;
/*     */     }
/*     */ 
/*     */     Node() {
/*  63 */       this(null, null, null, null, null);
/*     */     }
/*     */   }
/*     */ }

/* Location:           C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\
 * Qualified Name:     org.apache.hadoop.examples.dancing.DancingLinks
 * JD-Core Version:    0.6.0
 */