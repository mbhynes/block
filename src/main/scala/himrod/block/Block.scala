package himrod.block

import breeze.linalg._
import breeze.linalg.{DenseMatrix => BDM}
import breeze.numerics

/*import org.apache.spark.rdd.RDD*/
/*import org.apache.spark.SparkContext*/
/*import org.apache.spark.SparkContext._*/
/*import org.apache.spark.SparkException*/


// ====================================================================
// define a block id as its (row,col) in a larger BlockMat:
// 0 <= row <= num_row_blocks
// 0 <= col <= num_col_blocks
// ====================================================================
/*case class BlockID(val row: Long, val col: Long) extends Serializable*/
/*{*/
/*	def product(other: BlockID): BlockID = BlockID(row,other.col);*/
/**/
/*	def transpose(): BlockID = BlockID(col,row);*/
/**/
/*	override def equals(other: Any): Boolean = */
/*		other match {*/
/*			case id: BlockID =>*/
/*				(row == id.row) && (col == id.col)*/
/*			case _ => false*/
/*		};*/
/*	*/
/*	override def toString() = "("+row+","+col+")";*/
/**/
/*}*/
/**/
/*object BlockID {*/
/**/
/*	def fromID(id: Long, matSize: BlockSize, bsize: BlockSize): BlockID = */
/*	{*/
/*		val row: Long = (id % matSize.nrows) / bsize.nrows;*/
/*		val col: Long = (id / matSize.ncols) / bsize.ncols;*/
/*		BlockID(row,col);*/
/*	}*/
/*}*/

// ====================================================================
// define the dimensions (in rows/cols) of a block matrix
// ====================================================================
/*case class BlockSize(val nrows: Long, val ncols: Long) extends Serializable*/
/*{*/
/*	override def equals(other: Any): Boolean = */
/*		other match {*/
/*			case bs: BlockSize =>*/
/*				(nrows == bs.nrows) && (ncols == bs.ncols);*/
/*			case _ => false*/
/*		};*/
/**/
/*	override def toString() = "("+nrows+","+ncols+")";*/
/**/
/*	def transpose(): BlockSize = BlockSize(ncols,nrows);*/
/**/
/*	def innerDimEqual(other: BlockSize): Boolean = (ncols == other.nrows);*/
/**/
/*	def product(other: BlockSize): BlockSize = BlockSize(nrows,other.ncols);*/
/*}*/

// ====================================================================
// define a single Block as a local BreezeDenseMatrix[Double] 
//
// ====================================================================
case class BlockMismatchException(msg: String) extends Exception
case class BlockSizeErrorException(msg: String) extends Exception

case class Block(val id: BlockID, val mat: BDM[Double]) extends Serializable
{
	override def hashCode(): Int = (row() + col()*nrows()).toInt;

	def equals(other: Block): Boolean =
	{
		(id == other.id) && (mat == other.mat)	
	}
	
	//scalar operations
	def +(a: Double): Block = Block(id, mat+a);
	def -(a: Double): Block = Block(id, mat-a);
	def *(a: Double): Block = Block(id, mat*a);
	def /(a: Double): Block = Block(id, mat/a);

	//matrix-matrix Block operations 
	def +(other: Block): Block = 
	{
		if (id != other.id)
			throw BlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);
		else
			Block(id, mat + other.mat);
	}
	def -(other: Block): Block = 
	{
		if (id != other.id)
			throw BlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);
		else
			Block(id, mat - other.mat);
	}
	def *(other: Block) =
	{
		if (!innerDimEqual(other))
			throw BlockSizeErrorException("Matrix inner dimensions do not match: "+size()+" and "+other.size);
		else
		{
			val outerID: BlockID = BlockID(id.row,other.id.col);
			Block(outerID, mat * other.mat);
		}
	}

	def transpose(): Block = Block(id.transpose(),mat.t);

	def isZero(): Boolean = !any(mat);

	def nrows(): Long = mat.rows;

	def ncols(): Long = mat.cols;

	def size(): BlockSize = BlockSize(mat.rows,mat.cols);

	def innerDimEqual(other: Block): Boolean = size().innerDimEqual(other.size());

	def row(): Long = id.row;
	
	def col(): Long = id.col;
	
	override def toString: String = {
		id.toString() + ":" + mat.data.mkString(",");
	}

	def print() = {
		println(id);
		println(mat);
	}
}

object Block 
{
	def fromArray(
		id: BlockID, 
		matSize: BlockSize, 
		mat: Array[Double]) = 
	{
		val m = matSize.nrows.toInt;
		val n = matSize.ncols.toInt;
		val block: BDM[Double] = new BDM(m,n,mat);
		Block(id, block);
	}

	def rand(
		id: BlockID, 
		bsize: BlockSize) = 
	{
		val m = bsize.nrows.toInt;
		val n = bsize.ncols.toInt;
		val mat: BDM[Double] = BDM.rand(m,n);
		Block(id,mat);
	}

	def zeros(
		id: BlockID, 
		bsize: BlockSize) = 
	{
		val m = bsize.nrows.toInt;
		val n = bsize.ncols.toInt;
		val mat: BDM[Double] = BDM.zeros(m,n);
		Block(id,mat);
	}

	def eye(
		id: BlockID, 
		bsize: BlockSize) = 
	{
		def onDiag(id:BlockID): Boolean = (id.row == id.col);
		
		val m = bsize.nrows.toInt;
		val n = bsize.ncols.toInt;
		val mat: BDM[Double] = {
			if (onDiag(id) && m==n)
				BDM.eye[Double](m);
			else
				BDM.zeros(m,n);
		}
		Block(id,mat);
	}

}
