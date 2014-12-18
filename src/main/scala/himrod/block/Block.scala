package himrod.block

import breeze.linalg.{DenseMatrix => BDM}
import breeze.linalg._
/*import breeze.numerics*/

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException


// ====================================================================
// define a block id as its (row,col) in a larger BlockMat:
// 0 <= row <= num_row_blocks
// 0 <= col <= num_col_blocks
// ====================================================================
class BlockID(val row: Long, val col: Long) extends Serializable
{
	override def equals(other: Any): Boolean = 
		other match {
			case id: BlockID =>
				(row == id.row) && (col == id.col)
			case _ => false
		};
	
}

// ====================================================================
// define the dimensions (in rows/cols) of a block matrix
// ====================================================================
class BlockSize(val nrows: Long, val ncols: Long) extends Serializable
{
	override def equals(other: Any): Boolean = 
		other match {
			case bs: BlockSize =>
				(nrows == bs.nrows) && (ncols == bs.ncols);
			case _ => false
		};

	def innerDimEqual(other: BlockSize): Boolean = (ncols == other.nrows);
}

// ====================================================================
// define a single Block as a local BreezeDenseMatrix[Double] 
//
// ====================================================================
class Block(val id: BlockID, val mat: BDM[Double]) extends Serializable
{
	def isZero(): Boolean = !any(mat);

	def size(): BlockSize = new BlockSize(mat.rows,mat.cols);

	def innerDimEqual(other: Block): Boolean = size().innerDimEqual(other.size());

	def row(): Long = id.row;
	
	def col(): Long = id.col;
	
	override def toString: String = {
		val str = new StringBuilder(
			"\block_{" + row() + "," + col() + "}="
		)
		str.append(mat.data.mkString(","));
		str.toString;
	}
}

