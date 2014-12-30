package himrod.block

import breeze.linalg._
import breeze.linalg.{DenseMatrix => BDM}
import breeze.numerics

// ====================================================================
// define a single Block as a local BreezeDenseMatrix[Double] 
// ====================================================================
/*case class BlockMismatchException(msg: String) extends Exception*/
/*case class BlockSizeErrorException(msg: String) extends Exception*/

case class PrintBlock(
	val id: BlockID, 
	val mat: BDM[Double]
	) extends Serializable
{
	/*override def hashCode(): Int = (row() + col()*nrows()).toInt;*/

	/*def equals(other: Block): Boolean =*/
	/*{*/
	/*	(id == other.id) && (mat == other.mat)	*/
	/*}*/
	/**/
	/*//scalar operations*/
	/*def +(a: Double): Block = Block(id, mat+a);*/
	/*def -(a: Double): Block = Block(id, mat-a);*/
	/*def *(a: Double): Block = Block(id, mat*a);*/
	/*def /(a: Double): Block = Block(id, mat/a);*/

	/*//matrix-matrix Block operations */
	/*def +(other: Block): Block = */
	/*{*/
	/*	if (id != other.id)*/
	/*		throw BlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);*/
	/*	else*/
	/*		Block(id, mat + other.mat);*/
	/*}*/
	/*def -(other: Block): Block = */
	/*{*/
	/*	if (id != other.id)*/
	/*		throw BlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);*/
	/*	else*/
	/*		Block(id, mat - other.mat);*/
	/*}*/
	/*def *(other: Block) =*/
	/*{*/
	/*	if (!innerDimEqual(other))*/
	/*		throw BlockSizeErrorException("Matrix inner dimensions do not match: "+size()+" and "+other.size);*/
	/*	else*/
	/*	{*/
	/*		val outerID: BlockID = BlockID(id.row,other.id.col);*/
	/*		Block(outerID, mat * other.mat);*/
	/*	}*/
	/*}*/

	/*def transpose(): Block = Block(id.transpose(),mat.t);*/

	/*def isZero(): Boolean = !any(mat);*/

	/*def nrows(): Long = mat.rows.toLong;*/

	/*def ncols(): Long = mat.cols.toLong;*/

	/*def size(): BlockSize = BlockSize(mat.rows,mat.cols);*/

	/*def innerDimEqual(other: Block): Boolean = size().innerDimEqual(other.size());*/

	/*def row(): Long = id.row;*/
	/**/
	/*def col(): Long = id.col;*/
	/**/
	override def toString: String = 
	{
		val len: Int = mat.data.length;
		val rowOffset: Int = mat.rows * id.row.toInt;
		val colOffset: Int = mat.cols * id.col.toInt;

		val charsPerLine: Int = 10;
		val sb: StringBuilder = new StringBuilder(mat.data.length * charsPerLine);
		val delim: String = ",";
		val nr: Int = mat.rows;
		for ( (x,i) <- mat.data.zipWithIndex )
			sb.append( (rowOffset + i%nr) +delim+ (colOffset + i/nr) +delim+ x +"\n");
		sb.toString
	}

	/*def print() = {*/
	/*	val str: String = id.toString() + ":" + mat.data.mkString(",");*/
	/*	println(str);*/
	/*}*/
}

/*object Block */
/*{*/
/*	def fromArray(*/
/*		id: BlockID, */
/*		matSize: BlockSize, */
/*		mat: Array[Double]) = */
/*	{*/
/*		val m = matSize.nrows.toInt;*/
/*		val n = matSize.ncols.toInt;*/
/*		val block: BDM[Double] = new BDM(m,n,mat);*/
/*		Block(id, block);*/
/*	}*/
/**/
/*	def rand(*/
/*		id: BlockID, */
/*		bsize: BlockSize) = */
/*	{*/
/*		val m = bsize.nrows.toInt;*/
/*		val n = bsize.ncols.toInt;*/
/*		val mat: BDM[Double] = BDM.rand(m,n);*/
/*		Block(id,mat);*/
/*	}*/
/**/
/*	def ones(*/
/*		id: BlockID, */
/*		bsize: BlockSize) = */
/*	{*/
/*		val m = bsize.nrows.toInt;*/
/*		val n = bsize.ncols.toInt;*/
/*		val mat: BDM[Double] = BDM.ones(m,n);*/
/*		Block(id,mat);*/
/*	}*/
/**/
/*	def zeros(*/
/*		id: BlockID, */
/*		bsize: BlockSize) = */
/*	{*/
/*		val m = bsize.nrows.toInt;*/
/*		val n = bsize.ncols.toInt;*/
/*		val mat: BDM[Double] = BDM.zeros(m,n);*/
/*		Block(id,mat);*/
/*	}*/
/**/
/*	def eye(*/
/*		id: BlockID, */
/*		bsize: BlockSize) = */
/*	{*/
/*		def onDiag(id:BlockID): Boolean = (id.row == id.col);*/
/*		*/
/*		val m = bsize.nrows.toInt;*/
/*		val n = bsize.ncols.toInt;*/
/*		val mat: BDM[Double] = {*/
/*			if (onDiag(id) && m==n)*/
/*				BDM.eye[Double](m);*/
/*			else*/
/*				BDM.zeros(m,n);*/
/*		}*/
/*		Block(id,mat);*/
/*	}*/
/**/
/*}*/
