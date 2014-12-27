package himrod.block

import breeze.linalg._
import breeze.linalg.{DenseVector => BDV}
import breeze.numerics

/*import org.apache.spark.rdd.RDD*/
/*import org.apache.spark.SparkContext*/
/*import org.apache.spark.SparkContext._*/
/*import org.apache.spark.SparkException*/

case class ColBlockMismatchException(msg: String) extends Exception
case class ColBlockSizeErrorException(msg: String) extends Exception

case class ColBlock(
	val id: BlockID,
	val vec: BDV[Double]
	) extends Serializable
{
	override def hashCode(): Int = (row() + col()*nrows()).toInt;

	def equals(other: ColBlock): Boolean =
	{
		(id == other.id) && (vec == other.vec)	
	}
	
	//scalar operations
	def +(a: Double): ColBlock = ColBlock(id, vec+a);
	def -(a: Double): ColBlock = ColBlock(id, vec-a);
	def *(a: Double): ColBlock = ColBlock(id, vec*a);
	def /(a: Double): ColBlock = ColBlock(id, vec/a);

	//=============================
	//element-wise Vector operations 
	//=============================

	//element-wise addition
	def +(other: ColBlock): ColBlock = 
	{
		if (id != other.id)
			throw ColBlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);
		else
			ColBlock(id, vec + other.vec);
	}
	//element-wise subtraction
	def -(other: ColBlock): ColBlock = 
	{
		if (id != other.id)
			throw ColBlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);
		else
			ColBlock(id, vec - other.vec);
	}
	//element-wise multiplication
	def *(other: ColBlock) =
	{
		if (id != other.id)
			throw ColBlockMismatchException("Vector inner dimensions do not match: "+size()+" and "+other.size);
		else
		{
			val outerID: BlockID = BlockID(id.row,other.id.col);
			ColBlock(outerID, vec :* other.vec);
		}
	}

	//=============================
	// vector operations
	//=============================

	//=============================
	// dot product: dot(v,u) =  v' * u
	def dot(other: ColBlock): Double = 
	{
		if (id != other.id)
			throw ColBlockMismatchException("BlockIDs do not match: " + id + " and " + other.id);
		else
			vec dot other.vec;
	}

	//=============================
	// vector-matrix multiplication; multiply v.transpose by A:
	// v' * A 
	def *(A: Block): ColBlock =
	{
		if (length() != A.nrows())
			throw ColBlockSizeErrorException("Vector length does not match matrix inner dimensions: " + vec.length+" and "+A.size);
		else
		{
			val outerID: BlockID = BlockID(id.row,A.id.col);
			ColBlock(outerID, (vec.t * A.mat).t);
		}
	}

	// default to 2-norm
	def vectorNorm(): Double = norm(vec);

	// compute p-norm
	def vectorNorm(p: Int): Double = norm(vec,p);

	//=============================
	def isZero(): Boolean = !any(vec);

	def length(): Long = vec.length.toLong;

	def size(): BlockSize = BlockSize(vec.length.toLong,1L);

	def nrows(): Long = length();

	def ncols(): Long = 1L;

	// check that inner prods are equal for vector-matrix multiplication
	def innerDimEqual(A: Block): Boolean = (vec.length == A.nrows);

	def row(): Long = id.row;
	
	def col(): Long = id.col;
	
	override def toString: String = {
		id.toString() + ":" + vec.data.mkString(",");
	}

	def print() = {
		println(id);
		println(vec);
	}
}

object ColBlock 
{
	def fromArray(
		id: BlockID, 
		len: Int, 
		vec: Array[Double]) = 
	{
		val block: BDV[Double] = new BDV(vec);
		ColBlock(id, block);
	}

	//random generator doesn't work due to dependencies on commons.math3
	def rand(
		id: BlockID, 
		len: Int) = 
	{
		ColBlock.zeros(id,len);
	}

	def zeros(
		id: BlockID, 
		len: Int) = 
	{
		val vec: BDV[Double] = BDV.zeros[Double](len);
		ColBlock(id,vec);
	}
}
