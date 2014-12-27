package himrod.block

import breeze.linalg.{DenseMatrix => BDM}
import breeze.linalg.{DenseVector => BDM}
import breeze.linalg._
import breeze.numerics

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

import org.apache.spark.mllib.random.RandomRDDs._

//============================================================
// Define an RDD formed from an array of Blocks: (BlockID, BreezeDenseMatrix[Double])
// Each block is lexicographically ordered by BlockID = (row,col)
// where:
//	0 <= row <= num_block_rows
//	0 <= col <= num_block_cols
// 
//============================================================
case class BlockVecSizeMismatchException(msg: String) extends Exception

case class BlockVec(
	val size: BlockSize, // size of matrix in blocks
	val bsize: BlockSize, //size of uniform blocks
	val blocks: RDD[ColBlock]
	) extends Serializable 
{
	def nrows(): Long = size.nrows;
	def ncols(): Long = size.nrows;
	def brows(): Long = bsize.nrows;
	def bcols(): Long = bsize.ncols;

	def getBlock(id: BlockID): RDD[ColBlock] = {
		blocks.filter(x => x.id == id)
	}
	def getBlock(row: Long, col: Long): RDD[ColBlock] = {
		blocks.filter(x => x.id == BlockID(row,col))
	}

	def numBlocks(): Long = size.nrows * size.ncols; 

	// scalar operations
	def +(a: Double): BlockVec = BlockVec(size,bsize,blocks.map(_+a));
	def -(a: Double): BlockVec = BlockVec(size,bsize,blocks.map(_-a));
	def *(a: Double): BlockVec = BlockVec(size,bsize,blocks.map(_*a));
	def /(a: Double): BlockVec = BlockVec(size,bsize,blocks.map(_/a));

	// BlockVec addition
	def +(other: BlockVec): BlockVec = 
	{
		if (size == other.size && bsize == other.bsize)
		{
			type BlockTuple = (BDM[Double],BDM[Double])

			def splitBlock(M: BlockVec): (BlockID,BDV[Double]) = (M.id, M.mat);
			def addBlocks(tuple: (BlockID,BlockTuple)): BlockVec =
			{
				val id = tuple._1;
				val A = tuple._2._1;
				val B = tuple._2._2;
				val result: BDV[Double] = A+B;
				ColBlock(id, result)
			}
			val A = blocks
				.map(splitBlock)
				.persist();
			val B = other.blocks
				.map(splitBlock)
				.persist();
			val result = A.join(B).map(addBlocks);
			BlockVec(size,bsize,result)
		}
		else
			throw BlockVecSizeMismatchException("BlockVecs are not similarly partitioned.");
	}

	//vector-matrix multiplication, v' * A
	def *(A: BlockMat) =
	{
		type BlockTuple = (ColBlock,Block)
		type BlockComponents = (BlockID, BDV[Double])

		def makeColBlock(v: BlockComponents): ColBlock = ColBlock(v._1,v._2);

		//multiply (v,A) => (ij, b_ij)
		def multiplyBlocks(tup: BlockTuple): BlockComponents = 
		{
			val v = tup._1;
			val A = tup._2;
			//transpose the size, since we don't store ColBlocks in transpose
			(v.id.product(A.id).transpose, v.vec * A.mat);
		}

		if (size.innerDimEqual(A.size)vec.innerDimEqual(A.bsize) && bsize.innerDimEqual(A.bsize))
		{
			val A = blocks.map(x => (x.col,x))
			val B = other.blocks.map(x => (x.row,x))
			val AB: RDD[Block] = A
				.join(B) // RDD[(k,(A,B))]
				.map(tup => multiplyBlocks(tup._2))
				.persist(StorageLevel.MEMORY_AND_DISK)
				.reduceByKey(_ + _)
				.map(makeColBlock)

			BlockVec(size.product(other.size),bsize.product(other.bsize),AB);
		}
		else
			throw BlockVecSizeMismatchException("BlockVecs are not similarly partitioned.");
	}

	def transpose(): BlockVec =
	{
		BlockVec(
			size.transpose(),
			bsize.transpose(),
			blocks.map(b => b.transpose())
			);
	}

	def print() = 
	{
		println("size: " + size);
		println("bsize: " + bsize);
		println("numParts: " + blocks.partitions.length);
		blocks.collect.foreach(println);
		println("");
	}

	def saveAsTextFile(fout: String) = blocks.saveAsTextFile(fout);

}
object BlockVec {

	/*type BlockSize = (Long,Long)*/
	type Index = (Long,Long)

	// get the lexicographic linear index of (i,j) in an (N,M) matrix 
	private def getID(size: BlockSize, ij: Index): Long = {
		ij._1 + (ij._2 * size.nrows);  
	}

	// return the lexicographic linear index of the block in which (i,j) entry resides
	private def getAbsBlockID(matSize: BlockSize, bsize: BlockSize, ij: Index): Long = 
	{
		val i_block: Long = bsize.nrows * (ij._1 / bsize.nrows);
		val j_block: Long = bsize.ncols * (ij._2 / bsize.ncols);
		getID(matSize,(i_block,j_block));
	}
	
	// return linear lexico. index of (i,j) relative to its block
	private def getRelBlockID(matSize: BlockSize, bsize: BlockSize, ij: Index): Long =
	{
		val i_block: Long = (ij._1 % bsize.nrows);
		val j_block: Long = (ij._2 % bsize.ncols);
		getID(bsize,(i_block,j_block));
	}

	// load a dense matrix from a text file
	def fromTextFile(
		sc: SparkContext, 
		fin: String, 
		delim: String,
		matSize: BlockSize,
		bsize: BlockSize) = 
	{
		def toBlock(x: (Long, Iterable[(Long,Double)]) ) = 
		{
			val id: BlockID = BlockID.fromID(x._1,matSize,bsize);
			val A = Array.ofDim[Double]((bsize.nrows*bsize.ncols).toInt);

			// fill the matrix A
			for (tuple <- x._2)
				A(tuple._1.toInt) = tuple._2
			
			// instantiate BreezeDenseMatrix
			val mat: BDM[Double] = new BDM(bsize.nrows.toInt,bsize.ncols.toInt,A);
			Block(id,mat);
		}

		val nblocksRow: Long = matSize.nrows / bsize.nrows;
		val nblocksCol: Long = matSize.ncols / bsize.ncols;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		// convert matrix A_ij textfile to tuple (absBlockID, relBlockID, A_ij)
		val blocks = sc.textFile(fin, numPartitions)
			.map { line => 
				val tokens = line.split(delim); 
				val ij: Index = (tokens(0).toLong, tokens(1).toLong);
				val id_abs: Long = getAbsBlockID(matSize,bsize,ij);
				val id_rel: Long = getRelBlockID(matSize,bsize,ij);
				(id_abs, (id_rel,tokens(2).toDouble) );
			}
			// groupBy the linear block index, ensuring that each block is a partition
			.groupByKey()
			.coalesce(numPartitions)
			.map(toBlock)

		val bmatSize: BlockSize = BlockSize(nblocksRow,nblocksCol); 
		BlockVec(bmatSize,bsize,blocks);
	}

	//generate random BlockVec
	def rand(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockVec =
	{
		val nblocksRow: Long = matSize.nrows / bsize.nrows;
		val nblocksCol: Long = matSize.ncols / bsize.ncols;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		def ID(n: Int): BlockID = {
			BlockID(n.toLong % nblocksRow, n.toLong / nblocksRow);
		}

		def toBDM(dat: Array[Double]): BDM[Double] = 
		{
			new BDM(
				bsize.nrows.toInt,
				bsize.ncols.toInt,
				dat
			);
		}

		def toBlock(v: (BlockID,BDM[Double])) = Block(v._1,v._2)

		val blockNums = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.map(x => ID(x._1))

		val numEls = matSize.nrows * matSize.ncols;

		val dat = normalRDD(sc,numEls,numPartitions)
			.glom
			.map(toBDM);

		val blocks = blockNums
			.zip(dat)
			.map(toBlock);

		BlockVec(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}
	def zeros(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockVec =
	{
		val nblocksRow: Long = matSize.nrows / bsize.nrows;
		val nblocksCol: Long = matSize.ncols / bsize.ncols;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		def ID(n: Int): BlockID = {
			BlockID(n.toLong % nblocksRow, n.toLong / nblocksRow);
		}

		def newBlock(it: Iterator[(Int,Int)]) = 
		{
			it.map(v => Block.zeros(ID(v._1),bsize));
		}

		val blocks = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.mapPartitions(newBlock);

		BlockVec(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}
}		
