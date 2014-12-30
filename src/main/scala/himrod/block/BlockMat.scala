package himrod.block

import breeze.linalg.{DenseVector => BDV}
import breeze.linalg.{DenseMatrix => BDM}
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
case class BlockMatSizeMismatchException(msg: String) extends Exception

case class BlockMat(
	val size: BlockSize, // size of matrix in blocks
	val bsize: BlockSize, //size of uniform blocks
	val blocks: RDD[(BlockID,BDM[Double])]
	) extends Serializable 
{
	/*override def equals(other: Any): Boolean = */
	/*{*/
	/*	match other*/
	/*	{*/
	/*		case B: BlockMat => {*/
	/*			(size == B.size) && (bsize == B.bsize) && blocks*/
	/*		}*/
	/*	}*/
	/*}*/

	def nrows(): Long = size.nrows;
	def ncols(): Long = size.nrows;
	def brows(): Long = bsize.nrows;
	def bcols(): Long = bsize.ncols;

	/*def getBlock(id: BlockID): RDD[Block] = {*/
	/*	blocks.filter(x => x.id == id)*/
	/*}*/
	/*def getBlock(row: Long, col: Long): RDD[Block] = {*/
	/*	blocks.filter(x => x.id == BlockID(row,col))*/
	/*}*/

	def numBlocks(): Long = size.nrows * size.ncols; 

	// scalar operations
	def scalarMap(f: (BDM[Double]) => BDM[Double]): BlockMat = {
		BlockMat(size,bsize,blocks.mapValues{f} );
	}

	def +(a: Double): BlockMat = 
	{
		val f = (m: BDM[Double]) => m+a;
		scalarMap(f);
	}
	def -(a: Double): BlockMat = 
	{
		val f = (m: BDM[Double]) => m-a;
		scalarMap(f);
	}
	def *(a: Double): BlockMat = 
	{
		val f = (m: BDM[Double]) => m*a;
		scalarMap(f);
	}
	def /(a: Double): BlockMat = 
	{
		val f = (m: BDM[Double]) => m/a;
		scalarMap(f);
	}

	//===================================
	//elementwise operations
	//===================================
	def forEach(other: BlockMat, f: (BDM[Double],BDM[Double]) => BDM[Double]): BlockMat = 
	{
		type MatIter = Iterable[BDM[Double]];
		type MatIterTuple = (MatIter,MatIter);

		def applyFunc(pair: MatIterTuple) =
		{
			val firstEmpty = pair._1.isEmpty;
			val secondEmpty = pair._2.isEmpty;

			if (firstEmpty && !secondEmpty)
			{
				val A = BDM.zeros[Double](size.nrows.toInt,size.ncols.toInt);
				for (B <- pair._2)
					yield f(A,B);
			}
			else if (!firstEmpty && secondEmpty)
			{
				val B = BDM.zeros[Double](size.nrows.toInt,size.ncols.toInt);
				for (A <- pair._1)
					yield f(A,B);
			}
			else if (!firstEmpty && !secondEmpty)
			{
				for (A <- pair._1; B <- pair._2)
					yield f(A,B);
			}
			// this returns nothing, since both Iterators are empty
			// but we need to yield something
			else 
			{
				for (A <- pair._1; B <- pair._2)
					yield f(A,B);
			}
		}

		if (size == other.size && bsize == other.bsize)
		{
			val result = blocks
				.cogroup(other.blocks)
				.flatMapValues{applyFunc};

			BlockMat(size,bsize,result)
		}
		else
			throw BlockMatSizeMismatchException("BlockMats are not similarly partitioned.");
	}

	def -(other: BlockMat): BlockMat =
	{
		val f = (A: BDM[Double],B: BDM[Double]) => A - B
		forEach(other,f);
	}
	def +(other: BlockMat): BlockMat =
	{
		val f = (A: BDM[Double],B: BDM[Double]) => A + B
		forEach(other,f);
	}
	def /(other: BlockMat): BlockMat =
	{
		val f = (A: BDM[Double],B: BDM[Double]) => A :/ B
		forEach(other,f);
	}
	def *(other: BlockMat): BlockMat =
	{
		val f = (A: BDM[Double],B: BDM[Double]) => A :* B
		forEach(other,f);
	}
	
	// matrix product <v,Av>
	def matProduct(vec: BlockVec): Double = vec.dot(this.multiply(vec))

	// matrix-vector multiplication
	def multiply(vec: BlockVec): BlockVec =
	{
		type MatVecTuple = ((BlockID,BDM[Double]),(BlockID,BDV[Double]))

		def multiplyBlocks(tup: MatVecTuple): (BlockID, BDV[Double]) = 
		{
			val A = tup._1;
			val v = tup._2;
			(A._1.product(v._1), A._2 * v._2);
		}

		if (size.innerDimEqual(vec.size) && bsize.innerDimEqual(vec.bsize))
		{
			val A = blocks.map(x => (x._1.col,x))
			val v = vec.blocks.map(x => (x._1.row,x))
			val Av: RDD[(BlockID,BDV[Double])] = A
				.join(v) 
				.map(tup => multiplyBlocks(tup._2))
				.persist(StorageLevel.MEMORY_AND_DISK)
				.reduceByKey(_ + _)
				.partitionBy(new HashPartitioner(vec.size.nrows.toInt))

			BlockVec(size.product(vec.size),bsize.product(vec.bsize),Av);
		}
		else
			throw BlockMatSizeMismatchException("BlockMats are not similarly partitioned.");
	}

	def multiply(other: BlockMat): BlockMat =
	{
		type MatTuple = ((BlockID,BDM[Double]),(BlockID,BDM[Double]))

		def multiplyBlocks(tup: MatTuple): (BlockID,BDM[Double]) = 
		{
			val A = tup._1;
			val B = tup._2;
			(A._1.product(B._1), A._2 * B._2);
		}

		if (size.innerDimEqual(other.size) && bsize.innerDimEqual(other.bsize))
		{
			val A = blocks.map(x => (x._1.col,x))
			val B = other.blocks.map(x => (x._1.row,x))
			val AB: RDD[(BlockID,BDM[Double])] = A
			/*val AB = A*/
				.join(B) 
				.map(tup => multiplyBlocks(tup._2))
				.persist(StorageLevel.MEMORY_AND_DISK)
				.reduceByKey(_ + _)
				/*.partitionBy(new HashPartitioner(size.nrows * other))*/

			BlockMat(size.product(other.size),bsize.product(other.bsize),AB);
		}
		else
			throw BlockMatSizeMismatchException("BlockMats are not similarly partitioned.");
	}

	def transpose(): BlockMat =
	{
		BlockMat(
			size.transpose(),
			bsize.transpose(),
			blocks.map{ b => (b._1.transpose(),b._2.t) }
		);
	}

	// print small matrices for test only
	def print() = 
	{
		println("size: " + size);
		println("bsize: " + bsize);
		println("numParts: " + blocks.partitions.length);
		blocks
			.map{x => PrintBlock(x._1,x._2)}
			.collect
			.foreach(println);
		println();
	}

	def saveAsTextFile(fout: String) = 
	{
		blocks
			.map{x => PrintBlock(x._1,x._2)}
			.saveAsTextFile(fout);
	}

}
object BlockMat {

	/*type BlockSize = (Long,Long)*/
	private type Index = (Long,Long)

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
		def toBlock(x: (Long, Iterable[(Long,Double)]) ): (BlockID, BDM[Double]) = 
		{
			val id: BlockID = BlockID.fromID(x._1,matSize,bsize);
			val A = Array.ofDim[Double]((bsize.nrows*bsize.ncols).toInt);

			// fill the matrix A
			for (tuple <- x._2)
				A(tuple._1.toInt) = tuple._2
			
			// instantiate BreezeDenseMatrix
			val mat: BDM[Double] = new BDM(bsize.nrows.toInt,bsize.ncols.toInt,A);
			(id,mat);
		}

		val nblocksRow: Long = (1.0 * matSize.nrows / bsize.nrows).ceil.toLong;
		val nblocksCol: Long = (1.0 * matSize.ncols / bsize.ncols).ceil.toLong;
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
		BlockMat(bmatSize,bsize,blocks);
	}

	//generate random BlockMat
	def rand(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =
	{
		val nblocksRow: Long = matSize.nrows / bsize.nrows;
		val nblocksCol: Long = matSize.ncols / bsize.ncols;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		def ID(n: Int): BlockID = BlockID(
			n.toLong % nblocksRow, 
			n.toLong / nblocksRow,
			nblocksRow,
			nblocksCol
		);

		def toBDM(dat: Array[Double]): BDM[Double] = 
		{
			new BDM(
				bsize.nrows.toInt,
				bsize.ncols.toInt,
				dat
			);
		}

		val blockNums = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.map(x => ID(x._1))

		val numEls = matSize.nrows * matSize.ncols;

		val dat = normalRDD(sc,numEls,numPartitions)
			.glom
			.map(toBDM);

		val blocks: RDD[(BlockID,BDM[Double])] = blockNums
			.zip(dat)

		BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}

	// create uniform BlockMat with given generator function f()=>BDM[Double]
	def generate(
		sc: SparkContext,
		matSize: BlockSize,
		bsize: BlockSize,
		f: () => BDM[Double] ): BlockMat =
	{
		val nblocksRow: Long = matSize.nrows / bsize.nrows;
		val nblocksCol: Long = matSize.ncols / bsize.ncols;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		def genNewID(n: Int): BlockID = BlockID(
			n.toLong % nblocksRow, 
			n.toLong / nblocksRow,
			nblocksRow,
			nblocksCol
		);

		def genNewBlock(it: Iterator[(Int,Int)]): Iterator[(BlockID,BDM[Double])] = {
			it.map(v => (genNewID(v._1), f() ));
		}

		val blocks: RDD[(BlockID,BDM[Double])] = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.mapPartitions(genNewBlock);

		BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}

	def ones(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =
	{
		val f = () => BDM.ones[Double](bsize.nrows.toInt,bsize.ncols.toInt);
		BlockMat.generate(sc,matSize,bsize,f);
	}
	def zeros(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =
	{
		val f = () => BDM.zeros[Double](bsize.nrows.toInt,bsize.ncols.toInt);
		BlockMat.generate(sc,matSize,bsize,f);
	}

	def fill(
		sc: SparkContext, 
		matSize: BlockSize,
		bsize: BlockSize,
		a: Double): BlockMat =
	{
		val r = bsize.nrows.toInt;
		val c = bsize.ncols.toInt;
		val f = () => BDM.fill[Double](r,c){a};
		BlockMat.generate(sc,matSize,bsize,f);
	}

	// only works for square matrices w/ square blocks
	/*def eye(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =*/
	/*{*/
	/*	val nblocksRow: Long = matSize.nrows / bsize.nrows;*/
	/*	val nblocksCol: Long = matSize.ncols / bsize.ncols;*/
	/*	val numPartitions: Int = (nblocksRow * nblocksCol).toInt;*/

	/*	def ID(n: Int): BlockID = {*/
	/*		BlockID(n.toLong % nblocksRow, n.toLong / nblocksRow);*/
	/*	}*/

	/*	val blocks = sc*/
	/*		.parallelize(0 to numPartitions-1, numPartitions)*/
	/*		.map(x => Block.eye(ID(x),bsize));*/

	/*	BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);*/
	/*}*/
}		
