package himrod.block

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
	val blocks: RDD[Block]
	) extends Serializable 
{
	def nrows(): Long = size.nrows;
	def ncols(): Long = size.nrows;
	def brows(): Long = bsize.nrows;
	def bcols(): Long = bsize.ncols;

	def getBlock(id: BlockID): RDD[Block] = {
		blocks.filter(x => x.id == id)
	}
	def getBlock(row: Long, col: Long): RDD[Block] = {
		blocks.filter(x => x.id == BlockID(row,col))
	}

	def numBlocks(): Long = size.nrows * size.ncols; 

	// scalar operations
	def +(a: Double): BlockMat = BlockMat(size,bsize,blocks.map(_+a));
	def -(a: Double): BlockMat = BlockMat(size,bsize,blocks.map(_-a));
	def *(a: Double): BlockMat = BlockMat(size,bsize,blocks.map(_*a));
	def /(a: Double): BlockMat = BlockMat(size,bsize,blocks.map(_/a));

	//elementwise operations
	def forEach(other: BlockMat, f: (BDM[Double],BDM[Double]) => BDM[Double]): BlockMat = 
	{
		type MatTuple = (BDM[Double],BDM[Double])

		def applyFunc(tuple: (BlockID,MatTuple)): Block =
		{
			val id = tuple._1;
			val A: BDM[Double] = tuple._2._1;
			val B: BDM[Double] = tuple._2._2;
			Block(id, f(A,B));
		}

		if (size == other.size && bsize == other.bsize)
		{
			val A = (this.unzip).persist();
			val B = (other.unzip).persist();
			val result = A
				.join(B)
				.map(applyFunc);

			BlockMat(size,bsize,result)
		}
		else
			throw BlockMatSizeMismatchException("BlockMats are not similarly partitioned.");
	}

	def -(other: BlockMat): BlockMat =
	{
		def subtractFunc = (A: BDM[Double],B: BDM[Double]) => A - B
		forEach(other,subtractFunc);
	}

	// BlockMat addition
	def +(other: BlockMat): BlockMat = 
	{
		if (size == other.size && bsize == other.bsize)
		{
			type BlockTuple = (BDM[Double],BDM[Double])

			def splitBlock(M: Block): (BlockID,BDM[Double]) = (M.id, M.mat);
			def addBlocks(tuple: (BlockID,BlockTuple)): Block =
			{
				val id = tuple._1;
				val A = tuple._2._1;
				val B = tuple._2._2;
				val result: BDM[Double] = A+B;
				Block(id, result)
			}

			val A = blocks
				.map(splitBlock)
				.persist();
			val B = other.blocks
				.map(splitBlock)
				.persist();
			val result = A.join(B).map(addBlocks);
			BlockMat(size,bsize,result)
		}
		else
			throw BlockMatSizeMismatchException("BlockMats are not similarly partitioned.");
	}

	def *(other: BlockMat) =
	{
		type BlockTuple = (Block,Block)
		type BlockComponents = (BlockID, BDM[Double])

		def makeBlock(v: BlockComponents): Block = Block(v._1,v._2);

		def multiplyBlocks(tup: BlockTuple): BlockComponents = 
		{
			val A = tup._1;
			val B = tup._2;
			(A.id.product(B.id), A.mat * B.mat);
		}

		if (size.innerDimEqual(other.size) && bsize.innerDimEqual(other.bsize))
		{
			val A = blocks.map(x => (x.col,x))
			val B = other.blocks.map(x => (x.row,x))
			val AB: RDD[Block] = A
				.join(B) // RDD[(k,(A,B))]
				.map(tup => multiplyBlocks(tup._2))
				.persist(StorageLevel.MEMORY_AND_DISK)
				.reduceByKey(_ + _)
				.map(makeBlock)

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

	def unzip(): RDD[(BlockID,BDM[Double])] = blocks.map(x => (x.id,x.mat) )

	def saveAsTextFile(fout: String) = blocks.saveAsTextFile(fout);

}
object BlockMat {

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
			val A = Array.fill[Double]((bsize.nrows*bsize.ncols).toInt)(0);

			// fill the matrix A
			for (tuple <- x._2)
				A(tuple._1.toInt) = tuple._2
			
			// instantiate BreezeDenseMatrix
			val mat: BDM[Double] = new BDM(bsize.nrows.toInt,bsize.ncols.toInt,A);
			Block(id,mat);
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

		def ID(n: Int): BlockID = {
			BlockID(n.toLong % nblocksRow, n.toLong / nblocksRow);
		}

		/*def toBDM(it: Iterator[Double]): Iterator[BDM[Double]] =*/
		/*{*/
			/*val b: BDM[Double] = new BDM(*/
			/*	bsize.nrows.toInt,*/
			/*	bsize.ncols.toInt,*/
			/*	it.toArray*/
			/*);*/
			/*b;*/
		/*}*/

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

		BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}
	/*def rand(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =*/
	/*{*/
	/*	val nblocksRow: Long = matSize.nrows / bsize.nrows;*/
	/*	val nblocksCol: Long = matSize.ncols / bsize.ncols;*/
	/*	val numPartitions: Int = (nblocksRow * nblocksCol).toInt;*/

	/*	def ID(n: Int): BlockID = {*/
	/*		BlockID(n.toLong % nblocksRow, n.toLong / nblocksRow);*/
	/*	}*/

	/*	val blocks = sc*/
	/*		.parallelize(0 to numPartitions-1, numPartitions)*/
	/*		.map(x => Block.rand(ID(x),bsize));*/

	/*	BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);*/
	/*}*/

	def zeros(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =
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

		BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}

	// only works for square matrices w/ square blocks
	def eye(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockMat =
	{
		val nblocksRow: Long = matSize.nrows / bsize.nrows;
		val nblocksCol: Long = matSize.ncols / bsize.ncols;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		def ID(n: Int): BlockID = {
			BlockID(n.toLong % nblocksRow, n.toLong / nblocksRow);
		}

		val blocks = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => Block.eye(ID(x),bsize));

		BlockMat(BlockSize(nblocksRow,nblocksCol),bsize,blocks);
	}
}		

/*def *(other: BlockMat) =*/
/*{*/
/*	type BlockTuple = (List[Iterable[Block]],List[Iterable[Block]])*/

/*	def inRow(b: Block, row: Int): Boolean = (b.row.toInt == row);*/
/*	def inCol(b: Block, col: Int): Boolean = (b.col.toInt == col);*/

/*	def getCol(b: Block) = b.id.col;*/
/*	def getRow(b: Block) = b.id.row;*/

/*	val nrows = other.size.nrows.toInt;*/
/*	val B_rows: Array[RDD[Block]] = Array.ofDim[RDD[Block]](nrows);*/
/*	for (i <- 0 to nrows-1)*/
/*		B_rows(i) = other.blocks.filter(x => inRow(x,i))*/
/*	*/
/*	val AB = A.mapPartitions( { iter => */
/*		val col: Int = iter.col.toInt*/
/*		iter.*/
/*	}, preservesPartitioning = false);*/

/*def cartesianMultiply(tup: (BlockTuple)) =*/
/*{*/
/*	// loop over all blocks */
/*	for (a <- tup._1.toList; b <- tup._2.toList)*/
/*		yield(a*b)*/
/*}*/
/*Brows: Array[RDD[Block]] =*/
/*{*/
/*	for ()*/
/*}*/

/*val numSplits: Int = numBlocks().toInt;*/
/*// form two key-value rdds with their blocks' row/cols as the key*/
/*val A = blocks*/
/*	.map(b => (getCol(b),b))*/
/*	.groupByKey(numSplits); //RDD[(k,Seq[Block])]*/
/*val B = other.blocks*/
/*	.map(b => (getRow(b),b))*/
/*	.groupByKey(numSplits); //RDD[(k,Seq[Block])]*/

/*// group A,B so that A.rows match B.cols*/
/*val C = A*/
/*	.cogroup(B) // RDD[(k,(Seq[Block],Seq[Block])]*/
/*	.map {x => */
/*		val blocks = x._2*/
/*		cartesianMultiply( (blocks._1.toList, blocks._2.toList))*/
/*	}*/
/*C*/
