package himrod.block

import breeze.linalg.{DenseVector => BDV}
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
// Define an RDD formed from an array of ColBlocks: (BlockID, BreezeDenseVector[Double])
// Each block is lexicographically ordered by BlockID = (row,col)
// where:
//	0 <= row <= num_block_rows
//	col = 0 -- this is here for compatibility with BlockMat
// 
//============================================================
case class BlockVecSizeMismatchException(msg: String) extends Exception

case class BlockVec(
	val size: BlockSize, // size of matrix in blocks
	val bsize: BlockSize, //size of uniform blocks
	val blocks: RDD[ColBlock] // RDD of vector blocks 
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
	/*def +(other: BlockVec): BlockVec = */
	/*{*/
	/*	if (size == other.size && bsize == other.bsize)*/
	/*	{*/
	/*		type BlockTuple = (BDV[Double],BDV[Double])*/

	/*		def splitBlockVec(v: ColBlock): (BlockID,BDV[Double]) = (v.id, v.vec);*/

	/*		def addBlockVecs(tuple: (BlockID,BlockTuple)): ColBlock =*/
	/*		{*/
	/*			val id = tuple._1;*/
	/*			val u = tuple._2._1;*/
	/*			val v = tuple._2._2;*/
	/*			val result: BDV[Double] = u + v;*/
	/*			ColBlock(id, result)*/
	/*		}*/
	/*		val A = blocks*/
	/*			.map(splitBlockVec)*/
	/*			.persist();*/
	/*		val B = other.blocks*/
	/*			.map(splitBlockVec)*/
	/*			.persist();*/
	/*		val result = A.join(B).map(addBlockVecs);*/
	/*		BlockVec(size,bsize,result)*/
	/*	}*/
	/*	else*/
	/*		throw BlockVecSizeMismatchException("BlockVecs are not similarly partitioned.");*/
	/*}*/

	def unzip(): RDD[(BlockID,BDV[Double])] = blocks.map(x => (x.id,x.vec) )

	def dot(other: BlockVec): Double =
	{
		type VecTuple = (BDV[Double], BDV[Double])

		def dotFunc(tuple: (BlockID,VecTuple)): Double = 
		{
			val u = (tuple._2)._1;
			val v = (tuple._2)._2;
			u dot v;
		}

		val uv = (this.unzip)
			.join(other.unzip)
			.map(dotFunc);

		uv.reduce(_+_);
	}

	//element-wise operation f(u_1,u_2) = v
	def forEach(other: BlockVec, f: (BDV[Double],BDV[Double]) => BDV[Double] ) =
	{
		type VecTuple = (BDV[Double], BDV[Double])

		def applyFunc(tuple: (BlockID,VecTuple)): ColBlock = 
		{
			val u = (tuple._2)._1;
			val v = (tuple._2)._2;
			ColBlock(tuple._1, f(u,v));
		}

		if (size == other.size && bsize == other.bsize)
		{
			val uv = (this.unzip)
				.join(other.unzip)
				.map(applyFunc);

			BlockVec(size,bsize,uv);
		}
		else
			throw BlockVecSizeMismatchException("BlockVecs are not similarly partitioned.");
	}

	def *(other: BlockVec): BlockVec =
	{
		def multFunc = (u: BDV[Double],v: BDV[Double]) => u :* v
		forEach(other,multFunc);
	}
	def +(other: BlockVec): BlockVec =
	{
		def addFunc = (u: BDV[Double],v: BDV[Double]) => u + v
		forEach(other,addFunc);
	}
	def -(other: BlockVec): BlockVec =
	{
		def subtractFunc = (u: BDV[Double],v: BDV[Double]) => u - v
		forEach(other,subtractFunc);
	}
	def /(other: BlockVec): BlockVec =
	{
		def divFunc = (u: BDV[Double],v: BDV[Double]) => u / v
		forEach(other,divFunc);
	}

	//vector-matrix multiplication, v' * A
	def *(M: BlockMat): BlockVec =
	{
		type VecMatTuple = (ColBlock,Block)
		type BlockVecComponents = (BlockID, BDV[Double])

		def makeColBlock(v: BlockVecComponents): ColBlock = ColBlock(v._1,v._2);

		def multiplyBlocks(tup: VecMatTuple): BlockVecComponents = 
		{
			val v = tup._1;
			val A = tup._2;
			(v.id.product(A.id).transpose, (v.vec.t * A.mat).t);
		}

		// compare the number of blocks
		if ((size.transpose.innerDimEqual(M.size)) && (bsize.transpose.innerDimEqual(M.bsize)))
		{
			val v = blocks.map(x => (x.col,x))
			val A = M.blocks.map(x => (x.row,x))
			val vA: RDD[ColBlock] = v
				.join(A) // RDD[(k,(A,B))]
				.map(tup => multiplyBlocks(tup._2))
				.persist(StorageLevel.MEMORY_AND_DISK)
				.reduceByKey(_ + _)
				.map(makeColBlock)

			val newSize: BlockSize = size
				.transpose
				.product(M.size)
				.transpose

			val newBSize: BlockSize = bsize
				.transpose
				.product(M.size)
				.transpose

			BlockVec(newSize,newBSize,vA);
		}
		else
			throw BlockVecSizeMismatchException("BlockVecs are not similarly partitioned.");
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

	def fromTextFile(
		sc: SparkContext, 
		fin: String, 
		delim: String,
		vecSize: Long,
		bsize: Long): BlockVec = 
	{
		def toBlock(x: (Long, Iterable[(Long,Double)]) ) = 
		{
			val id: BlockID = BlockID(x._1,0L);
			val v: Array[Double] = Array.fill[Double](bsize.toInt)(0);

			// fill the nonzero values of vector v
			for (tuple <- x._2)
				v(tuple._1.toInt) = tuple._2
			
			val vec: BDV[Double] = new BDV(v);
			ColBlock(id,vec);
		}

		val numPartitions: Int = (vecSize / bsize).toInt;

		// convert vector v_i textfile to tuple (absBlockID, relBlockID, A_ij)
		val blocks = sc.textFile(fin, numPartitions)
			.map { line => 
				val tokens = line.split(delim); 
				val id_abs: Long = tokens(0).toLong;
				val id_rel: Long = id_abs % bsize;
				(id_abs, (id_rel,tokens(2).toDouble) );
			}
			// groupBy the linear block index, ensuring that each block is a partition
			.groupByKey()
			.coalesce(numPartitions)
			.map(toBlock)

		BlockVec(
			BlockSize(numPartitions,1L),
			BlockSize(bsize,1L),
			blocks);
	}

	//generate random BlockVec
	def rand(sc: SparkContext, vecSize: Long, bsize: Long): BlockVec =
	/*def rand(sc: SparkContext, matSize: BlockSize, bsize: BlockSize): BlockVec =*/
	{
		val numPartitions: Int = (vecSize / bsize).toInt;
		/*val nblocksCol: Long = matSize.ncols / bsize.ncols;*/
		/*val numPartitions: Int = (nblocksRow * nblocksCol).toInt;*/

		def ID(n: Int): BlockID = {
			BlockID(n.toLong % numPartitions, 1L);
		}

		def toBDV(dat: Array[Double]): BDV[Double] = new BDV(dat);

		def toColBlock(v: (BlockID,BDV[Double])) = ColBlock(v._1,v._2)

		val blockNums = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.map(x => ID(x._1))

		val dat = normalRDD(sc,vecSize,numPartitions)
			.glom
			.map(toBDV);

		val blocks = blockNums
			.zip(dat)
			.map(toColBlock);

		BlockVec(
			BlockSize(numPartitions,1L),
			BlockSize(bsize,1L),
			blocks);
	}

	def zeros(sc: SparkContext, vecSize: Long, bsize: Long): BlockVec =
	{
		/*val nblocksCol: Long = matSize.ncols / bsize.ncols;*/
		/*val numPartitions: Int = (nblocksRow * nblocksCol).toInt;*/
		val numPartitions: Int = (vecSize / bsize).toInt;

		def ID(n: Int): BlockID = {
			BlockID(n.toLong % numPartitions, 1L);
		}

		def newBlock(it: Iterator[(Int,Int)]) = {
			it.map(v => ColBlock.zeros(ID(v._1),bsize.toInt));
		}

		val blocks = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.mapPartitions(newBlock);

		BlockVec(
			BlockSize(vecSize,1L),
			BlockSize(bsize,1L),
			blocks);
	}
}		
