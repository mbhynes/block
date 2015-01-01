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
	val blocks: RDD[(BlockID,BDV[Double])] // RDD of vector blocks 
	) extends Serializable 
{
	/*def persist() = A.persist(StorageLevel.MEMORY_AND_DISK_SER);*/

	def nrows(): Long = size.nrows;
	def ncols(): Long = size.nrows;
	def brows(): Long = bsize.nrows;
	def bcols(): Long = bsize.ncols;

	/*def getBlock(id: BlockID): RDD[ColBlock] = {*/
	/*	blocks.filter(x => x.id == id)*/
	/*}*/
	/*def getBlock(row: Long, col: Long): RDD[ColBlock] = {*/
	/*	blocks.filter(x => x.id == BlockID(row,col))*/
	/*}*/

	def numBlocks(): Long = size.nrows * size.ncols; 

	// scalar operations
	def scalarMap(f: (BDV[Double]) => BDV[Double]): BlockVec = {
		BlockVec(size,bsize,blocks.mapValues{f});
	}
	def +(a: Double): BlockVec = 
	{
		val f = (m: BDV[Double]) => m+a;
		scalarMap(f);
	}
	def -(a: Double): BlockVec = 
	{
		val f = (m: BDV[Double]) => m-a;
		scalarMap(f);
	}
	def *(a: Double): BlockVec = 
	{
		val f = (m: BDV[Double]) => m*a;
		scalarMap(f);
	}
	def /(a: Double): BlockVec = 
	{
		val f = (m: BDV[Double]) => m/a;
		scalarMap(f);
	}

	/*def unzip(): RDD[(BlockID,BDV[Double])] = blocks.map(x => (x.id,x.vec) )*/
	def dot(other: BlockVec): Double =
	{
		type VecTuple = (BDV[Double],BDV[Double])

		def dotFunc(tuple: (BlockID,VecTuple)): Double = 
		{
			val u = (tuple._2)._1;
			val v = (tuple._2)._2;
			u dot v;
		}

		val uv: RDD[Double] = blocks
			.join(other.blocks)
			.map(dotFunc);

		uv.reduce(_+_);
	}

	//element-wise operation f(u_1,u_2) = v
	def forEach(other: BlockVec, f: (BDV[Double],BDV[Double]) => BDV[Double] ) =
	{
		type VecIter = Iterable[BDV[Double]];
		type VecIterTuple = (VecIter,VecIter);

		def applyFunc(pair: VecIterTuple) = 
		{
			val firstEmpty = pair._1.isEmpty;
			val secondEmpty = pair._2.isEmpty;

			if (firstEmpty && !secondEmpty)
			{
				val u = BDV.zeros[Double](size.nrows.toInt);
				for (v <- pair._2)
					yield f(u,v);
			}
			else if (!firstEmpty && secondEmpty)
			{
				val v = BDV.zeros[Double](size.nrows.toInt);
				for (u <- pair._1)
					yield f(u,v);
			}
			else if (!firstEmpty && !secondEmpty)
			{
				for (u <- pair._1; v <- pair._2)
					yield f(u,v);
			}
			else // this returns nothing, since both Iterators are empty
			{
				for (u <- pair._1; v <- pair._2)
					yield f(u,v);
			}
		}

		if (size == other.size && bsize == other.bsize)
		{
			val uv: RDD[(BlockID,BDV[Double])] = blocks
				.cogroup(other.blocks)
				.flatMapValues{applyFunc}

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
	def multiply(M: BlockMat): BlockVec =
	{
		type VecMatTuple = ((BlockID,BDV[Double]),(BlockID,BDM[Double]))
		/*type BlockVecComponents = (BlockID, BDV[Double])*/

		/*def makeColBlock(v: BlockVecComponents): ColBlock = ColBlock(v._1,v._2);*/

		def multiplyBlocks(tup: VecMatTuple): (BlockID, BDV[Double]) = 
		{
			val v = tup._1;
			val A = tup._2;
			((v._1.transpose.product(A._1)).transpose, (v._2.t * A._2).t);
		}

		// compare the number of blocks
		if ((size.transpose.innerDimEqual(M.size)) && (bsize.transpose.innerDimEqual(M.bsize)))
		{
			// treat v as v.transpose
			val v = blocks.map(x => (x._1.row,x))
			val A = M.blocks.map(x => (x._1.row,x))
			val vA: RDD[(BlockID,BDV[Double])] = v
				.join(A) // RDD[(k,(A,B))]
				.persist(StorageLevel.MEMORY_AND_DISK_SER)
				.map{x => multiplyBlocks(x._2)}
				.reduceByKey(_ + _)
				.partitionBy(new HashPartitioner(size.nrows.toInt))

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

	// metrics + operations
	def mapBlocksToScalar(f: (BDV[Double]) => Double): RDD[Double] = {
		blocks.map{x => f(x._2)};
	}

	def mapBlocks(f: (BDV[Double]) => BDV[Double]): BlockVec = {
		BlockVec(size,bsize,blocks.mapValues{f});
	}

	def print() = 
	{
		println("size: " + size);
		println("bsize: " + bsize);
		println("numParts: " + blocks.partitions.length);
		blocks
			.map(x => PrintColBlock(x._1,x._2))
			.collect
			.foreach(println);
		println("");
	}

	def saveAsTextFile(fout: String) = 
	{
		blocks.map(x => PrintColBlock(x._1,x._2))
		.saveAsTextFile(fout);
	}
}

object BlockVec {

	def fromTextFile(
		sc: SparkContext, 
		fin: String, 
		delim: String,
		vecSize: Long,
		bsize: Long): BlockVec = 
	{
		val numPartitions: Int = (1.0 * vecSize / bsize).ceil.toInt;

		def toBlock(x: (Long, Iterable[(Long,Double)]) ): (BlockID, BDV[Double]) = 
		{
			val id: BlockID = BlockID(x._1,0L,numPartitions.toLong,1L);
			val v: Array[Double] = Array.fill[Double](bsize.toInt)(0);

			// fill the nonzero values of vector v
			for (tuple <- x._2)
				v(tuple._1.toInt) = tuple._2
			
			(id,BDV(v));
		}
		// convert vector v_i textfile to tuple (absBlockID, relBlockID, A_ij)
		val blocks = sc.textFile(fin, numPartitions)
			.map{ line => 
				val tokens = line.split(delim); 
				val i: Long = tokens(0).toLong;
				val id_abs: Long = i / bsize; //block number
				val id_rel: Long = i % bsize;    //position in block
				(id_abs, (id_rel,tokens(1).toDouble) );
			}
			// groupBy the linear block index, ensuring that each block is a partition
			.groupByKey()
			.coalesce(numPartitions)
			.map(toBlock)
			.persist(StorageLevel.MEMORY_AND_DISK_SER);

		BlockVec(
			BlockSize(numPartitions.toLong,1L),
			BlockSize(bsize,1L),
			blocks);
	}


	//generate random BlockVec---this has to be done separately since
	//breeze has a problem dependency with its random generator
	def rand(sc: SparkContext, vecSize: Long, bsize: Long): BlockVec =
	{
		val numPartitions: Int = (1.0 * vecSize / bsize).ceil.toInt;

		def genNewID(n: Int): BlockID = BlockID(
			n.toLong % numPartitions, 
			0L,
			numPartitions,
			1L
		);

		def genNewBlock(dat: Array[Double]): BDV[Double] = new BDV(dat);

		val blockNums: RDD[BlockID] = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map(x => (x,x))
			.partitionBy(new HashPartitioner(numPartitions))
			.map(x => genNewID(x._1))

		val dat: RDD[BDV[Double]] = uniformRDD(sc,vecSize,numPartitions)
			.glom
			.map(genNewBlock);

		val blocks: RDD[(BlockID,BDV[Double])] = blockNums
			.zip(dat);

		BlockVec(
			BlockSize(numPartitions,1L),
			BlockSize(bsize,1L),
			blocks);
	}

	// general generator function using a supplied f() = BDV[Double]
	def generate(
		sc: SparkContext, 
		vecSize: Long, 
		bsize: Long,
		f:() => BDV[Double]): BlockVec =
	{
		val numPartitions: Int = (1.0 * vecSize / bsize).ceil.toInt;

		def genNewID(n: Int): BlockID = BlockID(
			n.toLong % numPartitions, 
			0L,
			numPartitions,
			1L
		);

		def genNewBlock(it: Iterator[(Int,Int)]): Iterator[(BlockID, BDV[Double])] = {
			it.map{v => (genNewID(v._1), f())};
		}

		val blocks: RDD[(BlockID,BDV[Double])] = sc
			.parallelize(0 to numPartitions-1, numPartitions)
			.map{x => (x,x)}
			.partitionBy(new HashPartitioner(numPartitions))
			.mapPartitions(genNewBlock);

		BlockVec(
			BlockSize(numPartitions,1L),
			BlockSize(bsize,1L),
			blocks);
	}

	def ones(sc: SparkContext, vecSize: Long, bsize: Long): BlockVec =
	{
		val f = () => BDV.ones[Double](bsize.toInt);
		BlockVec.generate(sc,vecSize,bsize,f);
	}

	def zeros(sc: SparkContext, vecSize: Long, bsize: Long): BlockVec =
	{
		val f = () => BDV.zeros[Double](bsize.toInt);
		BlockVec.generate(sc,vecSize,bsize,f);
	}

	// fill vector with single value
	def fill(sc: SparkContext, vecSize: Long, bsize: Long, a: Double): BlockVec =
	{
		val f = () => BDV[Double](Array.fill[Double](bsize.toInt)(a));
		BlockVec.generate(sc,vecSize,bsize,f);
	}

	// numerical operations
	def sin(v: BlockVec): BlockVec =
	{
		val f = (u: BDV[Double]) => breeze.numerics.sin(u);
		v.mapBlocks(f);
	}

	def cos(v: BlockVec): BlockVec =
	{
		val f = (u: BDV[Double]) => breeze.numerics.cos(u);
		v.mapBlocks(f);
	}

	def exp(v: BlockVec): BlockVec =
	{
		val f = (u: BDV[Double]) => breeze.numerics.exp(u);
		v.mapBlocks(f);
	}

	def normSquared(v: BlockVec): Double =
	{
		val f = (u: BDV[Double]) => u dot u;
		v.mapBlocksToScalar(f).sum;
	}
	
	def pow(v: BlockVec, n: Double): BlockVec = 
	{
		val f = (u: BDV[Double]) => breeze.numerics.pow(u,n);
		v.mapBlocks(f);
	}

	def sum(v: BlockVec): Double = 
	{
		val f = (v: BDV[Double]) => breeze.linalg.sum(v);

		val result: RDD[Double] = v
			.mapBlocksToScalar(f);
		result.sum;
	}

	// shorter form for v'*v
	/*def normSquared(v: BlockVec): Double = sum(pow(v,2));*/

	// default vector 2-norm
	def norm(v: BlockVec): Double = math.sqrt(BlockVec.normSquared(v));

	//vector p-norm
	def norm(v: BlockVec, p: Double): Double = {
		math.pow(sum(pow(v,p)), 1.0/p);
	}


}		
