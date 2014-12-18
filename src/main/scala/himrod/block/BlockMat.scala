package himrod.block

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException

//============================================================
// Define an RDD formed from an array of Blocks: (BlockID, BreezeDenseMatrix[Double])
// Each block is lexicographically ordered by BlockID = (row,col)
// where:
//	0 <= row <= num_block_rows
//	0 <= col <= num_block_cols
// 
//============================================================
class BlockMat(
	val nrows: Long, // number of blocks
	val ncols: Long, // number of cols, each of hi
	val bsize: BlockSize, //size of uniform blocks
	val blocks: RDD[Block]
	) extends Serializable 
{
}
object BlockMat {

	type Size = (Long,Long)
	type Index = (Long,Long)

	// get the lexicographic index of (i,j) in (N,M) matrix 
	private def getIndex(size: Size, ij: Index): Long = {
		ij._1 + (ij._2 * size._1);  
	}

	// return the lexicographic id of (i,j) entry in size=(N,M) matrix
	private def getAbsBlockID(matSize: Size, bsize: Size, ij: Index): Long = 
	{
		// get (i,j) index of topleft element in block
		val i_block: Long = bsize._1 * (ij._1 / bsize._1);
		val j_block: Long = bsize._2 * (ij._2 / bsize._2);
		/*println("(i,j)_topleft:" + (i_block,j_block));*/
		getIndex(matSize,(i_block,j_block));
	}
	
	// return indices (i,j) relative to topleft element in their block
	private def getRelBlockID(matSize: Size, bsize: Size, ij: Index): Long =
	{
		val i_block: Long = (ij._1 % bsize._1);
		val j_block: Long = (ij._2 % bsize._2);
		/*println("(i,j)_rel:" + (i_block,j_block));*/
		/*getIndex(matSize,(i_block,j_block));*/
		getIndex(bsize,(i_block,j_block));
	}

	def fromTextFile(
		sc: SparkContext, 
		fin: String, 
		delim: String,
		matSize: Size,
		bsize: Size
		) = 
	{
		def getBlockID(id: Long): BlockID = 
		{
			val row: Long = (id % matSize._1) / bsize._1;
			val col: Long = matSize._2 * (id / matSize._2) / bsize._2;
			new BlockID(row,col);
		}
		def toBlock(x: (Long, Iterable[(Long,Double)]) ) = 
		{
			val id: BlockID = getBlockID(x._1);
			val A = Array.ofDim[Double]((bsize._1*bsize._2).toInt);
			for (tuple <- x._2)
				A(tuple._1.toInt) = tuple._2

			val mat: BDM[Double] = new BDM(bsize._1.toInt,bsize._2.toInt,A);
			new Block(id,mat);
		}


		val nblocksRow: Long = matSize._1 / bsize._1;
		val nblocksCol: Long = matSize._2 / bsize._2;
		val numPartitions: Int = (nblocksRow * nblocksCol).toInt;

		// calculate the number of partitions
		/*val numPartitions = bsize._1 * bsize._2;*/
		/*val partFn = new Partitioner  */

		// convert matrix A_ij textfile to tuple (absBlockID, relBlockID, A_ij)
		val blocks = sc.textFile(fin, numPartitions)
			.map { line => 
				val tokens = line.split(delim); 
				val ij: Index = (tokens(0).toLong, tokens(1).toLong);
				/*println(tokens(0) + "," + tokens(1) + "->" + ij );*/
				val id_abs: Long = getAbsBlockID(matSize,bsize,ij);
				/*println("abs:" + id_abs);*/
				val id_rel: Long = getRelBlockID(matSize,bsize,ij);
				/*println("rel:" + id_rel);*/
				/*println(tokens(0) + "," + tokens(1) + "->" + ij );*/
				(id_abs, (id_rel,tokens(2).toDouble) );
			}
			.groupByKey()
			.map(toBlock)
			// reduce to (relBlockID, A_ij), such that each absBlockID has a partition

		// sort each partition {(id_rel,A_ij)}
		/*val arrayRDD = localMatRDD*/
		/*	.mapPartitions( iter => {*/
		/*			iter.toArray.sortWith((x,y)=>x._1.compare(y._1) < 0)*/
		/*			/*.map(v => v._2)*/*/
		/*			.iterator */
		/*		},*/
		/*		preservesPartitioning = true)*/
			/*.mapPartitions( iter => {*/
			/*	println(iter);*/
			/*	iter._2*/
			/*	},*/
			/*	preservesPartitioning = true)*/
			/*)*/

			/*arrayRDD*/
		/*localMatRDD*/
		new BlockMat(nblocksRow,nblocksCol,new BlockSize(bsize._1,bsize._2),blocks);
	}

	/*def fromArray(val matRDD: RDD[Array[Double]], val bsize: BlockSize)*/
	/*{*/
	/*}*/
}
