package himrod.block

/*import breeze.linalg._*/
/*import breeze.linalg.{DenseMatrix => BDM}*/
/*import breeze.numerics*/

/*import org.apache.spark.rdd.RDD*/
/*import org.apache.spark.SparkContext*/
/*import org.apache.spark.SparkContext._*/
/*import org.apache.spark.SparkException*/
// ====================================================================
// define a block id as its (row,col) in a larger BlockMat:
// 0 <= row <= num_row_blocks
// 0 <= col <= num_col_blocks
// ====================================================================
case class BlockID(val row: Long, val col: Long) extends Serializable
{
	def product(other: BlockID): BlockID = BlockID(row,other.col);

	def transpose(): BlockID = BlockID(col,row);

	override def equals(other: Any): Boolean = 
		other match {
			case id: BlockID =>
				(row == id.row) && (col == id.col)
			case _ => false
		};
	
	override def toString() = "("+row+","+col+")";

}

object BlockID {

	def fromID(id: Long, matSize: BlockSize, bsize: BlockSize): BlockID = 
	{
		val row: Long = (id % matSize.nrows) / bsize.nrows;
		val col: Long = (id / matSize.ncols) / bsize.ncols;
		BlockID(row,col);
	}

	// don't use this yet
	/*def fromIndex(row: Long, col: Long, matSize: BlockSize, bsize: BlockSize): BlockID = */
	/*{*/
	/*	val row: Long = (id % matSize.nrows) / bsize.nrows;*/
	/*	val col: Long = matSize.ncols * (id / matSize.ncols) / bsize.ncols;*/
	/*	BlockID(row,col);*/
	/*}*/
}


