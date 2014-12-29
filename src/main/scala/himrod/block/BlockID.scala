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
case class BlockID(
	row: Long, 
	col: Long, 
	nrows: Long,  //number of rows (blocks)
	ncols: Long)  //number of cols (blocks)
	extends Serializable
{
	def product(other: BlockID): BlockID = BlockID(row,other.col,nrows,other.ncols);

	def transpose(): BlockID = BlockID(col,row,ncols,nrows);

	override def equals(other: Any): Boolean = 
		other match {
			case id: BlockID =>
				(row == id.row) && (col == id.col) && 
					(nrows == id.nrows) && (ncols == id.ncols);
			case _ => false
		};
	
	override def hashCode(): Int = {
		(col * nrows + row).toInt;
	}

	override def toString() = "("+row+","+col+")";

}

object BlockID {
	def fromID(id: Long, matSize: BlockSize, bsize: BlockSize): BlockID = 
	{
		val row: Long = (id % matSize.nrows) / bsize.nrows;
		val col: Long = (id / matSize.ncols) / bsize.ncols;
		val nrows: Long = (1.0 * matSize.nrows / bsize.nrows).ceil.toLong;
		val ncols: Long = (1.0 * matSize.ncols / bsize.ncols).ceil.toLong;
		BlockID(row,col,nrows,ncols);
	}
}


