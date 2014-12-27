package himrod.block

// ====================================================================
// define the dimensions (in rows/cols) of a block matrix
// ====================================================================
case class BlockSize(val nrows: Long, val ncols: Long) extends Serializable
{
	override def equals(other: Any): Boolean = 
		other match {
			case bs: BlockSize =>
				(nrows == bs.nrows) && (ncols == bs.ncols);
			case _ => false
		};

	override def toString() = "("+nrows+","+ncols+")";

	def transpose(): BlockSize = BlockSize(ncols,nrows);

	def innerDimEqual(other: BlockSize): Boolean = (ncols == other.nrows);

	def product(other: BlockSize): BlockSize = BlockSize(nrows,other.ncols);
}


