package himrod.block

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class BlockMatSuite extends FunSuite with LocalSparkContext
{
	test("Instantiate BlockMat from file")
	{
		// full mat size
		val N = 4;
		val M = 4;
		val matSize: (Long,Long) = (N,M);

		// bsize
		val n = 2;
		val m = 2;
		val bsize: (Long,Long) = (n,m);

		val fin: String = "src/test/scala/himrod/block/data"
		val delim: String = ",";
		val test1 = BlockMat.fromTextFile(sc,fin,delim,matSize,bsize);
		test1.collect.foreach(println);
		/*println(test1.partitions.length)*/

		val test2 = BlockMat.fromTextFile(sc,fin,delim,matSize,(1,1));
		test2.collect.foreach(println);
		/*println(test2.partitions.length)*/

		val test3 = BlockMat.fromTextFile(sc,fin,delim,matSize,(4,1));
		test2.collect.foreach(println);
		/*println(test3.partitions.length)*/

		val test4 = BlockMat.fromTextFile(sc,fin,delim,matSize,(1,4));
		test4.collect.foreach(println);
		/*println(test4.partitions.length)*/
	}
	
}
