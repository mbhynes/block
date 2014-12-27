package himrod.block

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

class BlockMatSuite extends FunSuite with LocalSparkContext
{
	Logger.getLogger("org").setLevel(Level.WARN);
	Logger.getLogger("akka").setLevel(Level.WARN);

	// full mat size
	val N = 4;
	val M = 4;
	val matSize = BlockSize(N,M);

	// bsize
	val n = 2;
	val m = 2;
	val bsize = BlockSize(n,m);

	val fin: String = "src/test/scala/himrod/block/mat_data"
	val delim: String = ",";

	test("Instantiate BlockMat from file")
	{
		val test1 = BlockMat.fromTextFile(sc,fin,delim,matSize,bsize);
		test1.print();
		/*println(test1.partitions.length)*/

		val test2 = BlockMat.fromTextFile(sc,fin,delim,matSize,BlockSize(1,1));
		test2.print();
		/*println(test2.partitions.length)*/

		val test3 = BlockMat.fromTextFile(sc,fin,delim,matSize,BlockSize(4,1));
		test3.print();
		/*println(test3.partitions.length)*/

		val test4 = BlockMat.fromTextFile(sc,fin,delim,matSize,BlockSize(1,4));
		test4.print();
		/*println(test4.partitions.length)*/
	}

	test("Generate Random BlockMat")
	{
		//test rand generation
		val test1 = BlockMat.rand(sc,matSize,bsize);
		test1.print();
	}

	test("BlockMat Scalar Addition/Multiplication")
	{
		val test = BlockMat.rand(sc,matSize,bsize);
		test.print();
		val a: Double = 10;
		(test+a).print();
		(test*a).print();
	}

	test("BlockMat (Random) Addition/Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,fin,delim,matSize,BlockSize(4,4));
		val test2 = BlockMat.zeros(sc,matSize,BlockSize(4,4))+b;
		test1.print()
		test2.print()
		try
		{
			val test3 = test1 + test2;
			println("Success: ");
			test3.print()
		}
		catch
		{
			case ex: BlockMatSizeMismatchException => errorMessage(ex);
		}
	}

	test("BlockMat EYE")
	{
		BlockMat.eye(sc,matSize,bsize).print();
	}

	test("Matrix-Matrix Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,fin,delim,matSize,bsize);
		/*val test1 = BlockMat.zeros(sc,matSize,bsize)+a;*/
		val test2 = BlockMat.zeros(sc,matSize,bsize)+b;
		(test1 * test2).print
	}
	test("Matrix-Vector Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,fin,delim,matSize,bsize);
		/*val test1 = BlockMat.zeros(sc,matSize,bsize)+a;*/
		val test2 = BlockMat.zeros(sc,BlockSize(4,1),BlockSize(2,1))+b;
		test1.print;
		test2.print;
		(test1 * test2).print;
	}

	test("Matrix of Different BlockSize from File")
	{
		val bsize = BlockSize(3,2);
		val test1 = BlockMat.fromTextFile(sc,fin,delim,matSize,bsize);
		test1.print;
	}
	
	def errorMessage(ex: Exception) =
	{
		println(ex);
		println(ex.getMessage);
		println(ex.getCause);
		println(ex.printStackTrace);
	}
}
