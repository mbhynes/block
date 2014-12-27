package himrod.block

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

class BlockVecSuite extends FunSuite with LocalSparkContext
{
	Logger.getLogger("org").setLevel(Level.WARN);
	Logger.getLogger("akka").setLevel(Level.WARN);

	val vecSize: Long = 4;
	val vec_bsize: Long = 2;

	val vec_fin: String = "src/test/scala/himrod/block/vec_data"

	test("Instantiate BlockVec from file")
	{
		BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize).print();
		BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vecSize).print();
		BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,1).print();
	}

	test("Generate Random BlockVec")
	{
		//test rand generation
		val test1 = BlockVec.rand(sc,vecSize,vec_bsize);
		test1.print();
	}

	test("BlockVec Scalar Addition/Multiplication")
	{
		val test = BlockVec.rand(sc,vecSize,vec_bsize);
		test.print();
		val a: Double = 10;
		(test+a).print();
		(test*a).print();
	}

	test("BlockVec (Random) Addition/Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize);
		val test2 = BlockVec.zeros(sc,vecSize,vec_bsize)+b;
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
			case ex: BlockVecSizeMismatchException => errorMessage(ex);
		}
	}

	test("Vector-Vector Element-Wise Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize);
		/*val test1 = BlockVec.zeros(sc,vecSize,vec_bsize)+a;*/
		val test2 = BlockVec.zeros(sc,vecSize,vec_bsize)+b;
		(test1 * test2).print
	}
	test("Vector Dot product")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize);
		/*val test1 = BlockVec.zeros(sc,vecSize,vec_bsize)+a;*/
		val test2 = BlockVec.zeros(sc,vecSize,vec_bsize)+b;
		println("dot product = " + test1.dot(test2));
	}

	test("Vector of Different BlockSize from File")
	{
		val vec_bsize = BlockSize(3,2);
		val test1 = BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize);
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
