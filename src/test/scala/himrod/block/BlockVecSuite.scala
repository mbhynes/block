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

	val matSize: Long = 4;
	val bsize: Long = 2;

	val fin: String = "src/test/scala/himrod/block/vec_data"
	val delim: String = ",";

	test("Instantiate BlockVec from file")
	{
		BlockVec.fromTextFile(sc,fin,delim,matSize,bsize).print();
		BlockVec.fromTextFile(sc,fin,delim,matSize,matSize).print();
		BlockVec.fromTextFile(sc,fin,delim,matSize,1).print();
	}

	test("Generate Random BlockVec")
	{
		//test rand generation
		val test1 = BlockVec.rand(sc,matSize,bsize);
		test1.print();
	}

	test("BlockVec Scalar Addition/Multiplication")
	{
		val test = BlockVec.rand(sc,matSize,bsize);
		test.print();
		val a: Double = 10;
		(test+a).print();
		(test*a).print();
	}

	test("BlockVec (Random) Addition/Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockVec.fromTextFile(sc,fin,delim,matSize,bsize);
		val test2 = BlockVec.zeros(sc,matSize,bsize)+b;
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
		val test1 = BlockVec.fromTextFile(sc,fin,delim,matSize,bsize);
		/*val test1 = BlockVec.zeros(sc,matSize,bsize)+a;*/
		val test2 = BlockVec.zeros(sc,matSize,bsize)+b;
		(test1 * test2).print
	}
	test("Vector Dot product")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockVec.fromTextFile(sc,fin,delim,matSize,bsize);
		/*val test1 = BlockVec.zeros(sc,matSize,bsize)+a;*/
		val test2 = BlockVec.zeros(sc,matSize,bsize)+b;
		println("dot product = " + test1.dot(test2));
	}

	test("Vector of Different BlockSize from File")
	{
		val bsize = BlockSize(3,2);
		val test1 = BlockVec.fromTextFile(sc,fin,delim,matSize,bsize);
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
