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

	// vector sizes
	val vecSize: Long = N;
	val vec_bsize: Long = n;

	// files to read
	val vec_fin: String = "src/test/scala/himrod/block/vec_data"
	val mat_fin: String = "src/test/scala/himrod/block/mat_data"
	val delim: String = ",";

	/*test("Instantiate BlockVec from file")*/
	/*{*/
	/*	println("Instantiate BlockVec from file...")*/
	/*	BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize).print();*/
	/*	println("Instantiate BlockVec from file...")*/
	/*	BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vecSize).print();*/
	/*	println("Instantiate BlockVec from file...")*/
	/*	BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,1).print();*/
	/*}*/

	/*test("Generate Random BlockVec")*/
	/*{*/
	/*	//test rand generation*/
	/*	println("Generate Random BlockVec")*/
	/*	val test1 = BlockVec.rand(sc,vecSize,vec_bsize);*/
	/*	test1.print();*/
	/*}*/

	test("BlockVec Scalar Addition/Multiplication")
	{
		val test = BlockVec.rand(sc,vecSize,vec_bsize);
		val a: Double = 10;
		println("Vec scalar addition");
		(test+a).print();
		println("Vec scalar mult");
		(test*10.0*a).print();
	}

	test("Vector-Vector Element-Wise Addition")
	{
		val a: Double = 2;
		val b: Double = 10;
		/*val test1 = BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize);*/
		/*val test1 = BlockVec.zeros(sc,vecSize,vec_bsize)+a;*/
		val test1 = BlockVec.fill(sc,vecSize,vec_bsize,b);
		val test2 = BlockVec.ones(sc,vecSize,vec_bsize);
		val test3 = (test1 + test2);
		println("Vector-Vector Element-Wise Addition")
		test1.print();
		test2.print();
		test3.print();
	}

	test("Vector-Vector Element-Wise Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		/*val test1 = BlockVec.fromTextFile(sc,vec_fin,delim,vecSize,vec_bsize);*/
		val test1 = BlockVec.zeros(sc,vecSize,vec_bsize)+a;
		val test2 = BlockVec.ones(sc,vecSize,vec_bsize);
		val test3 = (test1 * test2);
		println("Vector-Vector Element-Wise Multiplication")
		test1.print();
		test2.print();
		test3.print();
	}

	test("Vector Dot product")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockVec.ones(sc,vecSize,vec_bsize);
		/*val test1 = BlockVec.zeros(sc,vecSize,vec_bsize)+a;*/
		val test2 = BlockVec.ones(sc,vecSize,vec_bsize);
		/*test1.print()*/
		/*test2.print()*/
		println("dot product = " + test1.dot(test2));
	}

	test("Matrix-Vector Multiplication BlockMat")
	{
		//test rand generation
		val mat = BlockMat.fill(sc,matSize,bsize,1);
		val vec = BlockVec.fill(sc,vecSize,vec_bsize,1);
		val result = mat.multiply(vec);

		println("M-V Multiplication");
		mat.print();
		vec.print();
		result.print();
		/*assert(result === BlockVec.fill(sc,vecSize,vec_bsize,16.0))*/
	}

	test("Vector-Matrix Multiplication BlockMat")
	{
		//test rand generation
		val mat = BlockMat.ones(sc,BlockSize(8,8),BlockSize(2,4));
		val vec = BlockVec.ones(sc,8L,4L); val result = vec.multiply(mat.transpose());

		println("V-M Multiplication");
		mat.print();
		vec.print();
		result.print();
	}

	test("Matrix Inner Product")
	{
		val mat = BlockMat.ones(sc,matSize,bsize);
		val vec = BlockVec.ones(sc,vecSize,vec_bsize);
		println("Matrix Inner Product: " + mat.matProduct(vec));
	}

	test("Vector p-norm")
	{
		val vec = BlockVec.ones(sc,vecSize,vec_bsize);
		println("1-norm: " + BlockVec.norm(vec,1));
		println("2-norm: " + BlockVec.norm(vec));
		println("3-norm: " + BlockVec.norm(vec,3));
	}

	
	/*test("Instantiate BlockMat from file")*/
	/*{*/
	/*	val test1 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,bsize);*/
	/*	test1.print();*/

	/*	val test2 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,BlockSize(1,1));*/
	/*	test2.print();*/

	/*	val test3 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,BlockSize(4,1));*/
	/*	test3.print();*/

	/*	val test4 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,BlockSize(1,4));*/
	/*	test4.print();*/
	/*}*/

	test("BlockMat Scalar Addition/Multiplication")
	{
		val test = BlockMat.rand(sc,matSize,bsize);
		test.print();
		val a: Double = 10;
		println("BlockMat Scalar Addition/Multiplication");
		(test+a).print();
		(test*a).print();
	}

	test("BlockMat Element-Wise Addition of different Sizes")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,bsize);
		val test2 = BlockMat.zeros(sc,matSize,bsize)+b;
		println("BlockMat Addition of different numBlocks")
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

	test("BlockMat Element-Wise Multiplication of different Sizes")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,bsize);
		val test2 = BlockMat.zeros(sc,matSize,bsize)+b;
		println("BlockMat Multiplication of different numBlocks")
		test1.print()
		test2.print()
		try
		{
			val test3 = test1.multiply(test2);
			println("Success: ");
			test3.print()
		}
		catch
		{
			case ex: BlockMatSizeMismatchException => errorMessage(ex);
		}
	}

	/*test("BlockMat EYE")*/
	/*{*/
	/*	BlockMat.eye(sc,matSize,bsize).print();*/
	/*}*/

	test("Matrix-Matrix Scalar Division")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,bsize);
		/*val test1 = BlockMat.zeros(sc,matSize,bsize)+a;*/
		val test2 = BlockMat.zeros(sc,matSize,bsize)+b;
		println("Matrix-Matrix Scalar Division")
		(test1 / test2).print
	}
	test("Matrix-Vector Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.fromTextFile(sc,mat_fin,delim,BlockSize(4L,4L),BlockSize(2L,2L));
		val test2 = BlockVec.ones(sc,4L,2L);
		val test3 = test1.multiply(test2);
		println("Matrix-Vector Multiplication")
		test1.print;
		test2.print;
		test3.print;

		println("Vector-Vector Addition of previous result:");
		(test3 + BlockVec.ones(sc,4L,2L)*2).print;
	}

	test("Matrix-Matrix Multiplication")
	{
		val a: Double = 2;
		val b: Double = 10;
		val test1 = BlockMat.ones(sc,matSize,bsize);
		val test2 = BlockMat.ones(sc,matSize,bsize);
		val test3 = test1.multiply(test2);
		println("Matrix-Matrix Multiplication")
		test3.print;
	}

	test("Eye Matrix")
	{
		val I = BlockMat.eye(sc,matSize,bsize);
		println("Square Identity Matrix");
		I.print

		/*println("Identity Matrix plus rand");*/
		/*(I + BlockMat.rand(sc,matSize,bsize)).print;*/

		val I2 = BlockMat.eye(sc,matSize,BlockSize(1L,N));
		println("Row-Block Identity Matrix");
		I2.print

		println("Row-Block Identity Matrix");
		val I3 = BlockMat.eye(sc,BlockSize(6L,4L),BlockSize(3L,2L));
		println("6x4 in 3x2 blocks Identity Matrix");
		I3.print

	}
	/*test("Matrix of Different BlockSize from File")*/
	/*{*/
	/*	val bsize = BlockSize(3,2);*/
	/*	val test1 = BlockMat.fromTextFile(sc,mat_fin,delim,matSize,bsize);*/
	/*	test1.print;*/
	/*}*/
	
	def errorMessage(ex: Exception) =
	{
		println(ex);
		println(ex.getMessage);
		println(ex.getCause);
		println(ex.printStackTrace);
	}
}
