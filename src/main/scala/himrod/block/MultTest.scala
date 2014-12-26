package himrod.block

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


package object MultTest 
{
	def mem_string(s: String) : Long = 
	{
		// strip last letter
		val n: Int = s.length
		val num: String = s.substring(0,n-1)
		val c: String = s.substring(n-1,n)
		val p: Long = num.toLong;
		c match 
		{
			case ("g"|"G") => p * 1000 * 1000 * 1000
			case ("m"|"M") => p * 1000 * 1000
			case ("k"|"K") => p * 1000
			case _ => p
		}
	}

	def main(args: Array[String]) = 
	{
		val conf = new SparkConf()
		val sc = new SparkContext(conf)

		// parse commandline arguments
		type ArgMap = Map[Symbol,Long]

		def next_arg(map: ArgMap, list: List[String]) : ArgMap = 
		{
			list match 
			{
				case Nil => map
				case ("--rows" | "-r") :: value :: tail =>
					next_arg(map ++ Map('rows -> value.toLong), tail)
				case ("--cols" | "-c") :: value :: tail =>
					next_arg(map ++ Map('cols -> value.toLong), tail)
				case ("--dim" | "-d") :: value :: tail =>
					next_arg(map ++ Map('dim -> value.toLong), tail)
				case _ :: tail =>
					next_arg(map, tail)
			}
		}
		val arg_list = args.toList;
		val vars: ArgMap = next_arg(Map(),arg_list);
		val fout: String = arg_list.last;

		// parse elements + set defaults
		val dim: Long = {
			if (vars.contains('dim))
				vars('dim);
			else
				1000;
		}

		//block size
		val rows: Long = {
			if (vars.contains('rows))
				vars('rows);
			else
				10;
		}
		val cols: Long = {
			if (vars.contains('cols))
				vars('cols);
			else
				10;
		}

		val msize: BlockSize = BlockSize(dim,dim);
		val bsize: BlockSize = BlockSize(rows,cols);

		val a: Double = 1;
		val b: Double = 1;
		val mat1: BlockMat = BlockMat.rand(sc,msize,bsize) * a;
		val mat2: BlockMat = BlockMat.rand(sc,msize,bsize) * b;

		val mat3: BlockMat = mat1 * mat2;
		mat3.saveAsTextFile(fout);
	}
}
