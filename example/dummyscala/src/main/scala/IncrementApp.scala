/* IncrementApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import java.io.File
import java.io.DataInputStream
import com.ericbarnhill.niftijio.NiftiVolume
import com.ericbarnhill.niftijio.NiftiHeader

object IncrementApp {
  val usage = """
    Usage: increment bb_dir output_dir iterations [--delay] [--work_dir]
  """

  def readImg( filename:String, data:PortableDataStream ) : Byte = {
    val niftibytes = data.open()
    val nifti = NiftiHeader.read(niftibytes, filename)
    niftibytes.close()
    return data.toArray()(2)
  }

  def main(args: Array[String]) {
    if (args.length == 0 || args.length < 3) { 
      println(usage)
      sys.exit(1)
    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextArgument(map: OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--work_dir" :: string :: tail =>
                                nextArgument(map ++ Map('work_dir -> string), tail)
        case "--delay" :: value :: tail =>
                                nextArgument(map ++ Map('delay -> value.toFloat), tail)
        case string :: tail if arglist.size - tail.size == 1  =>
                                nextArgument(map ++ Map('bb_dir -> string), tail)
        case string :: tail if arglist.size - tail.size == 2 =>
                                nextArgument(map ++ Map('output_dir -> string), tail)
        case string :: tail if arglist.size - tail.size == 3 =>
                                nextArgument(map ++ Map('iterations -> string.toInt), tail)
        case option :: tail => println("Unknown option "+option)
                               sys.exit(1)
      }
    }
    val options = nextArgument(Map(), arglist)
    
    val conf = new SparkConf().setAppName("Scala incrementation")
    val sc = new SparkContext(conf)

    val delay = if ((options get 'delay).isEmpty) 0 else (options get 'delay).get
    val of = new File(options('output_dir).asInstanceOf[String])
    val output_dir = of.getAbsolutePath()
    of.mkdirs()
    println(output_dir)

    val imRDD = sc.binaryFiles(options('bb_dir).asInstanceOf[String])
                  .map(x => readImg(x._1, x._2)).first() 
    println(imRDD)                              

    //val spark = SparkSession.builder.appName("Increment Application").getOrCreate()
    //val logData = spark.read.textFile(logFile).cache()
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println(s"Lines with a: $numAs, Lines with b: $numBs")
    //spark.stop()
  }
}
