/* IncrementApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream
import java.io.DataInputStream
import java.lang.management.ManagementFactory
import com.ericbarnhill.niftijio.NiftiVolume
import com.ericbarnhill.niftijio.NiftiHeader
import java.lang.System
import scala.math.pow

object IncrementApp {
  val usage = """
    Usage: increment app_name bb_dir output_dir iterations [--delay]
           [--log_dir]
  """

  def benchmark( task:String, t0:Long, t1:Long, benchmark_dir:String ) : Boolean = {

    if ( !benchmark_dir.isEmpty ){
      val pid = ManagementFactory.getRuntimeMXBean().getName()

      val bench_fn = pid.toString + "-bench.txt"

      val bench_file = new File(benchmark_dir, bench_fn)

      var pw: PrintWriter = null
      if ( bench_file.exists() && !bench_file.isDirectory() ) {
        pw = new PrintWriter(new FileOutputStream(bench_file, true))
      }
      else {
        pw = new PrintWriter(bench_file)
      }
      pw.append( task + "," + t0 + "," + t1 + "\n")
      pw.close

      return true
    }
    return false
  }

  def readImg( filename:String, data:PortableDataStream, benchmark_dir:String )
    : Tuple2[String, NiftiVolume] = {
    val t0 = System.nanoTime()
    val niftibytes = data.open()
    println(filename)
    val volume = NiftiVolume.read(niftibytes, filename)
    niftibytes.close()
    val t1 = System.nanoTime()

    println("Elapsed load time: " + (t1 - t0).toDouble / pow(10, 9) + "s")


    benchmark( "read", t0, t1, benchmark_dir )

    return (new File(filename).getName(), volume)
  }

  def incrementData( filename:String, volume: NiftiVolume, sleep: Int,
    benchmark_dir:String )
    : Tuple2[String, NiftiVolume] = {

    val t0 = System.nanoTime()
    for( i <- 0 to volume.data.sizeX - 1) {
      for( j <- 0 to volume.data.sizeY - 1) {
        for( k <- 0 to volume.data.sizeZ - 1) {
          for( l <- 0 to volume.data.dimension - 1 ) {
            volume.data.set(i, j, k, l, volume.data.get(i, j, k, l) + 1.0)
          }
        }
      }
    }
    val t1 = System.nanoTime()
    if ( sleep > 0 ){
      val inc_duration = (t1 - t0).toDouble / pow(10, 9)
      println("Incrementation duration: " + inc_duration + "s" )
      Thread.sleep(sleep * 1000)
    }

    val t2 = System.nanoTime()
    println("Elapsed inc time: " + (t2 - t0).toDouble / pow(10, 9) + "s")

    benchmark( "increment", t0, t1, benchmark_dir )

    return (filename, volume )
  }

  def saveData( fn: String, volume: NiftiVolume, benchmark_dir: String )
    : Tuple2[String, String] = {

    val t0 = System.nanoTime()
    volume.write(fn)
    val t1 = System.nanoTime()
    println("Elapsed write time: " + (t1 - t0).toDouble / pow(10, 9) + "s")

    benchmark( "write", t0, t1, benchmark_dir )

    return (fn, "SUCCESS")
  }

  def main(args: Array[String]) {
    if (args.length == 0 || args.length < 5) { 
      println(usage)
      sys.exit(1)
    }

    val t0 = System.nanoTime()

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextArgument(map: OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--delay" :: value :: tail =>
                                nextArgument(map ++ Map('delay -> value.toInt), tail)
        case "--log_dir" :: string :: tail =>
                                nextArgument(map ++ Map('log_dir -> string), tail)
        case string :: tail if arglist.size - tail.size == 1  =>
                                nextArgument(map ++ Map('app_name -> string), tail)
        case string :: tail if arglist.size - tail.size == 2  =>
                                nextArgument(map ++ Map('bb_dir -> string), tail)
        case string :: tail if arglist.size - tail.size == 3 =>
                                nextArgument(map ++ Map('output_dir -> string), tail)
        case string :: tail if arglist.size - tail.size == 4 =>
                                nextArgument(map ++ Map('iterations -> string.toInt), tail)
        case option :: tail => println("Unknown option "+option)
                               sys.exit(1)
      }
    }
    val options = nextArgument(Map(), arglist)
    
    val conf = new SparkConf().setAppName(options('app_name).asInstanceOf[String])
    val sc = new SparkContext(conf)

    val delay = if ((options get 'delay).isEmpty) 0 else (options get 'delay).get
    val of = new File(options('output_dir).asInstanceOf[String])
    val output_dir = of.getAbsolutePath()
    val log_dir = if ((options get 'log_dir).isEmpty) "" else (options get 'log_dir).get.asInstanceOf[String]
    val ld = new File(log_dir).getAbsolutePath()
    val its = options('iterations).asInstanceOf[Int]
    of.mkdirs()
    println(output_dir)

    var imRDD = sc.binaryFiles(options('bb_dir).asInstanceOf[String])
                  .map(x => readImg(x._1, x._2, ld))

    for ( i <- 1 to its ) {
      imRDD = imRDD.map(x => incrementData(x._1, x._2, delay.asInstanceOf[Int],
                                           ld))
    }

    val result = imRDD.map( x => saveData(new File(output_dir, x._1).getAbsolutePath(), x._2, ld) ).collect()

    result.foreach(x => println(x._1 + ": " + x._2))                              
    val t1 = System.nanoTime()
    benchmark("driver", t0, t1, ld )

    sc.stop()
  }
}
