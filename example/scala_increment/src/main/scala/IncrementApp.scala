/* IncrementApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import java.io.File
import java.io.DataInputStream
import com.ericbarnhill.niftijio.NiftiVolume
import com.ericbarnhill.niftijio.NiftiHeader
import java.lang.System
import scala.math.pow

object IncrementApp {
  val usage = """
    Usage: increment app_name bb_dir output_dir iterations [--delay] [--work_dir]
  """

  def readImg( filename:String, data:PortableDataStream )
    : Tuple2[String, NiftiVolume] = {
    val t0 = System.nanoTime()
    val niftibytes = data.open()
    println(filename)
    val volume = NiftiVolume.read(niftibytes, filename)
    niftibytes.close()
    val t1 = System.nanoTime()

    println("Elapsed load time: " + (t1 - t0).toDouble / pow(10, 9) + "s")
    return (new File(filename).getName(), volume)
  }

  def incrementData( filename:String, volume: NiftiVolume, sleep: Int )
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
      Thread.sleep((sleep - inc_duration).toLong * 1000)
    }

    val t2 = System.nanoTime()
    println("Elapsed inc time: " + (t2 - t0).toDouble / pow(10, 9) + "s")
    return (filename, volume )
  }

  def saveData( fn: String, volume: NiftiVolume )
    : Tuple2[String, String] = {

    val t0 = System.nanoTime()
    volume.write(fn)
    val t1 = System.nanoTime()
    println("Elapsed write time: " + (t1 - t0).toDouble / pow(10, 9) + "s")

    return (fn, "SUCCESS")
  }

  def main(args: Array[String]) {
    if (args.length == 0 || args.length < 4) { 
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
                                nextArgument(map ++ Map('delay -> value.toInt), tail)
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
    val its = options('iterations).asInstanceOf[Int]
    of.mkdirs()
    println(output_dir)

    var imRDD = sc.binaryFiles(options('bb_dir).asInstanceOf[String])
                  .map(x => readImg(x._1, x._2))

    for ( i <- 1 to its ) {
      imRDD = imRDD.map(x => incrementData(x._1, x._2, delay.asInstanceOf[Int]))
    }

    val result = imRDD.map( x => saveData(new File(output_dir, x._1).getAbsolutePath(), x._2) ).collect()

    result.foreach(x => println(x._1 + ": " + x._2))                              

    sc.stop()
  }
}
