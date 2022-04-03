import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}
// First I will sort both files using columin we are pertforming join on
object Join {
  //returns iterator of chunks, with 1000 rows each
  def getIterator(file: String): Iterator[Array[String]] = 
    Source.fromFile(file).getLines().map(_.split(','))
  
  def getHeader(iter: Iterator[Array[String]]): Array[String] = iter.next()
  def getChunks(iter: Iterator[Array[String]], size: Int = 1000) = 
    iter.grouped(size)
  
  def getColumnIndex(header: Array[String], column: String): Int = 
    header.indexOf(column)
  
  def sortChunk(chunk: List[Array[String]], column: Int): List[Array[String]] =
    chunk.sortBy(_(column))

  def writeToTempFile(data: List[String], prefix: String): File = {
    val file = File.createTempFile(s"$prefix", ".tmp")
    file.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- data) {
      bw.write(line)
    }
    bw.close()
    file
  }

  def sortAndWrite(file: String, column: String): Iterator[File] = {
    val iter = getIterator(file)
    val header = getHeader(iter)
    val chunks = getChunks(iter)
    val index: Int = getColumnIndex(header, column)
    var n: Int = 0
    val tempFiles = for {
      chunk <- chunks
      sorted: List[Array[String]] = sortChunk(chunk.toList, index)
      lines: List[String] = sorted.map(_.mkString(",") + '\n')
    } yield  writeToTempFile(lines, file + n.toString)
    tempFiles
  }
}


object Main {
  def main(args: Array[String]) = {

  }
}