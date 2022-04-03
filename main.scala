import scala.io.Source

// First I will sort both files using columin we are pertforming join on
object Join {
  //returns iterator of chunks, with 1000 rows each
  def getIterator(file: String): Iterator[Array[String]] = {
    val iterator = Source.fromFile(file).getLines.map(_.split(','))
  }
  def getHeader(iter: Iterator[Array[String]]): Array[String] = iter.next()
  def getChunks(iter: Iterator[Array[String]], size: Int = 1000): GroupedIterator[Array[String]] = 
    iter.grouped(size)
  
  def getColumnIndex(header: Array[String], column: String): Int = 
    header.indexOf(column)
  
  def sortChunk(chunk: List[Array[String]], column: Int): List[Array[String]] =
    chunk.sortBy(_(column))

}


object Main extends App {
  def main(args: Array[String]) = {

  }
}