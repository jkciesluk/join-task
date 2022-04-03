import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}
import scala.collection.mutable.PriorityQueue
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

  def mergeIterators(files: Iterator[File]): List[Iterator[Array[String]]] = 
    files.map(Source.fromFile(_).getLines().map(_.split(','))).filter(_.nonEmpty).toList
  
  def kMerge(iterators: List[Iterator[Array[String]]], columnIndex: Int): List[Array[String]] = {
    val ordering: Ordering[(Array[String], Iterator[Array[String]])] = Ordering.by(_._1(columnIndex))
    KMerge.merge(KMerge.createQueue(iterators, ordering))
  }
}

object KMerge {
  def createQueue(iterators: List[Iterator[Array[String]]], 
                  ordering: Ordering[(Array[String], Iterator[Array[String]])]
                  ): PriorityQueue[(Array[String], Iterator[Array[String]])] = {

    val heap: PriorityQueue[(Array[String], Iterator[Array[String]])] = PriorityQueue()(ordering)
    val nonEmpty = iterators.filter(_.nonEmpty)
    for {
      iterator <- iterators
      head = iterator.next()
    } heap.addOne((head, iterator))
    heap
  }
  def merge(queue: PriorityQueue[(Array[String], Iterator[Array[String]])]): List[Array[String]] = {
    if(queue.nonEmpty) {
      val (item, rest) = queue.dequeue()
      if(rest.nonEmpty) {
        item +: merge(queue.addOne(rest.next(), rest)) 
      }
      else {
        item +: merge(queue)
      }
    }
    else {
      List.empty
    }
  }
}


object Main {
  def main(args: Array[String]) = {

  }
}