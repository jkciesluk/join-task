import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}
import scala.collection.mutable.PriorityQueue
// First I will sort both files using columin we are pertforming join on



object Sort {
  //returns iterator of chunks, with 1000 rows each
  def getIterator(file: String): Iterator[Array[String]] = 
    Source.fromFile(file).getLines().map(_.split(','))
  
  def getHeader(iter: Iterator[Array[String]]): Array[String] = iter.next()
  def getChunks(iter: Iterator[Array[String]], size: Int = 1000): Iterator[Seq[Array[String]]] = 
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

  def sortAndWrite(chunks: Iterator[Seq[Array[String]]], columnIndex: Int, file: String): Iterator[File] = {
    val tempFiles = for {
      (chunk, n) <- chunks.zipWithIndex
      sorted: List[Array[String]] = sortChunk(chunk.toList, columnIndex)
      lines: List[String] = sorted.map(_.mkString(",") + '\n')
      
    } yield  writeToTempFile(lines, file + n.toString)
    tempFiles
  }

  def mergeIterators(files: Iterator[File]): List[Iterator[Array[String]]] = 
    files.map(Source.fromFile(_).getLines().map(_.split(','))).filter(_.nonEmpty).toList
  
  def kMerge(iterators: List[Iterator[Array[String]]], columnIndex: Int): Iterator[Array[String]] = {
    val ordering: Ordering[(Array[String], Iterator[Array[String]])] = Ordering.by(_._1(columnIndex))
    KMerge.merge(KMerge.createQueue(iterators, ordering))
  }

  def createSortedFile(file: String, column: String): File = {
    val iter = getIterator(file)
    val header = getHeader(iter)
    val chunks = getChunks(iter)
    val columnIndex: Int = getColumnIndex(header, column)
    val tempFiles: Iterator[File] = sortAndWrite(chunks, columnIndex, file)
    val sorted = kMerge(mergeIterators(tempFiles), columnIndex)
    
    val sortedFile = File.createTempFile(s"sorted$file", ".tmp")
    sortedFile.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(sortedFile))
    bw.write(header.mkString(",") + '\n')
    for (line <- sorted.map(_.mkString(",") + '\n')) {
      bw.write(line)
    }
    bw.close()
    sortedFile
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
  def merge(queue: PriorityQueue[(Array[String], Iterator[Array[String]])]): Iterator[Array[String]] = {
    new Iterator[Array[String]] {
      val q = queue
      def hasNext = queue.nonEmpty
      def next() = {
        val (item, rest) = q.dequeue()
        if(rest.nonEmpty) q.addOne(rest.next(), rest)
        item
      }
    }
  }
}


object Main {
  def main(args: Array[String]) = {

  }
}