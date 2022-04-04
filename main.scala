import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}
import scala.collection.mutable.PriorityQueue
// First I will sort both files using columin we are pertforming join on

object Join {

  def join(iter1: Iterator[Array[String]], file2: File, columnIndex1: Int, columnIndex2: Int, n: Int):Unit = {
    if (iter1.hasNext) {
      var size = 0
      val line1 = iter1.next()
      val iter2 = Source.fromFile(file2).getLines().map(_.split(',')).drop(n)
      if(iter2.hasNext) {
        var line2 = iter2.next()
        while(line1(columnIndex1).equals(line2(columnIndex2)) && iter2.hasNext) {
          println((line1++line2).mkString(","))
          line2=iter2.next()
          size += 1
        }
        if(line1(columnIndex1) < line2(columnIndex2)) {
          join(iter1, file2, columnIndex1, columnIndex2, n)
        }
        else {
          join(Iterator[Array[String]](line1) ++ iter1, file2, columnIndex1, columnIndex2, n+1)
        }
      }
      else {

      }
    }
  }
}






object Sort {
  //returns iterator of chunks, with 1000 rows each
  def toRow(arr: Array[String]): String = arr.mkString(",") + '\n'
  def toArr(row: String): Array[String] = row.split(',')
  def getIterator(file: String, size: Int = 2): (Array[String], Iterator[Seq[Array[String]]]) = {
    val iter = Source.fromFile(file).getLines().map(toArr(_))
    (iter.next(), iter.grouped(size))
  }
  
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

  def sortAndWrite(chunks: Iterator[Seq[Array[String]]], columnIndex: Int, file: String): List[File] = {
    val tempFiles = for {
      (chunk, n) <- chunks.zipWithIndex
      sorted: List[Array[String]] = sortChunk(chunk.toList, columnIndex)
      lines: List[String] = sorted.map(toRow(_))
      
    } yield  writeToTempFile(lines, file + n.toString)
    tempFiles.toList
  }

  def mergeIterators(files: List[File]): List[Iterator[Array[String]]] = 
    files.map(Source.fromFile(_).getLines().map(toArr(_))).filter(_.nonEmpty)
  
  def kMerge(iterators: List[Iterator[Array[String]]], columnIndex: Int): Iterator[Array[String]] = {
    val ordering: Ordering[(Array[String], Iterator[Array[String]])] = 
      Ordering.by((pair:(Array[String], Iterator[Array[String]])) => pair._1(columnIndex)).reverse
    KMerge.merge(KMerge.createQueue(iterators, ordering))
  }

  def createSortedFile(file: String, column: String): File = {
    val (header, chunks) = getIterator(file)
    val columnIndex: Int = getColumnIndex(header, column)
    val tempFiles: List[File] = sortAndWrite(chunks, columnIndex, file)
    val sorted = kMerge(mergeIterators(tempFiles), columnIndex)
    
    val sortedFile = File.createTempFile(s"sorted$file", ".tmp")
    sortedFile.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(sortedFile))
    for (line <- sorted.map(toRow(_))) {
      bw.write(line)
    }
    bw.close()
    tempFiles.map(_.delete())
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
    } heap.enqueue((head, iterator))
    heap
  }
  def merge(queue: PriorityQueue[(Array[String], Iterator[Array[String]])]): Iterator[Array[String]] = {
    new Iterator[Array[String]] {
      val q = queue
      def hasNext = queue.nonEmpty
      def next() = {
        val (item, rest) = q.dequeue()
        if(rest.hasNext) q.enqueue((rest.next(), rest))
        item
      }
    }
  }
}


object Main {
  def main(args: Array[String]) = {
    // val f1 = "file1.csv"
    // val f2 = "file2.csv"
    // val column = "age"
    // val mode = "left"
    // val sorted1 = Sort.createSortedFile(f1, column)
    // val sorted2 = Sort.createSortedFile(f2, column)
    // Join.join(Source.fromFile(sorted1).getLines().map(_.split(',')), sorted2, 0, 1, 0)
  }
}