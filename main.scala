import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}
import scala.collection.mutable.PriorityQueue
// First I will sort both files using columin we are pertforming join on



object Join {

  def join(file1: String, file2: String, column: String, mode: Int): Unit = {
    val (header1, columnIndex1, sorted1) = Sort.createSortedFile(file1, column)
    val (header2, columnIndex2, sorted2) = Sort.createSortedFile(file2, column)
    println(header1.mkString(",")+","+header2.mkString(","))
    mode match {
      case a if mode < 2 => 
        joinMaker(columnIndex1,columnIndex2, header1.length, header2.length, mode)(Source.fromFile(sorted1).getLines().map(_.split(',')), sorted2, 0)
      case b => 
        joinMaker(columnIndex2,columnIndex1, header1.length, header2.length, mode)(Source.fromFile(sorted2).getLines().map(_.split(',')), sorted1, 0)
    }
  }
  def joinMaker(columnIndex1: Int, columnIndex2: Int, hl1: Int, hl2: Int, mode: Int)(iter1: Iterator[Array[String]], file2: File, n: Int):Unit = {
    def fun(iter1: Iterator[Array[String]], file2: File, n: Int): Unit = {
      if (iter1.hasNext) {
        var matched = false      
        val line1 = iter1.next()
        val iter2 = Source.fromFile(file2).getLines().map(_.split(',')).drop(n)
        if(iter2.hasNext) {
          var line2 = iter2.next()
          while(line1(columnIndex1).equals(line2(columnIndex2)) && iter2.hasNext) {
            if(mode < 2) println((line1++line2).mkString(","))
            else println((line2++line1).mkString(","))
            line2=iter2.next()
            matched = true
          }
          if(line1(columnIndex1) < line2(columnIndex2)) {
            if(matched==false)  {
              mode match {
                case 0 => ()
                case 1 => println(line1.mkString(",") + ","*hl2)
                case 2 => println(","*hl1 + line1.mkString(","))
              }
            }
            fun(iter1, file2, n)
          }
          else {
            fun(Iterator[Array[String]](line1) ++ iter1, file2, n+1)
          }
        }
        else {
          mode match {
            case 0 => ()
            case 1 => for(line <- iter1) println(line1.mkString(",") + ","*hl2)
            case 2 => for(line <- iter1) println(","*hl1 + line1.mkString(","))
          }
        }
      }
      else {}
    }
    fun(iter1, file2, n)
  }
}






object Sort {
  //returns iterator of chunks, with 1000 rows each
  def toRow(arr: Array[String]): String = arr.mkString(",") + '\n'
  def toArr(row: String): Array[String] = row.split(',')
  def getIterator(file: String, size: Int = 1000000): (Array[String], Iterator[Seq[Array[String]]]) = {
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

  def createSortedFile(file: String, column: String): (Array[String], Int , File) = {
    val (header, chunks) = getIterator(file)
    val columnIndex: Int = getColumnIndex(header, column)
    val tempFiles: List[File] = sortAndWrite(chunks, columnIndex, file)
    if(tempFiles.length == 1) (header, columnIndex, tempFiles.head)
    else {
      val sorted = kMerge(mergeIterators(tempFiles), columnIndex)
      
      val sortedFile = File.createTempFile(s"sorted$file", ".tmp")
      sortedFile.deleteOnExit()
      val bw = new BufferedWriter(new FileWriter(sortedFile))
      for (line <- sorted.map(toRow(_))) {
        bw.write(line)
      }
      bw.close()
      tempFiles.map(_.delete())
      (header, columnIndex, sortedFile)
    }
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
    Join.join("file1.csv", "file2.csv", "age", 0)
  }
}