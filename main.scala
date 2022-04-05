import scala.io.Source
import java.io.{File, BufferedWriter, FileWriter}
import scala.collection.mutable.PriorityQueue
// First I will sort both files using columin we are pertforming join on



object Join {
  /** Creates iterator from file
    * @param file - name of file
    * @return Iterator of lines as arrays of strings
  */
  def getIterator(file: String): Iterator[Array[String]] = Source.fromFile(file).getLines().map(_.split(','))
  
  /** Index of column to join on
    * @param header - header from file
    * @param column - name of column to join on
    * @return Index of column in header or -1 if not found
  */
  def getColumnIndex(header: Array[String], column: String): Int = 
    header.indexOf(column)
  
  /** Joins two files on column `column` and prints result file to stdout
    * @param file1 - name of the first file
    * @param file2 - name of the second file
    * @param column - name of column to join on
    * @param mode - type of join: 0 - inner, 1 - left, 2 - right
  */  
  def join(file1: String, file2: String, column: String, mode: Int): Unit = {
    val f1 = new File(file1)
    val f2 = new File(file2)
    if(f1.exists && f2.exists) {
      val iter1 = getIterator(file1)
      val iter2 = getIterator(file2)
      val header1 = iter1.next()
      val header2 = iter2.next()
      val columnIndex1 = getColumnIndex(header1, column)
      val columnIndex2 = getColumnIndex(header2, column)
      if(columnIndex1 < 0 || columnIndex2 < 0) println("Invalid column name")
      else {
        val sorted1 = Sort.createSortedFile(iter1, file1, columnIndex1)
        val sorted2 = Sort.createSortedFile(iter2, file2, columnIndex2)
        println(header1.map(file1+"."+_).mkString(",")+","+header2.map(file2+"."+_).mkString(","))
        mode match {
          case a if mode < 2 => 
            joinMaker(columnIndex1,columnIndex2, header1.length, header2.length, mode)(Source.fromFile(sorted1).getLines().map(_.split(',')), sorted2, 0)
          case b => 
            joinMaker(columnIndex2,columnIndex1, header1.length, header2.length, mode)(Source.fromFile(sorted2).getLines().map(_.split(',')), sorted1, 0)
        }
      }
    }
    else { println("File does not exist")}
  }

  /** Creates join function and runs it.
    * Called by join function.
  */
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
  /** Sorts single chunk of data by column
    * @param chunk - chunk of data to sort
    * @param column - index of column to sort on
    * @return Sorted chunk
  */
  def sortChunk(chunk: List[Array[String]], column: Int): List[Array[String]] =
    chunk.sortBy(_(column))
  
  /** Writes data to temporary file
    * @param data - ready to write data
    * @param prefix - prefix of filename
    * @return File with saved data
  */
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
  
  /** Sorts chunks of data by `columnIndex` and writes them to temporary files
    * @param chunks - iterator of chunks with data to sort
    * @param columnIndex - index of column to sort on
    * @param file - prefix for temporary files names
    * @return List of temporary files with sorted data
  */
  def sortAndWrite(chunks: Iterator[Seq[Array[String]]], columnIndex: Int, file: String): List[File] = {
    val tempFiles = for {
      (chunk, n) <- chunks.zipWithIndex
      sorted: List[Array[String]] = sortChunk(chunk.toList, columnIndex)
      lines: List[String] = sorted.map(_.mkString(",") + "\n")
      
    } yield  writeToTempFile(lines, file + n.toString)
    tempFiles.toList
  }

  /** Gets an iterator from each file in `files`
    * @param files - list of files in csv format
    * @return List of iterators from each file, with lines as arrays of strings
  */
  def mergeIterators(files: List[File]): List[Iterator[Array[String]]] = 
    files.map(Source.fromFile(_).getLines().map(_.split(','))).filter(_.nonEmpty)
  
  /** Performs K - Merge sort on iterators, creating single sorted iterator
    * @param iterators - list of sorted iterators
    * @param columnIndex - index of column to sort on
    * @return Single sorted iterator with data from `iterators`
  */
  def kMerge(iterators: List[Iterator[Array[String]]], columnIndex: Int): Iterator[Array[String]] = {
    val ordering: Ordering[(Array[String], Iterator[Array[String]])] = 
      Ordering.by((pair:(Array[String], Iterator[Array[String]])) => pair._1(columnIndex)).reverse
    KMerge.merge(KMerge.createQueue(iterators, ordering))
  }

  /** Sorts data `iter` from `file` on `columnIndex`, with configurable size of chunk, default=1000000.
   * First, divides data into chunks, then sorts them separately, and saves them to temporary files.
   * Using K - Merge algorithm merges sorted files into single sorted file.
    * @param iter - data to be sorted
    * @param file - name of file
    * @param columnIndex - index of column to sort on
    * @param size - size of chunk, default=1000000
    * @return Sorted temporary file
  */
  def createSortedFile(iter: Iterator[Array[String]], file: String, columnIndex: Int, size: Int = 1000000): File = {
    val chunks = iter.grouped(1000000)
    val tempFiles: List[File] = sortAndWrite(chunks, columnIndex, file)
    if(tempFiles.length == 1) (tempFiles.head)
    else {
      val sorted = kMerge(mergeIterators(tempFiles), columnIndex)
      
      val sortedFile = File.createTempFile(s"sorted$file", ".tmp")
      sortedFile.deleteOnExit()
      val bw = new BufferedWriter(new FileWriter(sortedFile))
      for (line <- sorted.map(_.mkString(",") + "\n")) {
        bw.write(line)
      }
      bw.close()
      tempFiles.map(_.delete())
      sortedFile
    }
  }
}

object KMerge {
  /** Creates priority queue for k-merge
    * @param iterators - list of iterators to merge
    * @param ordering - ordering to use for queue
    * @return PriorityQueue
  */
  def createQueue(iterators: List[Iterator[Array[String]]], 
                  ordering: Ordering[(Array[String], Iterator[Array[String]])]
                  ): PriorityQueue[(Array[String], Iterator[Array[String]])] = {

    val heap: PriorityQueue[(Array[String], Iterator[Array[String]])] = PriorityQueue()(ordering)
    for {
      iterator <- iterators
      head = iterator.next()
    } heap.enqueue((head, iterator))
    heap
  }
  /** Merges iterators using priority queue `queue` into single iterator
    * @param queue - priority queue with iterators to merge and their first elements
    * @return Single sorted iterator
  */
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
    if(args.length < 3) println("Not enough arguments")
    else if (args.length == 3) 
      Join.join(args(0), args(1), args(2), 0)
    else {
      val mode = args(3).toLowerCase()
      mode match {
        case "inner" => Join.join(args(0), args(1), args(2), 0)
        case "left"  => Join.join(args(0), args(1), args(2), 1)
        case "right" => Join.join(args(0), args(1), args(2), 2)
        case _ => println("Invalid join mode")
      }
    }
  }
}