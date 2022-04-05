# Join on two big files
Comment to solution:


***1. Why sort the files first?***

   If we simply iterate both files, complexity of solution would be O(n*m), where n and m are sizes of both files (number of rows). Complexity of sorting: O(nlogn + mlogm), after that we can join files faster. If each row in file1 "matches" each row in file2, we still get O(n * m), but usually, we are closer to O(n+m).
   So total complexity of solution is O(nlogn + mlogm)

***2. How to sort the files?***

   Algorithm I used for sorting (external merge sort) first divides data into chunks of some size (configurable in code, depending on available memory), then sorts each of them seperately (can be done in parallel) and saves results to k (k = n/chunk size) temporary files. After that, we create priority queue and merge these k sorted files. Complexity of sorting chunks is O(k * size*logsize) = O(n*logsize).
   Merging uses priority queue, with deleting minimal element in O(1) and adding new in O(logk).
   So total complexity is O(n logsize + n log k) = O(n logn). Size of files isn't a problem, because we store only small chunks and list of iterators.

***3. Order of rows in the result***

   Rows appear in sorted order, not the original one because of sorting. If we want to keep original order, 
   we could sort only the second file (first one, if right join) and use one unsorted file for join. We save time on sorting, and rewriting first file, but we have to iterate the second (sorted) file from the beginning till we find matching "slice", we can't drop first rows.

***4. Problems with this solution.***

   Biggest issue is bottleneck on writing and reading from hard drive. We are writing our data to hard drive twice, first time with chunks, second time with sorted file.

***5. Packaging:***

   `scala-cli package main.scala -o join --assembly`