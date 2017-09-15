# Overview

WordCount is the classic problem in Computer Science which can be solved efficiently using MapReduce in Hadoop. This project aims to experiment with three variations of the problem.

  - WordCount - The standard WordCount example.
  - WordCountDuo - A simple variation over the basic example. This involves finding the count of consecutive words in strings.
  - WordCountCache - This implementation makes use of the Distributed Cache feature in Apache Hadoop to define the list of words for which the word count needs to be extracted.


# Implementation Details

The wordcount programs were run on a Hadoop Cluster setup on AWS using [this tutorial](https://blog.insightdatascience.com/spinning-up-a-free-hadoop-cluster-step-by-step-c406d56bae42) for reference.

The primary folders in the repository are:
- [Input](https://github.com/vikramsk/wordcount/tree/master/input) - The input file used for running these tests. 10 copies of the file were used for the tests.
- [Output](https://github.com/vikramsk/wordcount/tree/master/output) - The output files generated for different runs.
- [Screenshots](https://github.com/vikramsk/wordcount/tree/master/screens) - The screenshots for the essential steps in the process.

