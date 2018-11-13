import java.io._
import util.control.Breaks._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Mahima_Gupta_SON {

  def sorting_task[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

  def apriori_algo(basket: Iterator[Set[String]], threshold:Int): Iterator[Set[String]] =
  {
    var chunks = basket.toList
    val one = chunks.flatten.groupBy(identity).mapValues(_.size).filter(a => a._2 >= threshold).map(a => a._1).toSet
    var size = 2
    var frequency = one
    var hashmap = Set.empty[Set[String]]

    for (i <- one)
    {
      hashmap = hashmap + Set(i)
    }

    var result = Set.empty[String]
    var size_hashmap = hashmap.size

    while (frequency.size >= size)
    {
      var candidate_set = frequency.subsets(size)
      for (m <- candidate_set)
      {
        breakable
        {
          var b = 0
          for (i <-chunks)
          {
            if (m.subsetOf(i))
            {
              b = b + 1
              if (b >= threshold)
              {
                result = result ++ m
                hashmap = hashmap + m
                break
              }
            }
          }
        }
      }

      if (hashmap.size >size_hashmap)
      {
        frequency = result
        result = Set.empty[String]
        size += 1
        size_hashmap = hashmap.size
      } else
      {
        size = 1 + frequency.size
      }

    }
    hashmap.iterator
  }

  def main(args: Array[String]) : Unit =
  {

    val timeofstart = System.currentTimeMillis()

    val sconf = new SparkConf().setAppName("Mahima_Gupta_hw3").setMaster("local[2]")
    var sctxt = new SparkContext(sconf)
    var input = sctxt.textFile(args(0))
    //var input = sctxt.textFile("/Users/mahima/IdeaProjects/DMAssignment3/src/main/scala/yelp_reviews_test.txt")
    var file_header = input.first()
    input = input.filter(row => row != file_header)
    var input_data = input.map(row => row.split(","))
    var num_partition = input.getNumPartitions
    val support = args(1).toInt
    //val support = 30

    var num_baskets: RDD[Set[String]] = null
    num_baskets = input_data.map(x => (x(0), x(1))).groupByKey().map(_._2.toSet)

    var threshold = 1
    if (support/num_partition >threshold)
    {
      threshold = support/num_partition
    }
    var map1 = num_baskets.mapPartitions(chunk =>
    {
      apriori_algo(chunk, threshold)
    }).map(a=>(a,1)).reduceByKey((c1,c2)=>1).map(_._1).collect()

    val printed = sctxt.broadcast(map1)

    var map2 = num_baskets.mapPartitions(chunk =>
    {
      var list_chunks = chunk.toList
      var out = List[(Set[String],Int)]()
      for (i<- list_chunks)
      {
        for (j<- printed.value)
        {
          if (j.forall(i.contains))
          {
            out = Tuple2(j,1) :: out
          }
        }
      }
      out.iterator
    })
    var end = map2.reduceByKey(_+_).filter(_._2 >= support).map(_._1).map(a => (a.size,a)).collect()
    val maximum = end.maxBy(_._1)._1
    val output_file = new PrintWriter(new File(args(2)))
    //val output_file = new PrintWriter(new File("/Users/mahima/IdeaProjects/DMAssignment3/src/main/scala/Mahima_Gupta_SON_yelp_reviews_test_30.txt"))
    var sorted = sorting_task(end.filter(_._1 == 1).map(_._2))
    for (s<- sorted)
    {
      if (s==sorted.last){
        output_file.write(s.mkString("(",", ",")\n\n"))
      }
      else
      {
        output_file.write(s.mkString("(",", ","), "))
      }
    }
    var print = ""
    for (i <- 2.to(maximum)){
      var st = sorting_task(end.filter(_._1 == i).map(_._2))

      for (p<- st)
      {
        print = print + "("
        var s = p.toList.sorted
        for (q<- s){
          if (q==s.last){
            print = print + "" + q +"), "
          }
          else
          {
            print = print + "" + q +", "
          }
        }
      }
      print = print.dropRight(2) + "\n\n"
    }
    output_file.write(print)
    output_file.close()

    val timeofend = System.currentTimeMillis()
    println("Total Time taken: " + (timeofend - timeofstart)/1000 + " secs")
  }
}
