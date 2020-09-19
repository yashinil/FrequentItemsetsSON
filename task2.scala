import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable._

object task2 {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder()
      .appName("second")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

    val filter_threshold=args(0).toInt
    val threshold=args(1).toInt
    val input_path=args(2)
    val output_path=args(3)

    type pairRDD=(String,List[String])
    def edit_data(in:Array[String]): pairRDD ={
      var temp=""
      temp=temp.concat(in(0).slice(1,in(0).length-1))
      temp=temp.concat("-")
      temp=temp.concat(in(1).slice(1,in(1).length-1))
      var temp1=""
      temp1=temp1.concat(in(5).slice(1,in(5).length-1).replaceFirst("^0+(?!$)", ""))
      return (temp,List(temp1))
    }
    val largeRDD = sc.textFile(input_path).map(line=>line.split(',')).filter(line=> line(0)!="TRANSACTION_DT").map(edit_data)
    val basketsRDD = largeRDD.reduceByKey((a,b)=>a++b).filter(line=>line._2.size>filter_threshold).persist(StorageLevel.DISK_ONLY)
    for(x<-basketsRDD.take(10)){
      println(x)
    }
    type pair=(String,Int)
    val total_baskets=basketsRDD.count()

    //defining apriori function
    def apriori(iterator: Iterator[pairRDD]): Iterator[pair] = {
      var baskets=new ListBuffer[List[String]]()
      var baskets_in_partition=0
      var itemset_size=1
      var candidate:List[pair]=List()
      var temp_candidate=new ListBuffer[pair]()
      var frequent:Set[String]=Set()
      var frequent_itemset:List[String]=List()
      //calculating number of baskets in the partition
      //creating a list of baskets from the iterator
      //adding all possible singletons in frequent set
      for (elem <- iterator) {
        var temp=new ListBuffer[String]()
        baskets_in_partition+=1
        for(item<-elem._2) {
          temp+=item
          frequent+=item
        }
        var temp_basket=temp.toList
        baskets+=temp_basket
      }
      var list_of_baskets=baskets.toList
      //creating initial frequent itemsets of size1
      var temp_itemset=new ListBuffer[String]()
      for(item<-frequent){
        temp_itemset+=item
      }
      frequent_itemset=temp_itemset.toList
      //calculating reduced threshold for the partition
      var reduced_threshold=((baskets_in_partition*1.0/total_baskets)*threshold*1.0)
      //start a while loop for getting all the candidates
      while(frequent_itemset.size!=0){
        //println(frequent_itemset)
        //counting the frequency of each itemset in the list of baskets
        val frequency=Map[String,Int]()
        //for each basket in list of baskets
        for(basket<-list_of_baskets){
          val temp_basket=basket.toSet
          //for each tuple in frequent itemsets
          for(tpl<-frequent_itemset){
            //check if all the elements of the tuple are present in the basket
            var flag=1
            var temp_tpl=tpl.split(",")
            for(item<-temp_tpl){
              if(!temp_basket.contains(item)){
                flag=0
              }
            }
            //if all are present, increase the frequency of that tuple by 1
            if(flag==1){
              if(frequency.contains(tpl)){
                frequency(tpl)+=1
              }
              else {
                frequency(tpl)=1
              }
            }
          }
        }
        //clear the frequent itemset to put actually frequent itemsets
        temp_itemset.clear()
        frequent_itemset=temp_itemset.toList
        //check if frequency is greater than reduced threshold
        for((k,v)<-frequency){
          if(v>=reduced_threshold){
            temp_candidate+=((k,1))
            temp_itemset+=k
          }
        }
        //add actual frequents to the frequent itemsets
        frequent_itemset=temp_itemset.toList
        //println("actual frequent itemsets are:"+frequent_itemset)
        temp_itemset.clear()
        //flatten the tuple into a list of items
        for(tpl<-frequent_itemset){
          var temp_tpl=tpl.split(",")
          for(item<-temp_tpl){
            temp_itemset+=item
          }
        }
        //get the list of frequent items from the buffer after flattening
        frequent_itemset=temp_itemset.toList
        temp_itemset.clear()
        itemset_size+=1
        //convert the list to set and get all possible subsets
        var all_subsets=frequent_itemset.toSet[String].subsets.map(_.toList).toList
        //get only subsets of size= itemset_size
        for(subset<-all_subsets){
          if(subset.size==itemset_size){
            var temp_subset=subset.sorted
            var temp=""
            for(i<-0 to (temp_subset.size-2)){
              temp = temp.concat(temp_subset(i))
              temp = temp.concat(",")
            }
            temp = temp.concat(temp_subset(temp_subset.size-1))
            temp_itemset+=temp
          }
        }
        frequent_itemset=temp_itemset.toList
        //println(frequency)
      }
      candidate=temp_candidate.toList
      //println(candidate)

      return candidate.toIterator
    }

    val candidates=basketsRDD.mapPartitions(apriori).reduceByKey((a,b)=> 1).map(line=> line._1).collect()
    var sorted_candidates=candidates.sorted
    //write the output to the file in proper order you will have to process according to the size
    //for(x<-sorted_candidates){
    //  println(x)
    //}
    //println("*************************************************")
    //defining function for finding the actual frequent pairs
    def apriori_1(iterator: Iterator[pairRDD]): Iterator[pair] = {
      var baskets=new ListBuffer[List[String]]()
      //creating a list of baskets from the iterator
      for (elem <- iterator) {
        var temp=new ListBuffer[String]()
        for(item<-elem._2) {
          temp+=item
        }
        var temp_basket=temp.toList
        baskets+=temp_basket
      }
      var list_of_baskets=baskets.toList
      //counting the frequency of each itemset in the list of baskets
      val frequency=Map[String,Int]()
      //for each basket in list of baskets
      for(basket<-list_of_baskets){
        var temp_basket=basket.toSet
        //for each tuple in candidates
        for(tpl<-candidates){
          //check if all the elements of the tuple are present in the basket
          var flag=1
          var temp_tpl=tpl.split(",")
          for(item<-temp_tpl){
            if(!temp_basket.contains(item)){
              flag=0
            }
          }
          //if all are present, increase the frequency of that tuple by 1
          if(flag==1){
            if(frequency.contains(tpl)){
              frequency(tpl)+=1
            }
            else {
              frequency(tpl)=1
            }
          }
        }
      }
      var frequent_itemset:List[pair]=List()
      var temp_frequent_itemset=new ListBuffer[pair]()
      //check if frequency is greater than reduced threshold
      for((k,v)<-frequency){
        if(v>=threshold){
          temp_frequent_itemset+=((k,v))
        }
      }
      frequent_itemset=temp_frequent_itemset.toList
      //println(frequent_itemset)
      //println("*************************************************")
      return frequent_itemset.toIterator
    }

    var frequent_itemsets=basketsRDD.mapPartitions(apriori_1).reduceByKey((a,b)=>a+b).filter(line=> line._2>=threshold).map(line=>line._1).collect()
    var sorted_frequent_itemsets=frequent_itemsets.sorted
    // writing output to a file
    val out_file = new File(output_path)
    val print_Writer = new PrintWriter(out_file)
    print_Writer.write("Candidates:\n")
    var size=1
    var flag=1
    while(flag!=0){
      flag=0
      for(x<-sorted_candidates){
        var temp=x.split(",")
        if(temp.size==size){
          if(flag==0 && size>=2){
            print_Writer.write("\n\n(")
          }
          else if(flag==0){
            print_Writer.write("(")
          }
          else{
            print_Writer.write(",(")
          }
          flag=1
          for(i<-0 to (temp.size-2)){
            print_Writer.write("'"+temp(i)+"'"+", ")
          }
          print_Writer.write("'"+temp(temp.size-1)+"'"+")")
        }
      }
      size+=1
    }

    print_Writer.write("\n\nFrequent Itemsets:\n")
    size=1
    flag=1
    while(flag!=0){
      flag=0
      for(x<-sorted_frequent_itemsets){
        var temp=x.split(",")
        if(temp.size==size){
          if(flag==0 && size>=2){
            print_Writer.write("\n\n(")
          }
          else if(flag==0){
            print_Writer.write("(")
          }
          else{
            print_Writer.write(",(")
          }
          flag=1
          for(i<-0 to (temp.size-2)){
            print_Writer.write("'"+temp(i)+"'"+", ")
          }
          print_Writer.write("'"+temp(temp.size-1)+"'"+")")
        }
      }
      size+=1
    }
    print_Writer.close()
    val duration = (System.nanoTime - t1) / 1e9d
    print("Duration: ")
    print(duration)
  }
}