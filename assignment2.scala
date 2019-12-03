// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath = "./sample_input.txt"
val outputDirPath = "./myOutput"
// load data from file
val file = sc.textFile(inputFilePath,1)
// data size conversion table
val convertMap = scala.collection.Map("KB" -> 1024.0, "MB" -> 1048576.0, "GB" -> 1073741824.0)
// convert string includ data size to long integer in bytes
val convertSize = (x:String) => {
    val suffix = x.substring(x.length-2);
    if(!convertMap.contains(suffix)){
        java.lang.Long.valueOf(x.substring(0, x.length-1)).doubleValue();
    }else{
        java.lang.Long.valueOf(x.substring(0, x.length-2)) * convertMap(suffix);
    }
}
// Add 'B' to the end of a number
val addB = (x:Long) => x.toString+"B"
// calculate mean according sum and count
val mean = (s:Double, n:Int) => addB((s/n).toLong)
// calculate variance
val variance = (a:Seq[Double], s:Double) => {
    val mean = s/a.length;
    addB((a.map(x=>math.pow(x-mean,2)).reduce(_+_)/(a.length)).toLong)
}
// convert log record to key-value pair (Base URL, Data size)
val convertRecord = (x:String) => {
    val s = x.split(",");
    (s(0),convertSize(s(3)));
}
// filter out empty line from file records and convert each line to key-value pair
val lines = file.filter(x=>(!x.isEmpty)).map(convertRecord)
// group by base URL, sort the array containing payload size and calculate the sum of payload
val hosts = lines.groupByKey().map(x=>(x._1,x._2.toSeq.sorted,x._2.sum))
// get the first and last element of sorted array as smallest and largest payload size, calculate mean and variance via function defined before
val result = hosts.map(x=>x._1+","+addB(x._2.head.toLong)+","+addB(x._2.last.toLong)+","+mean(x._3,x._2.length)+","+variance(x._2,x._3))
// save result into specified folder
result.saveAsTextFile(outputDirPath)