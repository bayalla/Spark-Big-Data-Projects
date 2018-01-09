import org.apache.spark.{SparkConf, SparkContext}

object Analysis {

  val conf = new SparkConf().setAppName("Market Analysis").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    // Reading and cleaning the data
    val dataRaw = sc.textFile("data/source_data.txt")
    val header = dataRaw.first()
    val data = dataRaw.filter(rec => rec != header)
    data.cache()

    // Marketing Success Rate
    val numOfPeopleSubscribed = data.
                                filter(rec => rec.split(";")(16).equals("yes")).
                                count()

    val totalNumOfPeople = data.count()

    val successRate = BigDecimal(((numOfPeopleSubscribed.toFloat)/(totalNumOfPeople.toFloat))*100).
                                  setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    // 11.7 %
    println(successRate)
  }
}
