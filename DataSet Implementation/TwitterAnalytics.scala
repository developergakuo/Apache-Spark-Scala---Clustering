import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.math
import scala.collection.immutable.Range
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}


object TwitterAnalytics {
  val sparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    //å.master(("local[2]"))
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import sparkSession.implicits._


  type Tag = String
  type Likes = Double

  case class Tweet(
                    text: String,
                    hashTags: Array[Tag],
                    likes: Likes,
                    followers_count: Double,
                    friends_count: Double,
                    favorites_count: Double,
                    statuses_count: Double,
                    listed_count: Double)

  case class Tuple(
                    text: Double,
                    tags: Double,
                    likes: Double,
                    followers: Double,
                    friends: Double,
                    favorites: Double,
                    statuses: Double,
                    listed: Double)


  case class ExtendedTuple(error: Double,
                           errCoeff: Double,
                           text: Double,
                           tags: Double,
                           followers: Double,
                           friends: Double,
                           favorites: Double,
                           statuses: Double,
                           listed: Double)

  case class Theta(errCoeff: Double,
                          txtCoef: Double,
                          tagCoeff: Double,
                          follCoef: Double,
                          frndCoeff: Double,
                          favCoeff: Double,
                          statusCoef: Double,
                          listedCoef: Double)


  implicit class JValueExtended(value: JValue) { //extract from stackoverflow
    def has(childString: String): Boolean = {
      if ((value \ childString) != JNothing) {
        true
      } else {
        false
      }
    }
  }

  def parseTweet(tweet: String): Tweet = {
    implicit val formats = DefaultFormats
    val parsedTweetJSON = Try(parse(tweet)) //try to parse tweet json
    parsedTweetJSON match {
      case Success(value) => // for succesfully parsed tweet json, do the following

        var tweet_text: String = "Text"
        var hashtag: Array[Tag] = Array("no_hashtag")
        var likes: Likes = 0
        var followers_count: Double = 0
        var friends_count: Double = 0
        var favorites_count: Double = 0
        var statuses_count: Double = 0
        var listed_count: Double = 0
        //get only interesting tweets with likes
        if (value.has("retweeted_status")) {
          val retweeted_favorite_count0 = value \ "retweeted_status" \ "favorite_count"
          val retweeted_favorite_count1 = Try(retweeted_favorite_count0.extract[Likes])
          retweeted_favorite_count1 match {
            case Success(result) => likes = result
            case Failure(exception) => likes = -1
            case _ => likes = -1
          }

          val text = value \ "retweeted_status" \ "text"
          val tweettext = Try(text.extract[String])
          tweettext match {
            case Success(text) => tweet_text = text
            case Failure(exception) => tweet_text = "blank Tweet"
          }


          val hashtags_J = value \ "retweeted_status" \ "entities" \ "hashtags" \ "text"
          hashtags_J match {
            case JArray(jarray) => var hash_tag: Array[Tag] = new Array[Tag](jarray.values.length)
              jarray.values.map(x => hash_tag ++ Array(x.toString))
              hashtag = hash_tag
            case JString(jstring) => hashtag = Array[Tag](jstring.toString)
            case JNothing => hashtag = Array[Tag]("Blank")
            case _ => hashtag = Array[Tag]("Blank")

          }
          val followers_count0 = value \ "retweeted_status" \ "user" \ "followers_count"
          val followers_count1 = Try(followers_count0.extract[Likes])
          followers_count1 match {
            case Success(result) => followers_count = result
            case Failure(exception) => followers_count = -1
            case _ => followers_count = -1
          }
          val friends_count0 = value \ "retweeted_status" \ "user" \ "friends_count"
          val friends_count1 = Try(friends_count0.extract[Likes])
          friends_count1 match {
            case Success(result) => friends_count = result
            case Failure(exception) => friends_count = -1
            case _ => friends_count = -1
          }

          val favorites_count0 = value \ "retweeted_status" \ "user" \ "favourites_count"
          val favorites_count1 = Try(favorites_count0.extract[Likes])
          favorites_count1 match {
            case Success(result) => favorites_count = result
            case Failure(exception) => favorites_count = -1
            case _ => favorites_count = -1
          }

          val statuses_count0 = value \ "retweeted_status" \ "user" \ "statuses_count"
          val statuses_count1 = Try(statuses_count0.extract[Likes])
          statuses_count1 match {
            case Success(result) => statuses_count = result
            case Failure(exception) => statuses_count = -1
            case _ => statuses_count = -1
          }
          val listed_count0 = value \ "retweeted_status" \ "user" \ "listed_count"
          val listed_count1 = Try(listed_count0.extract[Likes])
          listed_count1 match {
            case Success(result) => listed_count = result
            case Failure(exception) => listed_count = -1
            case _ => listed_count = -1
          }


        } else if (value.has("quoted_status")) {
          val retweeted_favorite_count0 = value \ "quoted_status" \ "favorite_count"
          val retweeted_favorite_count1 = Try(retweeted_favorite_count0.extract[Likes])
          retweeted_favorite_count1 match {
            case Success(result) => likes = result
            case Failure(exception) => likes = -1
            case _ => likes = -1
          }

          val text = value \ "quoted_status" \ "text"
          val tweettext = Try(text.extract[String])
          tweettext match {
            case Success(text) => tweet_text = text
            case Failure(exception) => tweet_text = "blank Tweet"
          }


          val hashtags_J = value \ "quoted_status" \ "entities" \ "hashtags" \ "text"
          hashtags_J match {
            case JArray(jarray) => var hash_tag: Array[Tag] = new Array[Tag](jarray.values.length)
              jarray.values.map(x => hash_tag ++ Array(x.toString))
              hashtag = hash_tag
            case JString(jstring) => hashtag = Array[Tag](jstring.toString)
            case JNothing => hashtag = Array[Tag]("Blank")
            case _ => hashtag = Array[Tag]("Blank")

          }
          val followers_count0 = value \ "quoted_status" \ "user" \ "followers_count"
          val followers_count1 = Try(followers_count0.extract[Likes])
          followers_count1 match {
            case Success(result) => followers_count = result
            case Failure(exception) => followers_count = -1
            case _ => followers_count = -1
          }
          val friends_count0 = value \ "quoted_status" \ "user" \ "friends_count"
          val friends_count1 = Try(friends_count0.extract[Likes])
          friends_count1 match {
            case Success(result) => friends_count = result
            case Failure(exception) => friends_count = -1
            case _ => friends_count = -1
          }

          val favorites_count0 = value \ "quoted_status" \ "user" \ "favourites_count"
          val favorites_count1 = Try(favorites_count0.extract[Likes])
          favorites_count1 match {
            case Success(result) => favorites_count = result
            case Failure(exception) => favorites_count = -1
            case _ => favorites_count = -1
          }

          val statuses_count0 = value \ "quoted_status" \ "user" \ "statuses_count"
          val statuses_count1 = Try(statuses_count0.extract[Likes])
          statuses_count1 match {
            case Success(result) => statuses_count = result
            case Failure(exception) => statuses_count - 1
            case _ => statuses_count = -1
          }
          val listed_count0 = value \ "quoted_status" \ "user" \ "listed_count"
          val listed_count1 = Try(listed_count0.extract[Likes])
          listed_count1 match {
            case Success(result) => listed_count = result
            case Failure(exception) => listed_count = -1
            case _ => listed_count = -1
          }

        }

        Tweet(tweet_text, hashtag, likes, followers_count, friends_count, favorites_count, statuses_count, listed_count)

      case Failure(exception) => Tweet("null", Array("no_hashtag"), 0, 0, 0, 0, 0, 0)

    }

  }


  def calculateFeatureMean(dataset: Dataset[Tuple],sampleSize: Long): Tuple = {
    var accum: (Int, Int, Likes, Double, Double, Double, Double, Double) = (0, 0, 0, 0, 0, 0, 0, 0)
    val sumtuple = dataset.reduce((accum, elem) => Tuple(accum.text + elem.text, accum.tags + elem.tags, accum.likes + elem.likes, accum.followers + elem.followers, accum.friends + elem.friends, accum.favorites + elem.favorites, accum.statuses + elem.statuses, accum.listed + elem.listed))

    Tuple(sumtuple.text / sampleSize.toFloat, sumtuple.tags / sampleSize.toFloat, sumtuple.likes / sampleSize, sumtuple.followers / sampleSize, sumtuple.friends / sampleSize, sumtuple.favorites / sampleSize, sumtuple.statuses / sampleSize, sumtuple.listed / sampleSize)
  }

  def calculateFeatureSTD(dataset: Dataset[Tuple], means: Tuple, sampleSize: Long): Tuple = {
    val denom = sampleSize - 1
    val intermediateDataset = dataset.map(elem => Tuple(math.pow((means.text - elem.text), 2), math.pow((means.tags - elem.tags), 2), math.pow((means.likes - elem.likes), 2), math.pow(means.followers - elem.followers, 2), math.pow((means.friends - elem.friends), 2), math.pow((means.favorites - elem.favorites), 2), math.pow((means.statuses - elem.statuses), 2), math.pow((means.listed - elem.listed), 2)))

    val sumdTuple = intermediateDataset.reduce((accum, elem) => Tuple
    (accum.text + elem.text, accum.tags + elem.tags, accum.likes + elem.likes, accum.followers + elem.followers, accum.friends + elem.friends, accum.favorites + elem.favorites, accum.statuses + elem.statuses, accum.listed + elem.listed))
    Tuple(math.sqrt(sumdTuple.text / denom), math.sqrt(sumdTuple.tags / denom), math.sqrt(sumdTuple.likes / denom), math.sqrt(sumdTuple.followers / denom), math.sqrt(sumdTuple.friends / denom), math.sqrt(sumdTuple.favorites / denom), math.sqrt(sumdTuple.statuses / denom), math.sqrt(sumdTuple.listed / denom))
  }

  def standardizeFeature(dataset: Dataset[Tuple], means: Tuple, std: Tuple): Dataset[(Tuple)] = {
    dataset.map(elem => Tuple((elem.text - means.text) / std.text, (elem.tags - means.tags) / std.tags, (elem.likes - means.likes) / std.likes, (elem.followers - means.followers) / std.followers, (elem.friends - means.friends) / std.friends, (elem.favorites - means.favorites) / std.favorites, (elem.statuses - means.statuses) / std.statuses, (elem.listed - means.listed) / std.listed))

  }

  def cost(dataset: Dataset[ExtendedTuple],sampleSize:Long): Double = {

    //for each row, square(hθ(X(i))−y(i)) then sum all entries and divide by 2 * sampleSize
    val sampleSize = dataset.count()
    val accumulatedError = dataset.map(elem => (math.pow(elem.error, 2))).reduce((acc, elem) => acc + elem)
    val cost = ((1.toDouble / (2 * sampleSize)) * accumulatedError)
    cost
  }


  def expandEntries(dataset: Dataset[Tuple], theta: Theta): Dataset[ExtendedTuple] = {
    dataset.map(elem => ExtendedTuple(((theta.errCoeff + elem.text * theta.txtCoef + elem.tags * theta.tagCoeff + elem.followers * theta.follCoef + elem.friends * theta.frndCoeff + elem.favorites * theta.favCoeff + elem.statuses * theta.statusCoef + elem.listed * theta.listedCoef) - elem.likes ), 1, elem.text, elem.tags, elem.followers, elem.friends, elem.favorites, elem.statuses, elem.listed))
  }

  def genRandom(): Double={
    //generate a random number in the range [-2,2]
    -2 + scala.util.Random.nextDouble()*2
  }
  def testGradientDescent(dataset:Dataset[Tuple],theta: Theta): Double ={
    val countExamples = dataset.count()
    var rddWithErrorEntries = expandEntries(dataset,theta).persist()
    var cost1 = cost(rddWithErrorEntries,countExamples)
    cost1
  }

  def calculateLinearRegCoeff(dataset: Dataset[Tuple],sampleSize: Long): Theta = {
    val r = scala.util.Random
    val alpha = 0.003 //learning rate
    val sigma = 0.001 //minimum cost
    val limit = 10
    var coefficients = Theta(r.nextDouble*limit,r.nextDouble*limit,r.nextDouble*limit,r.nextDouble*limit,r.nextDouble*limit,r.nextDouble*limit,r.nextDouble*limit,r.nextDouble*limit) //initial guessed coefficients
    var datasetWithErrorEntries = expandEntries(dataset,coefficients).persist()
    var error = cost(datasetWithErrorEntries,sampleSize)
    var delta: Double =0
    do{
      coefficients = updateTheta(coefficients,datasetWithErrorEntries,alpha,sampleSize) //update coefficients
      datasetWithErrorEntries = expandEntries(dataset,coefficients).persist()
      val cost1 = cost(datasetWithErrorEntries,sampleSize)
      delta = error - cost1
      error = cost1 //update error for the next round

    }while(delta>sigma)
    coefficients

  }

  def updateTheta(theta: Theta, dataset: Dataset[ExtendedTuple], alpha: Double, countExamples: Long): Theta = {

    val intermediateDifferences = dataset.map(elem => Theta(elem.error * elem.errCoeff, elem.error * elem.text, elem.error * elem.tags, elem.error * elem.followers, elem.error * elem.friends, elem.error * elem.favorites, elem.error * elem.statuses, elem.error * elem.listed)).reduce((acc, elem) => Theta(acc.errCoeff + elem.errCoeff, acc.txtCoef + elem.txtCoef, acc.tagCoeff + elem.tagCoeff, acc.follCoef + elem.follCoef, acc.frndCoeff + elem.frndCoeff, acc.favCoeff + elem.favCoeff, acc.statusCoef + elem.statusCoef, acc.listedCoef + elem.listedCoef))
    val factor = alpha * (1 / countExamples)


    Theta(theta.errCoeff - intermediateDifferences.errCoeff * factor, theta.txtCoef - intermediateDifferences.txtCoef * factor, theta.tagCoeff - intermediateDifferences.tagCoeff * factor, theta.follCoef - intermediateDifferences.follCoef * factor, theta.frndCoeff - intermediateDifferences.frndCoeff * factor, theta.favCoeff - intermediateDifferences.favCoeff * factor, theta.statusCoef - intermediateDifferences.listedCoef * factor, theta.statusCoef - intermediateDifferences.listedCoef * factor)


  }

  def printCoefficients(theta: Theta): Unit = {

    println("Coefficients: Err: " + theta.errCoeff + " TextSize: " + theta.txtCoef + " HTags count: " + theta.tagCoeff +
      " Followers: " + theta.follCoef + " Friends: " + theta.frndCoeff + " TotalLikes: " + theta.favCoeff + " Statuses: " + theta.statusCoef + " Listed: " + theta.listedCoef)
  }


    def main(args: Array[String]): Unit = {

      import sparkSession.implicits._

      val tweets: RDD[Tweet] = sparkSession.sparkContext.textFile("/Users/soft/Downloads/tweets", 128).map(parseTweet)
    //val tweets: RDD[Tweet] = sparkSession.sparkContext.textFile("/data/twitter/tweetsraw",128).map(parseTweet)
    val required_tweets: Dataset[Tweet] = tweets.toDS()
      val sampleSize = required_tweets.count()
    //numerical_fields_Tweets.foreach(println)
    val numerical_fields_Tweets = required_tweets.map(element => Tuple(element.text.size, element.hashTags.size, element.likes,
      element.followers_count, element.friends_count, element.favorites_count, element.statuses_count, element.listed_count))
    val columnAverages = calculateFeatureMean(numerical_fields_Tweets,sampleSize)
    val columnSTD = calculateFeatureSTD(numerical_fields_Tweets, columnAverages,sampleSize)
    val standardizedDataset = standardizeFeature(numerical_fields_Tweets, columnAverages, columnSTD).persist()

    printCoefficients(calculateLinearRegCoeff(standardizedDataset,sampleSize))



  }

}