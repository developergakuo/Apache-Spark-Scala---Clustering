import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object TwitterAnalytics  {

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
                    listed_count:Double)
  case class Tuple(
                    text: Double,
                    tags: Double,
                    likes: Double,
                    followers: Double,
                    friends: Double,
                    favorites: Double,
                    statuses: Double,
                    listed:Double)


  case class ExtendedTuple( error:Double,
                            errCoeff: Double,
                            text: Double,
                            tags: Double,
                            followers: Double,
                            friends: Double,
                            favorites: Double,
                            statuses: Double,
                            listed:Double)
  case class Theta( errCoeff:Double,
                           txtCoef: Double,
                           tagCoeff: Double,
                           follCoef: Double,
                           frndCoeff: Double,
                           favCoeff: Double,
                           statusCoef: Double,
                           listedCoef:Double)



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
    val parsedTweetJSON =Try(parse(tweet)) //try to parse tweet json
    parsedTweetJSON match {
      case Success(value) => // for succesfully parsed tweet json, do the following

        var tweet_text: String = "Text"
        var hashtag:Array[Tag] = Array("no_hashtag")
        var likes: Likes = 0
        var  followers_count: Double =0
        var friends_count: Double = 0
        var favorites_count: Double= 0
        var statuses_count: Double =0
        var listed_count:Double =0
        //get only interesting tweets with likes
        if (value.has("retweeted_status")){
          val retweeted_favorite_count0 = value\"retweeted_status"\"favorite_count"
          val retweeted_favorite_count1= Try(retweeted_favorite_count0.extract[Likes])
          retweeted_favorite_count1 match {
            case Success(result)=>likes= result
            case Failure(exception)=>likes=  -1
            case _=> likes= -1
          }

          val text = value \"retweeted_status"\ "text"
          val tweettext= Try(text.extract[String])
          tweettext match {
            case Success(text) =>tweet_text= text
            case Failure(exception) =>tweet_text=  "blank Tweet"
          }


          val hashtags_J = value\"retweeted_status"\"entities"\"hashtags"\"text"
          hashtags_J match {
            case JArray(jarray)=> var hash_tag:Array[Tag] =  new Array[Tag](jarray.values.length)
              jarray.values.map(x=>hash_tag ++ Array(x.toString))
              hashtag =hash_tag
            case JString(jstring)=> hashtag =Array[Tag](jstring.toString)
            case JNothing => hashtag=Array[Tag]("Blank")
            case _ => hashtag = Array[Tag]("Blank")

          }
          val followers_count0 = value\"retweeted_status"\"user"\"followers_count"
          val followers_count1= Try(followers_count0.extract[Likes])
          followers_count1 match {
            case Success(result)=>followers_count= result
            case Failure(exception)=> followers_count = -1
            case _=> followers_count =  -1
          }
          val friends_count0 = value\"retweeted_status"\"user"\"friends_count"
          val friends_count1= Try(friends_count0.extract[Likes])
          friends_count1 match {
            case Success(result)=>friends_count= result
            case Failure(exception)=> friends_count=  -1
            case _=> friends_count= -1
          }

          val favorites_count0 = value\"retweeted_status"\"user"\"favourites_count"
          val favorites_count1= Try(favorites_count0.extract[Likes])
          favorites_count1 match {
            case Success(result)=>favorites_count= result
            case Failure(exception)=>favorites_count = -1
            case _=>favorites_count = -1
          }

          val statuses_count0 = value\"retweeted_status"\"user"\"statuses_count"
          val statuses_count1= Try(statuses_count0.extract[Likes])
          statuses_count1 match {
            case Success(result)=>statuses_count= result
            case Failure(exception)=> statuses_count = -1
            case _=> statuses_count = -1
          }
          val listed_count0 = value\"retweeted_status"\"user"\"listed_count"
          val listed_count1= Try(listed_count0.extract[Likes])
          listed_count1 match {
            case Success(result)=>listed_count= result
            case Failure(exception)=>listed_count= -1
            case _=>listed_count = -1
          }


        } else if (value.has("quoted_status")){
          val retweeted_favorite_count0 = value\"quoted_status"\"favorite_count"
          val retweeted_favorite_count1= Try(retweeted_favorite_count0.extract[Likes])
          retweeted_favorite_count1 match {
            case Success(result)=>likes= result
            case Failure(exception)=> likes= -1
            case _=>likes= -1
          }

          val text = value \"quoted_status"\ "text"
          val tweettext= Try(text.extract[String])
          tweettext match {
            case Success(text) =>tweet_text= text
            case Failure(exception) => tweet_text="blank Tweet"
          }


          val hashtags_J = value\"quoted_status"\"entities"\"hashtags"\"text"
          hashtags_J match {
            case JArray(jarray)=> var hash_tag:Array[Tag] =  new Array[Tag](jarray.values.length)
              jarray.values.map(x=>hash_tag ++ Array(x.toString))
              hashtag =hash_tag
            case JString(jstring)=> hashtag =Array[Tag](jstring.toString)
            case JNothing => hashtag= Array[Tag]("Blank")
            case _ => hashtag=Array[Tag]("Blank")

          }
          val followers_count0 = value\"quoted_status"\"user"\"followers_count"
          val followers_count1= Try(followers_count0.extract[Likes])
          followers_count1 match {
            case Success(result)=>followers_count= result
            case Failure(exception)=>followers_count= -1
            case _=>followers_count= -1
          }
          val friends_count0 = value\"quoted_status"\"user"\"friends_count"
          val friends_count1= Try(friends_count0.extract[Likes])
          friends_count1 match {
            case Success(result)=>friends_count= result
            case Failure(exception)=>friends_count= -1
            case _=> friends_count = -1
          }

          val favorites_count0 = value\"quoted_status"\"user"\"favourites_count"
          val favorites_count1= Try(favorites_count0.extract[Likes])
          favorites_count1 match {
            case Success(result)=>favorites_count= result
            case Failure(exception)=>favorites_count= -1
            case _=> favorites_count = -1
          }

          val statuses_count0 = value\"quoted_status"\"user"\"statuses_count"
          val statuses_count1= Try(statuses_count0.extract[Likes])
          statuses_count1 match {
            case Success(result)=>statuses_count= result
            case Failure(exception)=>statuses_count -1
            case _=> statuses_count= -1
          }
          val listed_count0 = value\"quoted_status"\"user"\"listed_count"
          val listed_count1= Try(listed_count0.extract[Likes])
          listed_count1 match {
            case Success(result)=>listed_count= result
            case Failure(exception)=> listed_count= -1
            case _=> listed_count= -1
          }

        }

        Tweet(tweet_text,hashtag,likes,followers_count,friends_count,favorites_count,statuses_count,listed_count)

      case Failure(exception) => Tweet("null",Array("no_hashtag"),0,0,0,0,0,0)

    }

  }



  def calculateFeatureMean(rdd: RDD[Tuple],sampleSize:Long):Tuple={
    val samplesize = rdd.count()
    val sumtuple= rdd.fold(Tuple(0,0,0,0,0,0,0,0))((accum,elem) =>Tuple
    (accum.text + elem.text,accum.tags + elem.tags,accum.likes + elem.likes,accum.followers + elem.followers,accum.friends  + elem.friends, accum.favorites + elem.favorites,accum.statuses + elem.statuses,accum.listed + elem.listed))
    Tuple(sumtuple.text/samplesize.toFloat,sumtuple.tags/samplesize.toFloat,sumtuple.likes/samplesize,sumtuple.followers/samplesize,sumtuple.friends/samplesize,sumtuple.favorites/samplesize,sumtuple.statuses/samplesize,sumtuple.listed/samplesize)
  }
  def calculateFeatureSTD(rdd: RDD[Tuple],means:Tuple,sampleSize:Long): Tuple={
    val denom = sampleSize - 1
    val intermediateRDD = rdd.map(elem=>Tuple(math.pow((means.text-elem.text),2),math.pow((means.tags-elem.tags),2),math.pow((means.likes-elem.likes),2),math.pow(means.followers-elem.followers,2),math.pow((means.friends-elem.friends),2),math.pow((means.favorites-elem.favorites),2),math.pow((means.statuses-elem.statuses),2),math.pow((means.listed-elem.listed),2)))

    val sumdTuple = intermediateRDD.fold(Tuple(0,0,0,0,0,0,0,0))((accum,elem) =>Tuple
    (accum.text + elem.text,accum.tags + elem.tags,accum.likes + elem.likes,accum.followers + elem.followers,accum.friends + elem.friends, accum.favorites + elem.favorites,accum.statuses + elem.statuses,accum.listed + elem.listed))
    Tuple(math.sqrt(sumdTuple.text/denom),math.sqrt(sumdTuple.tags/denom),math.sqrt(sumdTuple.likes/denom),math.sqrt(sumdTuple.followers/denom),math.sqrt(sumdTuple.friends/denom),math.sqrt(sumdTuple.favorites/denom),math.sqrt(sumdTuple.statuses/denom),math.sqrt(sumdTuple.listed/denom))
  }
  def standardizeFeature(rdd: RDD[Tuple], means:Tuple, std: Tuple):RDD[(Tuple)]= {
    rdd.map(elem=>Tuple((elem.text-means.text)/std.text,(elem.tags-means.tags)/std.tags,(elem.likes-means.likes)/std.likes,(elem.followers-means.followers)/std.followers,(elem.friends-means.friends)/std.friends,(elem.favorites-means.favorites)/std.favorites,(elem.statuses-means.statuses)/std.statuses,(elem.listed-means.listed)/std.listed))

  }
  def cost(rdd:RDD[ExtendedTuple],sampleSize:Long):Double={
    //for each row, square(hθ(X(i))−y(i)) then sum all entries and divide by 2 * sampleSize
    val accumulatedError=rdd.map(elem=>(math.pow(elem.error,2 ))).fold(0)((acc,elem)=>acc+elem)
    val cost =((1.toDouble/(2*sampleSize))*accumulatedError)
    cost
  }
  def expandEntries(rdd:RDD[Tuple],theta:Theta):RDD[ExtendedTuple]={
    //calculate predicted value and calculate error as ((hθ(X)=θ^TX)-Y) -> ExtendedTuple's error field and Coefficient's errorcoeff as ExtendedTuple's errorcoeff
    rdd.map(elem=>ExtendedTuple(((theta.errCoeff+elem.text*theta.txtCoef + elem.tags *theta.tagCoeff + elem.followers *theta.follCoef  +elem.friends *theta.frndCoeff +elem.favorites *theta.favCoeff + elem.statuses *theta.statusCoef +elem.listed *theta.listedCoef) - elem.likes),1,elem.text,elem.tags,elem.followers,elem.friends,elem.favorites,elem.statuses,elem.listed))
  }

  def genRandom(): Double={
    //generate a random number in the range [-2,2]
    -2 + scala.util.Random.nextDouble()*2
  }
  def testGradientDescent(rdd:RDD[Tuple],theta: Theta): Double ={
    val countExamples = rdd.count()
    var rddWithErrorEntries = expandEntries(rdd,theta).persist()
    var cost1 = cost(rddWithErrorEntries,countExamples)
    cost1
  }
  def gradientDescent(rdd:RDD[Tuple],sampleSize:Long): Theta ={
    val alpha = 0.001 //learning rate
    val sigma = 0.001
    var theta = Theta(genRandom(),genRandom(),genRandom(),genRandom(),genRandom(),genRandom(),genRandom(),genRandom()) //initial guessed coefficients
    var rddWithErrorEntries = expandEntries(rdd,theta).persist()
    var error = cost(rddWithErrorEntries,sampleSize)
    var delta: Double =0
    do{
      theta= updateTheta(theta,rddWithErrorEntries,alpha,sampleSize) //update coefficients
      rddWithErrorEntries = expandEntries(rdd,theta).persist()
      val cost1 = cost(rddWithErrorEntries,sampleSize)
      delta = error - cost1
      error = cost1 //update error for the next round

    }while(delta>sigma)
    theta

  }

  def updateTheta(theta:Theta,rdd:RDD[ExtendedTuple],alpha:Double,countExamples:Long): Theta={
    val factor = alpha*(1.toDouble/countExamples)
    //calculate coefficients deltas
    val intermediateDifferences= rdd.map(elem=>(elem.error*elem.errCoeff, elem.error*elem.text, elem.error*elem.tags, elem.error*elem.followers, elem.error*elem.friends, elem.error*elem.favorites, elem.error*elem.statuses,elem.error*elem.listed)) // (hθ(X(i))−y(i))x(i)j
      .fold((0,0,0,0,0,0,0,0))((acc,elem) => (acc._1+elem._1,acc._2+elem._2,acc._3+elem._3,acc._4+elem._4,acc._5+elem._5,acc._6+elem._6,acc._7+elem._7,acc._8+elem._8))

    //update coefficients
    Theta(theta.errCoeff - intermediateDifferences._1 *factor,theta.txtCoef - intermediateDifferences._2 *factor,theta.tagCoeff -intermediateDifferences._3 *factor,theta.follCoef -intermediateDifferences._4 *factor,theta.frndCoeff - intermediateDifferences._5*factor,theta.favCoeff - intermediateDifferences._6 *factor,theta.statusCoef - intermediateDifferences._7 *factor,theta.listedCoef - intermediateDifferences._8 *factor)


  }

  def printCoefficients(theta:Theta): Unit={

    println("Coefficients: Err: "+theta.errCoeff + " TextSize: "+ theta.txtCoef + " HTags count: " + theta.tagCoeff +
      " Followers: " + theta.follCoef + " Friends: "+ theta.frndCoeff + " TotalLikes: "+ theta.favCoeff + " Statuses: "+ theta.statusCoef + " Listed: "+ theta.listedCoef)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterAnalytics")
    //conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val tweets = sc.textFile("/Users/soft/Downloads/tweets",12).map(parseTweet).persist()
    val tweets = sc.textFile("/Users/soft/Downloads/testTweets2",8).map(parseTweet).persist()
    //val tweets = sc.textFile("/data/twitter/tweetsraw",72).map(parseTweet).persist()

    val required_tweets = tweets.filter(element => element.likes!=0)
    //required_tweets.foreach(println)

    val numerical_fields_Tweets = required_tweets.map(element=>Tuple(element.text.size,element.hashTags.size,element.likes,
      element.followers_count, element.friends_count,element.favorites_count,element.statuses_count,element.listed_count))
    //numerical_fields_Tweets.foreach(println)
    val sampleSize = numerical_fields_Tweets.count()
    val columnAverages =calculateFeatureMean(numerical_fields_Tweets,sampleSize)
    val columnSTD = calculateFeatureSTD(numerical_fields_Tweets,columnAverages,sampleSize)
    val standardizedRDD= standardizeFeature(numerical_fields_Tweets,columnAverages,columnSTD)

    printCoefficients(gradientDescent(standardizedRDD,sampleSize))
  println("TEST_COST: "+testGradientDescent(standardizedRDD,Theta(0.3798297578049597, 0.7181256623765928 , -0.1129920131678076 , 2.4219107633096986 , 0.22972193416344092 ,0.2696693537870232 , 0.014312531466087514 ,-2.3556630472098212)))


  }




}