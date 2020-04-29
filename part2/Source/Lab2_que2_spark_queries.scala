import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
object Lab2_que2_spark_queries {
  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Fifa Spark Dataframe Sql")
      .config(conf = conf)
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // We rae using all 3 Fifa dataset given on Kaggle Repository
    //a.Import the dataset and create df and print Schema
    val worldcup = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("D:\\MS Assignments\\BDP\\Lab_2\\WorldCups.csv")

    // creation of structtype
    val structschema=StructType(
      StructField("roundid", IntegerType, true) ::
        StructField("matchid", IntegerType, true) ::
        StructField("initials", StringType, true) ::
        StructField("coachname", StringType, true) ::
        StructField("lineup", StringType, true) ::
        StructField("shirtno", IntegerType, true) ::
        StructField("playername", StringType, true) ::
        StructField("position", StringType, true) ::
        StructField("event", StringType, true) :: Nil)

    val structsch=spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED").schema(structschema)
      .load("D:\\MS Assignments\\BDP\\Lab_2\\WorldCupPlayers.csv")

    val players = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("D:\\MS Assignments\\BDP\\Lab_2\\WorldCupPlayers.csv")


    val matches = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("D:\\MS Assignments\\BDP\\Lab_2\\WorldCupMatches.csv")

    // Printing the Schema
    println("The schema for world cup is: ")
    worldcup.printSchema()
    println("The schema for players is: ")
    players.printSchema()
    println("The schema for matches is: ")
    matches.printSchema()
    println("The schema for structsch is: ")
    structsch.printSchema()

    //First of all create four Temp View
    worldcup.createOrReplaceTempView("worldcup")
    matches.createOrReplaceTempView("matches")
    players.createOrReplaceTempView("players")
    structsch.createOrReplaceTempView("sschema")

    // Counting the number of world cups won by Brazil
    val query1 = spark.sql("select count(Winner) from worldcup where Winner like '%Brazil%'")
    println("Number of times Italy stood as winner is:")
    query1.show()


    // Countries in which the attendance recorded is maximum
    val query2 = spark.sql("select Country,Attendance from worldcup Order By Attendance desc limit 8")
    println("Countries in which the attendance recorded is maximum:")
    query2.show()

    // Listing out the Nations that had recorded more than 30 goals
    val query3 = spark.sql("select Country, GoalsScored from worldcup where GoalsScored>30")
    println("Countries that scored more than 30 goals:")
    query3.show()

    // Sorting out the Years and the   goals scored in Descending order
    val query4 = spark.sql("select Year, GoalsScored from worldcup Order By GoalsScored desc")
    println("Sorting out the Countries and their goals scored in Descending order:")
    query4.show()

    // Implementing Join operation on Matches and Players Dataset
    val query5 = spark.sql("select p.MatchID,m.Stadium,m.RoundID from players p join matches m on (p.MatchID=m.MatchID) limit 10")
    println("Implementing Join operation on Matches and Players Dataset")
    query5.show()

    // Number of matches played in each year
    val query6 = spark.sql("select Year, count(Year) from matches group by Year")
    println("Count of number of matches played in each year")
    query6.show()

    // Number of matches played in each stadium along with the name of it is listed
    val query7 = spark.sql("select count(Stadium), Stadium from matches group by Stadium order by count(Stadium) desc limit 20")
    println("Count of matches played in each stadium")
    query7.show()

    // list all the number of times Caudron Rauol  was coach for France
    val query8 = spark.sql("select Coach_Name,count(Coach_Name) from players where Coach_Name LIKE '%CAUDRON Raoul (FRA)%' group by Coach_Name")
    println("Number of times Caudron Rauol trained France team:")
    query8.show()

    // List statistics of all games played by Sweden in the year 1938
    val query9 = spark.sql("select * from Matches where Home_Team_Name like '%Sweden%' AND Year like '%1938%'")
    println("Statistics of all the games played in Sweden:")
    query9.show()

    // List the players who are captains from all countries
    val query10 = spark.sql("select Player_Name, Position from players where Position like '%C%'")
    println("List of captains from all countries:")
    query10.show()

    // List players whose shirtnumber is 4. This is performed on structtype schema
    val query11 = spark.sql("select playername,shirtno from sschema where shirtno like '%4%'")
    query11.show()

    val csv = sc.textFile("D:\\MS Assignments\\BDP\\Lab_2\\WorldCups.csv")
    val header = csv.first()
    val data = csv.filter(line => line != header)
    val rdd = data.map(line=>line.split(",")).collect()

    //RDD for displaying year status.
    val fil = data.filter(line => line.contains("1998"))
    fil.collect.foreach(println)

    // using dataframe
    worldcup.filter("Year=1998").show(10)

    // using df-sql query
    spark.sql(" Select * from WorldCup where Year = 1998 ").show()

    //RDD for displaying winner counts
    val states = data.map(_.split(",")(1))
    val Scount = states.map(Scount => (Scount,1))
    val statecounts = Scount.reduceByKey((x,y)=> x+y).map(tup => (tup._2,tup._1))sortByKey(false)
    statecounts.take(10).foreach(println)

    worldcup.groupBy("Winner").count().filter("count > 0").orderBy("Winner").show(10)

    //using df-sql query
    val sql1: Unit = spark.sql("SELECT count(*),Winner FROM WorldCup GROUP BY Winner ORDER BY count(*) DESC").show()



    //RDD for showing details about few columns for 89 goals scored
    val rddSt = data.filter(line=>line.split(",")(6)=="89")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()
    rddSt.foreach(println)

    //using Dataframe
    worldcup.filter("GoalsScored=89").show()

    //using DF - Sql
    spark.sql(" Select * from WorldCup where GoalsScored = 89 ").show()

    //RDD
    // maximum qualified teams

    val rddMax = data.filter(line=>line.split(",")(7) == "24")
      .map(line=> (line.split(",")(0),line.split(",")(1))).collect()

    rddMax.foreach(println)

    // DataFrame
    worldcup.filter("QualifiedTeams == 24").show()

    // Spark SQL

    spark.sql(" Select * from WorldCup where MatchesPlayed in " + "(Select Max(QualifiedTeams) from WorldCup )" ).show()

    //RDD
    val venue = data.filter(line => line.split(",")(1)==line.split(",")(4))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(4)))
      .collect()

    venue.foreach(println)

    // Using Dataframe

    worldcup.select("Year","Country","Third").filter("Country==Third").show(10)

    // usig Spark SQL

    val venued: Unit = spark.sql("select Year,Country,Third from WorldCup where Country = Third").show()


  }
}
