package learn
import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.concurrent.ForkJoinWorkerThread

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import org.scalameter.api._

import scala.concurrent.{Await, Future}
import scala.io.Source

/**
  * Created by szelvenskiy on 2/4/17.
  */


trait Record{
  def get(index: Int): String
}

trait ColumnarDataSource[C<:Record]{

  def data: Iterator[C]

  def headers: Seq[String]

  def headerIndex(fieldName: String): Int

  implicit class FieldLookup(row: C){

    def getString(field: String): String = row.get(headerIndex(field))
    def getDouble(field: String): Double = {
      val strValue = getString(field)
      if(strValue.nonEmpty) strValue.toDouble else 0D
    }
    def getInt(field: String): Int = {
      val strValue = getString(field)
      if(strValue.nonEmpty) strValue.toInt else 0
    }
    def getSeq(field: String): Seq[String] = getString(field).split('|')
  }

}


class CSVWithPipes(fileName: String) extends ColumnarDataSource[Record]{

  import scala.collection.JavaConversions._
  import scala.collection.JavaConverters._

  private val input= CSVParser.parse(new File(fileName), Charset.defaultCharset(), CSVFormat.RFC4180).iterator().asScala

  private val headerLine = input.next

  private val fieldIndex = headerLine.zipWithIndex.toMap

  private def buildCSVRecordColumn(c: CSVRecord): Record  = new Record {
    override def get(index: Int): String = c.get(index)
  }

  override def data: Iterator[Record] = input.map(buildCSVRecordColumn)

  override def headers: Seq[String] = headerLine.iterator().toList

  override def headerIndex(fieldName: String): Int = fieldIndex(fieldName)


}

case class MovieFeatures(director: String, actors: Seq[String], year: Int, keywords: Seq[String], budget: Double, gross: Double, genres: Seq[String]){
  def profit: Double = gross - budget
}

class MovieFeaturesSource(rawSource: ColumnarDataSource[Record]){

  //val rawSource = new RawCSVDataSource("../data/movie_metadata.csv")

  import rawSource._

  val data: Iterator[MovieFeatures] = rawSource.data.map{row: Record =>
        MovieFeatures(
          director = row.getString("director_name"),
          actors = Seq(row.getString("actor_1_name"), row.getString("actor_2_name"), row.getString("actor_3_name")),
          year = row.getInt("title_year"),
          keywords = row.getSeq("plot_keywords"),
          budget = row.getDouble("budget"),
          gross = row.getDouble("gross"),
          genres = row.getSeq("genres")
        )
    }

}

object Partitioning {
  val dataIter = Source.fromFile("../data/movie_metadata_.csv").getLines()

  val header = dataIter.next()

  Files.createDirectory(Paths.get("../data/movie_metadata.csv/"))

  dataIter.grouped(1261).zipWithIndex.foreach{case (rows, partitionId) =>
    val file = new File(s"../data/movie_metadata.csv/part00$partitionId")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(header+"\n")
    rows.foreach{row => bw.write(row+"\n")}
    bw.close()
  }
}

object MoviesAnalyticsSequential{

  //val movieData = new MovieFeaturesSource(new CSVWithPipes("../data/movie_metadata.csv")).data

  def profitPerKeyword(movieData: Iterator[MovieFeatures]): Map[String, Double] = movieData
    .flatMap{row => row.keywords.map{keyword => keyword -> row.profit}}
    .foldLeft(Map[String, Double]()){case(agg, (keyword, profit)) => agg.updated(keyword, agg.getOrElse(keyword, 0D)+profit)}

  def result = profitPerKeyword(new MovieFeaturesSource(new CSVWithPipes("../data/movie_metadata_.csv")).data)

}

object MoviesAnalyticsParallel{

  import scala.concurrent.duration._

  import scala.concurrent.ExecutionContext.Implicits.global

  val parts = 0 to 3

  import MoviesAnalyticsSequential._

  def result = {

    val job = Future.sequence(parts.map{part =>
      Future{profitPerKeyword(new MovieFeaturesSource(new CSVWithPipes(s"../data/movie_metadata.csv/part00$part")).data)}
    }).map{ results: Seq[Map[String, Double]] =>
      results.reduce{(left: Map[String, Double], right: Map[String, Double])=>
        val res: Map[String, Double]  = (left.keySet ++ right.keySet).map{keyword =>
          keyword -> (left.getOrElse(keyword,0D) + right.getOrElse(keyword,0D))
        }.toMap
        res
      }
    }

    Await.result(job, 1 second)
  }

}

object Bench extends App{

  println("=======Start Seq")
  val beforeSeq = System.nanoTime()
  (0 to 1000).foreach{i=>MoviesAnalyticsSequential.result}
  val seqTime = System.nanoTime() - beforeSeq
  println(s"=======Seq: ${seqTime/1000000}")

  println("=======Start Par")
  val beforePar = System.nanoTime()
  (0 to 1000).foreach{i=>MoviesAnalyticsParallel.result}
  val parTime = System.nanoTime() - beforePar
  println(s"=======Seq: ${parTime/1000000}")

}

/*
object Benchmarks extends Bench.Microbenchmark{

  performance of "profitPerKeyword" in {
    measure method "sequential" in {
      MoviesAnalyticsSequential.result
    }
    measure method "parallel" in {
      MoviesAnalyticsParallel.result
    }

  }

}
*/