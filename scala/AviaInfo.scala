import scala.io.BufferedSource
import scala.io.Source._

case class AviaCompanies(
                        NAME: String,
                        IATA: String,
                        COUNTRY:String,
                        SOURCE_AIRPORT:String
                        )


case class Report(
                  country:String,
                  iata:String,
                  amnt: Int
                  )

object AviaInfo {
    //не видит файл если путь не полный
  val file: BufferedSource = fromFile("C:\\Users\\User\\IdeaProjects\\untitled\\datasource\\avia\\airports.dat.txt","C:\\Users\\User\\IdeaProjects\\untitled\\datasource\\avia\\routes.dat.txt")

  val countrystat: String = file.toString//toString?


  def flyinfo(countrystat: Seq[AviaCompanies]):Iterable[Report] = countrystat.map{
    mapkey =>(
      mapkey.IATA,
      mapkey.NAME,
      mapkey.COUNTRY,
      mapkey.SOURCE_AIRPORT
    )

  }.groupBy{
    case(iata,_,_,_) => iata
  }.map{
    case(country, rep) => rep.foldLeft(Report(country, "", 0)){
      case(report,(name,iata,country,source)) => report.copy(
        amnt = report.amnt + 1,
        country = report.country
      )
    }
  }

}
