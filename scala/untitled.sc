import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}

case class PhoneTalk(
                      caller: Int,
                      receiver: Int,
                      startTime: LocalDateTime,
                      endTime: LocalDateTime,
                      cost: Int
                    )

case class Report(
                   user: Int,
                   totalDuration: Long,
                   totalCost: Int
                 )

object SimpleMapReduce {
  val talks: Seq[PhoneTalk] = Seq(
    PhoneTalk(1, 3, LocalDateTime.parse("2019-05-12T02:00:00"), LocalDateTime.parse("2019-05-12T02:05:00"), 5),
    PhoneTalk(2, 3, LocalDateTime.parse("2019-05-12T02:00:00"), LocalDateTime.parse("2019-05-12T02:30:00"), 10),
    PhoneTalk(3, 1, LocalDateTime.parse("2019-05-12T14:00:00"), LocalDateTime.parse("2019-05-12T14:01:00"), 1),
    PhoneTalk(3, 4, LocalDateTime.parse("2019-05-12T04:00:00"), LocalDateTime.parse("2019-05-12T04:04:30"), 5),
    PhoneTalk(2, 3, LocalDateTime.parse("2019-05-12T10:00:00"), LocalDateTime.parse("2019-05-12T10:10:00"), 4)
  )
  val stringFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("hh-mm-ss")

  def talksToReports(phoneTalks: Seq[PhoneTalk]): Iterable[Report] = phoneTalks.map {
    talk =>
      (
        talk.caller,
        talk.startTime,
        talk.endTime,
        talk.cost
      )
  }.groupBy {
    case (user, _, _, _) => user
  }.map {
    case (user, info) => info.foldLeft(Report(user, 0, 0)) {
      case (report, (_, start, end, cost)) => report.copy(
        totalDuration = report.totalDuration + Duration.between(end, start).getSeconds,
        totalCost = report.totalCost + cost
      )
    }
  }


}
