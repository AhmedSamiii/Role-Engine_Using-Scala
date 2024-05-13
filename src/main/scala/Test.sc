import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.{BufferedSource, Source}
import java.time._

val source: BufferedSource = Source.fromFile("F:\\SAMI_ITI\\ITIMaterial\\Apache Scala\\RoleEngine\\src\\main\\Resources\\TRX1000.csv")
val orders= source.getLines().drop(1).toList // drop header

def ParsingOrder(order: String):  (String, String, String, String, String, String, String) = {
  val fields = order.split(",")
  (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6))
}


def isExpiringSoon(order: String): Boolean = {
  val Splitedorder = order.split(",")
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
  // Extract timestamp and expiry date
  val timestamp = LocalDateTime.parse(Splitedorder(0), formatter)
  val expiryDate = LocalDate.parse(Splitedorder(2))
  // Calculate remaining days
  val remainingDays = java.time.Duration.between(timestamp, expiryDate.atStartOfDay()).toDays()
  // Check if remaining days is less than 30
  remainingDays < 30
}


def calculateDiscount(order: String): Double = {
  val Splitedorder = order.split(",")
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
  // Extract timestamp and expiry date
  val timestamp = LocalDateTime.parse(Splitedorder(0), formatter)
  val expiryDate = LocalDate.parse(Splitedorder(2))
  // Calculate remaining days
  val remainingDays = java.time.Duration.between(timestamp, expiryDate.atStartOfDay()).toDays()
  // Calculate discount based on remaining days
  val discountPercentage = if (remainingDays >= 0 && remainingDays <= 29) {
    30.0 - remainingDays
  } else {
    0.0
  }
  return discountPercentage
}

  // Map isExpiringSoon over orders
  val expiringSoon = orders.map(isExpiringSoon)

  // Map calculateDiscount over orders
  val discounts = orders.map(calculateDiscount)
