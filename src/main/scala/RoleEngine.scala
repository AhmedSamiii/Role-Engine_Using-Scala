import java.sql.{Connection, DriverManager, PreparedStatement}
import java.io.{File, FileOutputStream, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.io.{BufferedSource, Source}

 // Logs a message with a specified log level and timestamp to the log file.
 // @param level The log level (e.g., INFO, ERROR).
 // @param message The message to be logged.
 
class Logger(logFile: String) {
    private val writer = new PrintWriter(new FileOutputStream(logFile, true))

    def log(level: String, message: String): Unit = {
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        writer.println(s"$timestamp $level $message")
        writer.flush()
    }

    def close(): Unit = writer.close()
}

 //Implicit class for auto-closing resources after use.
 // @param resource The resource to be automatically closed.
 // @tparam A The type of the resource, which must implement the AutoCloseable interface.

implicit class AutoClose[A <: AutoCloseable](resource: A) {
    def autoClose[B](code: A => B): B =
        try {
            code(resource)
        } finally {
            resource.close()
        }
}


object RoleEngine extends App {
    val source: BufferedSource = Source.fromFile("src/main/resources/TRX1000.csv")
    val orders: List[String] = source.getLines().drop(1).toList // drop header
    val logger = new Logger("rules_engine.log")

    // Define the function types
    type Qualifier = Order => Boolean
    type Calculator = Order => Double

    // Define a case class to represent an order
    case class Order(orderDate: LocalDate,
                     productName: String,
                     expiryDate: LocalDate,
                     quantity: Int,
                     unitPrice: Double,
                     channel: String,
                     paymentMethod: String,
                     remainingDays: Long) // Additional field

    // This method to convert each line to object for simplicity in operations
    def toOrder(line: String): Order = {
        val Splitedorder = line.split(",") // Split by tab character
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val orderDate = LocalDate.parse(Splitedorder(0).substring(0, 10), dateFormatter)
        val expiryDate = LocalDate.parse(Splitedorder(2), dateFormatter)
        val remainingDays = java.time.Duration.between(orderDate.atStartOfDay(), expiryDate.atStartOfDay()).toDays()
        Order(
            orderDate,
            Splitedorder(1),
            expiryDate,
            Splitedorder(3).toInt,
            Splitedorder(4).toDouble,
            Splitedorder(5),
            Splitedorder(6),
            remainingDays
        )
    }

    // ALl of the following are the Qualifiers and Calculators methods
    def isExpiringSoon(order: Order): Boolean = {
        // Check if remaining days is less than 30
        order.remainingDays < 30
    }

    def expiringDiscount(order: Order): Double = {
        // Calculate discount based on remaining days
        if (order.remainingDays >= 0 && order.remainingDays <= 29) {
            30 - order.remainingDays
        } else {
            0
        }
    }
        // Check if the product is either cheese or wine (case-insensitive)
    def cheeseAndWineOnSaleQualifier(order: Order): Boolean = {
        val productNameLowerCase = order.productName.toLowerCase
        productNameLowerCase.contains("cheese") || productNameLowerCase.contains("wine")
    }
	
        // Check if the product is cheese or wine and calculate discount accordingly
    def cheeseAndWineDiscountCalculator(order: Order): Double = {
        if (order.productName.toLowerCase.contains("cheese")) {
            10 // 10% discount for cheese
        } else if (order.productName.toLowerCase.contains("wine")) {
            5 // 5% discount for wine
        } else {
            0 // No discount for other products
        }
    }
	
        // Check if the order is on 23rd of March
    def specialDiscountQualifier(order: Order): Boolean = {
        order.orderDate.getMonthValue == 3 && order.orderDate.getDayOfMonth == 23
    }
	
        // 50% discount for products sold on 23rd of March
    def specialDiscountCalculator(order: Order): Double = {
        50.0
    }

        // Check if quantity is more than 5
    def quantityDiscountQualifier(order: Order): Boolean = {
        order.quantity > 5
    }

        // Calculate discount based on quantity
    def quantityDiscountCalculator(order: Order): Double = {
        if (order.quantity >= 6 && order.quantity <= 9) {
            5.0 // 5% discount for 6-9 units
        } else if (order.quantity >= 10 && order.quantity <= 14) {
            7.0 // 7% discount for 10-14 units
        } else if (order.quantity >= 15) {
            10.0 // 10% discount for more than 15 units
        } else {
            0.0 // No discount for less than 6 units
        }
    }

        // Check if the sales are made through the app
    def salesThroughAppQualifier(order: Order): Boolean = {
        order.channel.equalsIgnoreCase("App")
    }

    // Calculate discount based on quantity rounded up to the nearest multiple of 5 for sales made through the App
    def salesThroughAppDiscountCalculator(order: Order): Double = {
        if (order.channel.equalsIgnoreCase("App")) {
            val discountPercent = math.ceil(order.quantity / 5.0) * 5
            discountPercent
        } else {
            0.0
        }
    }


    // Check if the payment method is Visa card
    def visaCardQualifier(order: Order): Boolean = {
        order.paymentMethod.equalsIgnoreCase("Visa")
    }

        // 5% discount for sales made using Visa cards
    def visaCardDiscountCalculator(order: Order): Double = {
        5
    }

    // Define a function to return the list of tuples with functions
    def getRoleList(): List[(Qualifier, Calculator)] = {
        List(
            (isExpiringSoon, expiringDiscount),
            (cheeseAndWineOnSaleQualifier, cheeseAndWineDiscountCalculator),
            (specialDiscountQualifier, specialDiscountCalculator),
            (quantityDiscountQualifier, quantityDiscountCalculator),
            (salesThroughAppQualifier, salesThroughAppDiscountCalculator), // New rule
            (visaCardQualifier, visaCardDiscountCalculator) // New rule
        )
    }

    // Function to process orders and return the order data with or without discount
    def getOrderWithDiscount(order: Order, roleList: List[(Qualifier, Calculator)]): String = {
        // Filter roleList to get only the rules that qualify for the order
        val qualifyingRules = roleList.filter { case (qualifier, _) => qualifier(order) }

        if (qualifyingRules.nonEmpty) {
            // Calculate discounts for qualifying rules
            val discounts = qualifyingRules.map { case (_, calculator) => calculator(order) }

            // Order discounts in descending order and take top 2
            val topTwoDiscounts = discounts.sorted(Ordering[Double].reverse).take(2)

            // Calculate the average of the top two discounts
            val averageDiscount = if (topTwoDiscounts.nonEmpty) topTwoDiscounts.sum / topTwoDiscounts.length else 0.0

            // Calculate the final price
            val finalPrice = order.unitPrice * order.quantity * (1 - averageDiscount / 100)
            // Format the order line with the discount and return
            s"${order.orderDate},${order.productName},${order.expiryDate},${order.quantity},${order.unitPrice},${order.channel},${order.paymentMethod},${order.remainingDays},$averageDiscount,$finalPrice"
        } else {
            // If no rules qualify for the order, return the order line without discount
            s"${order.orderDate},${order.productName},${order.expiryDate},${order.quantity},${order.unitPrice},${order.channel},${order.paymentMethod},${order.remainingDays},0.0,${order.unitPrice*order.quantity}"
        }
    }

    // Function to write data to the database in batch mode
    def writeToDatabaseBatch(orderDataList: List[String]): Unit = {
        // Define connection parameters
        val url = "jdbc:oracle:thin:@localhost:1521/XE"
        val username = "airlinecompany_dwh"
        val password = "123"

        // Define the SQL query to check if the table exists
        val checkTableQuery = "SELECT table_name FROM user_tables WHERE table_name = 'ORDERS_WITH_DISCOUNT'"

        // Define the SQL query to create the table
        val createTableQuery =
            """
              |CREATE TABLE orders_with_discount (
              |    order_date DATE,
              |    product_name VARCHAR(255),
              |    expiry_date DATE,
              |    quantity INT,
              |    unit_price DECIMAL(10, 2),
              |    channel VARCHAR(255),
              |    payment_method VARCHAR(255),
              |    remaining_days INT,
              |    discount DECIMAL(10, 2),
              |    final_price DECIMAL(10, 2)
              |)
              |""".stripMargin.replaceAll("\n", " ")

        val insertQuery =
            """INSERT INTO orders_with_discount
              |(order_date, product_name, expiry_date, quantity, unit_price, channel, payment_method, remaining_days, discount, final_price)
              |VALUES
              |(TO_DATE(?, 'YYYY-MM-DD'), ?, TO_DATE(?, 'YYYY-MM-DD'), ?, ?, ?, ?, ?, ?, ?)""".stripMargin

        // Establish a connection and prepare the statement
        DriverManager.getConnection(url, username, password).autoClose { connection =>
            val tableExistsResultSet = connection.createStatement().executeQuery(checkTableQuery)
            val tableExists = tableExistsResultSet.next()

            if (!tableExists) {
                // Create the table if it doesn't exist(Because Oracle Doesn't support if not exist )
                connection.prepareStatement(createTableQuery).executeUpdate()
            }

            val preparedStatement = connection.prepareStatement(insertQuery)

            // Loop through each order data and add it to the batch
            orderDataList.foreach { orderData =>
                val dataArray = orderData.split(",")
                preparedStatement.setObject(1, dataArray(0)) // order_date
                preparedStatement.setObject(2, dataArray(1)) // product_name
                preparedStatement.setObject(3, dataArray(2)) // expiry_date
                preparedStatement.setObject(4, dataArray(3).toInt) // quantity
                preparedStatement.setObject(5, dataArray(4).toDouble) // unit_price
                preparedStatement.setObject(6, dataArray(5)) // channel
                preparedStatement.setObject(7, dataArray(6)) // payment_method
                preparedStatement.setObject(8, dataArray(7).toInt) // remaining_days
                preparedStatement.setObject(9, dataArray(8).toDouble) // discount
                preparedStatement.setObject(10, dataArray(9).toDouble) // final_price
                preparedStatement.addBatch()
            }

            // Execute the batch insertion
            val batchResult = preparedStatement.executeBatch()
        }
    }

    // Function to log counts
    def logCounts(totalOrdersRead: Int, ordersInserted: Int, ordersWithDiscount: Int, ordersWithoutDiscount: Int): Unit = {
        logger.log("INFO", s"Total orders read from CSV: $totalOrdersRead")
        logger.log("INFO", s"Total orders inserted into database: $ordersInserted")
        logger.log("INFO", s"Total orders with discount: $ordersWithDiscount")
        logger.log("INFO", s"Total orders without discount: $ordersWithoutDiscount")
    }

    // Process orders and collect order data
    val orderDataList = orders.map(toOrder).map(getOrderWithDiscount(_, getRoleList()))

    // Write data to the database in batch mode
    writeToDatabaseBatch(orderDataList)

    // Log the counts
    logCounts(
        totalOrdersRead = orders.length,
        ordersInserted = orderDataList.length,
        ordersWithDiscount = orderDataList.count(_.split(",")(8).toDouble > 0),
        ordersWithoutDiscount = orderDataList.count(_.split(",")(8).toDouble == 0)
    )

    // Close the source
    source.close()

    // Close the logger
    logger.close()
}

