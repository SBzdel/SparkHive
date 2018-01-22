import org.apache.spark.sql.{Dataset, SparkSession}

case class Student (id: Int, firstName: String, lastName: String, age: Int)
case class Book (id: Int, name: String, student_id: Int)

class SparkHive {

  def getStudentsWithListOfBooks(spark: SparkSession, students: Dataset[Student], books: Dataset[Book]): Map[String, List[String]] ={

    import spark.implicits._

    val studentsWithListOfBooks = students.join(books, books.col("student_id") === students.col("id"))
      .select("firstName", "name")
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toList
      .groupBy(_._1)
      .map{ case (k, v) => (k, v.map(_._2)) }

    for ((k,v) <- studentsWithListOfBooks) println(k + " - " + v)

    studentsWithListOfBooks
  }

  def createTempViewAndSelect(spark: SparkSession, books: Dataset[Book]): Dataset[Book] = {

    import spark.implicits._

    books.createOrReplaceTempView("books")
    val seventhPartOfPotterBooks = spark.sql("SELECT * FROM books b WHERE b.name LIKE '%Harry%7%'").as[Book]
    seventhPartOfPotterBooks.show()
    seventhPartOfPotterBooks
  }

  def sumOfStudentsAge(students: Dataset[Student]): Long = {
    val ageSum = MySum.toColumn.name("age_sum")
    val result = students.select(ageSum)
    result.show()
    result.first()
  }

  def lengthRecursive[A](l:List[A]):Int = l match {
    case Nil => 0
    case h::tail => 1 + lengthRecursive(tail)
  }

}

object SparkHive {
  def main(args: Array[String]): Unit = {

    val sparkHive: SparkHive = new SparkHive

    val spark = SparkSession.builder()
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val students = spark.sql("SELECT * FROM test.student").as[Student]
    val books = spark.sql("SELECT * FROM test.book").as[Book]

    students.show()
    books.show()

    sparkHive.getStudentsWithListOfBooks(spark, students, books)
    sparkHive.createTempViewAndSelect(spark, books)
    sparkHive.sumOfStudentsAge(students)
  }
}
