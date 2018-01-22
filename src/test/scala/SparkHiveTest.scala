import org.junit._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkHiveTest extends FunSuite with BeforeAndAfterAll {

  private val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  private var students: Dataset[Student] = _
  private var books: Dataset[Book] = _
  private var sparkApp: SparkHive = _

  override def beforeAll(): Unit = {

    val studentsList = Seq(Student(2, "Serhii", "Bzdel", 22),
                            Student(3, "Vova", "Tysjak", 23),
                            Student(4, "Andrii", "Savka", 21),
                            Student(5, "Taras", "Petrushak", 23),
                            Student(6, "Nazar", "Pron", 25))

    val booksList = Seq(Book(2, "Harry Poter 1", 2),
                         Book(3, "Harry Poter 2", 3),
                         Book(4, "Harry Poter 3", 4),
                         Book(5, "Harry Poter 4", 5),
                         Book(6, "Harry Poter 5", 6),
                         Book(7, "Harry Poter 6", 2),
                         Book(8, "Harry Poter 7_1", 3),
                         Book(9, "Harry Poter 7_2", 4))

    students = studentsList.toDS
    books = booksList.toDS()
    sparkApp = new SparkHive
  }

  test("test lengths"){
    Assert.assertTrue(books.count() == 8)
    Assert.assertTrue(students.count() == 5)
  }

  test("test dataset's class"){
    Assert.assertTrue(students.first().isInstanceOf[Student])
    Assert.assertTrue(books.first().isInstanceOf[Book])
  }

  test("test function for counting length of List"){
    val listOfFiveElements = List(1, 2, 3, 4, 5)
    Assert.assertTrue(sparkApp.lengthRecursive(listOfFiveElements) == 5)
  }

  test("test count of books for Serhii"){
    val studentsWithListOfBooks = sparkApp.getStudentsWithListOfBooks(spark, students, books)
    val listOfSerhiisBooks = studentsWithListOfBooks.get("Serhii")

    Assert.assertTrue(sparkApp.lengthRecursive(listOfSerhiisBooks.get) == 2)
  }

  test("test seventh parts of Potter books"){
    val seventhPartsBooks = sparkApp.createTempViewAndSelect(spark, books)
    Assert.assertTrue(seventhPartsBooks.count() == 2)
    val seventhPartsBooksList = seventhPartsBooks.collectAsList()
    Assert.assertTrue(seventhPartsBooksList.get(0).name.equalsIgnoreCase("Harry Poter 7_1"))
    Assert.assertTrue(seventhPartsBooksList.get(1).name.equalsIgnoreCase("Harry Poter 7_2"))
  }

  test("test UDF MySum of student's age"){
    var sum: Long = 0
    val listOfStudents = students.collect().toList
    listOfStudents.foreach(sum += _.age)
    Assert.assertEquals(sum, sparkApp.sumOfStudentsAge(students))
  }

}
