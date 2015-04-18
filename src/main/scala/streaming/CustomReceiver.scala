package streaming

import java.io.{PrintWriter, InputStreamReader, BufferedReader}
import java.net.{ServerSocket, Socket}
import java.util.Random

import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by gaoyanjie on 2015/4/18.
 */
class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

object UseCustomReceiver {
  def main (args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val host = "localhost"
    val port = 9999
    val customReceiverStream = ssc.receiverStream(new CustomReceiver(host, port))
    val lines = customReceiverStream
    //val words = lines.flatMap(_.split(" "))
    //words.print()
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

object UseCustomerProducer {
  def main(args: Array[String]) {
    val port = 9999
    val viewsPerSecond = 10
    val sleepDelayMs = (1000.0 / viewsPerSecond).toInt
    val listener = new ServerSocket(port)
    println("Listening on port: " + port)

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(sleepDelayMs)
            out.write(getRandomString(3))
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }

  def getNextString() : String = {
    val rand = new Random().nextDouble()
    val word = rand + "helloword\r\n"
    println(word)
    word
  }

  def getRandomString(length: Int) : String = {
      val base = "abcdefghijklmnopqrstuvwxyz0123456789";
      val random = new Random();
      val sb = new StringBuffer();
      for (i <- 0 to length) {
        val number = random.nextInt(base.length());
        sb.append(base.charAt(number));
      }
      println(sb.toString)
      sb.append("\r\n")
      return sb.toString();
    }

}