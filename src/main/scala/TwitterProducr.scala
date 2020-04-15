import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.Hosts
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util._
import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.io._

import com.google.common.collect.Lists


object TwitterProducr {

  def main(args: Array[String]): Unit = {
    new TwitterProducr().run()
  }
}

class TwitterProducr {

  var logger: Logger = LoggerFactory.getLogger(classOf[TwitterProducr].getName)


  val terms: List[String] = Lists.newArrayList("corona")

  def run(): Unit = {
    logger.info("Setup")

    //    Set your blocking queues : Be sure to size these properly based on expected TPS of your Stream
    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](100000)

    // create a twitter client
    val client: Client = createTwitterClient(msgQueue)
    // Attempts to establish a connection.
    client.connect()

    // create a kafka producer
    val producer: KafkaProducer[String, String] = createKafkaProducer()

    // add a shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("stopping application...")
      logger.info("shutting down client from twitter...")
      client.stop()
      logger.info("closing producer...")
      producer.close()
      logger.info("done!")
    }))
    // on a different thread, or multiple different threads....
    while (!client.isDone) {
      var msg: String = null
      try msg = msgQueue.poll(5, TimeUnit.SECONDS)
      catch {
        case e: InterruptedException => {
          e.printStackTrace()
          client.stop()
        }

      }
      if (msg != null) {
        val output = CreateData.data(msg)
        val fw= new FileWriter("test2.txt",true)
        try{
          fw.write(output)
        }
        finally fw.close()

        logger.info(msg)
        producer.send(new ProducerRecord("Twitter_topic_final", null, output), new Callback() {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            if (e != null)
              logger.error("Something bad happened", e)
          }
        })
      }
    }
    logger.info("End of application")
  }

  var consumerKey: String = "ed4hZFVHA6Ijh1xOaD2eWP8h8"
  var consumerSecret: String = "t1FcFFnUqx2SdhO8CA0JASFj3LLlfVpIODFfBFJgkWoG9mL9Ma"
  var token: String = "1245637284436697095-9UvJYcb2cmBNVL0UQbHDPwhbXR6iLK"
  var secret: String = "hE9dakUinpj6CgKJK1uQ8Q9X0FowsobhKRh8FKCBTY3ov"

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {

    //    Declare the host you want to connect to , the endpoint, and authentication
    val hosebirdHosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    //    set up track terms
    hosebirdEndpoint.trackTerms(terms)
    // These secrets should be read from a config file
    val hosebirdAuth: Authentication = new OAuth1(consumerKey, consumerSecret, token, secret)

    val builder: ClientBuilder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    val hosebirdClient: Client = builder.build()
    hosebirdClient
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {
    val bootstrapServers: String = "127.0.0.1:9092"

    // create Producer properties
    val properties: Properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // create safe Producer
    //    acks =0 no response is required, if the broker goes offline or an exception happens , we won't know and will lose data
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, java.lang.Integer.toString(java.lang.Integer.MAX_VALUE))

    // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    // high throughput producer (at the expense of a bit of latency and CPU usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10")
    // 32 KB batch size
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, java.lang.Integer.toString(32 * 1024))
    // create the producer
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    producer
  }




}

