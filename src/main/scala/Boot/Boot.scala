package Boot

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnNext}
import akka.stream.actor.{ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import akka.stream.scaladsl._
import org.akkamon.core.exporters.StatsdExporter
import org.akkamon.core.instruments.{CounterTrait, LoggingTrait, TimingTrait}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

case class Tick()

/**
  * Additionally add the following to make it more visible.
  * 1. Source sends out a sinus wave.
  * 2. Make sure that the output somehow also sends this to statsd
  *
  */
object Boot extends App {

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()
  val collectorActor = system.actorOf(Props[CollectingActor])

  val p = RunnableGraph.fromGraph(scenario6).run()

  /**
    * 1. Fast publisher, Faster consumer
    * - publisher with a map to send, and a throttler (e.g 50 msg/s)
    * - Result: publisher and consumer rates should be equal.
    */
  def scenario1: Graph[ClosedShape, akka.NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._


      // get the elements for this flow.
      val source = throttledSource(1 second, 20 milliseconds, 20000, "fastProducer")
      val fastSink = Sink.actorSubscriber(DelayingActor.props("fastSink", 0))

      // connect source to sink
      source ~> fastSink
      ClosedShape
    }
  }

  /**
    * Create a source which is throttled to a number of message per second.
    */
  def throttledSource(delay: FiniteDuration, interval: FiniteDuration, numberOfMessages: Int, name: String): Source[Int, NotUsed] =
    Source.fromGraph(
      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        // two sources
        val tickSource = Source.tick(delay, interval, ())
        val rangeSource = Source(1 to numberOfMessages)


        val sendMap = b.add(Flow[Int].map({ x => StatsdExporter.processCounter(name); x }))

        // we use zip to throttle the stream
        val zip = b.add(Zip[Unit, Int]())
        val unzip = b.add(Flow[(Unit, Int)].map(_._2))

        // setup the message flow
        tickSource ~> zip.in0
        rangeSource ~> zip.in1
        zip.out ~> unzip ~> sendMap

        SourceShape(sendMap.outlet)
      }
    )

  /**
    * 2. Fast publisher, fast consumer in the beginning get slower, no buffer
    * - same publisher as step 1. (e.g 50msg/s)
    * - consumer, which gets slower (starts at no delay, increase delay with every message.
    * - Result: publisher and consumer will start at same rate. Publish rate will go down
    * together with publisher rate.
    *
    * @return
    */
  def scenario2: Graph[ClosedShape, akka.NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // get the elements for this flow.
      val source = throttledSource(1 second, 20 milliseconds, 10000, "fastProducer")

      val slowingSink = Sink.actorSubscriber((SlowDownActor.props("slowingDownSink", 10l, 0)))

      // connect source to sink

      source ~> slowingSink
      ClosedShape
    }
  }

  /**
    * 3. Fast publisher, fast consumer in the beginning get slower, with drop buffer
    * - same publisher as step 1. (e.g 50msg/s)
    * - consumer, which gets slower (starts at no delay, increase delay with every message.
    * - Result: publisher stays at the same rate, consumer starts dropping messages
    */
  def scenario3: Graph[ClosedShape, akka.NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // first get the source
      val source = throttledSource(1 second, 30 milliseconds, 6000, "fastProducer")
      val slowingSink = Sink.actorSubscriber(SlowDownActor.props("slowingDownSinkWithBuffer", 20l, 0))

      // now get the buffer, with 100 messages, which overflow
      // strategy that starts dropping messages when it is getting
      // too far behind.
      //      val buffer = Flow[Int].buffer(3000, OverflowStrategy.dropHead)
      val buffer = Flow[Int].buffer(1000, OverflowStrategy.backpressure)

      // connect source to sink with additional step
      source ~> buffer ~> slowingSink
      ClosedShape
    }
  }

  /**
    * 4. Fast publisher, 2 fast consumers, one consumer which gets slower
    * - Result: publisher rate and all consumer rates go down at the same time
    */
  def scenario4: Graph[ClosedShape, akka.NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // first get the source
      val source = throttledSource(1 second, 20 milliseconds, 9000, "fastProducer")

      // and the sinks
      val fastSink = Sink.actorSubscriber(DelayingActor.props("broadcast_fastsink", 0l))
      val slowingDownSink = Sink.actorSubscriber(SlowDownActor.props("broadcast_slowsink", 20l, 0))

      // and the broadcast
      val broadcast = builder.add(Broadcast[Int](2))

      // use a broadcast to split the stream
      source ~> broadcast ~> fastSink
      broadcast ~> slowingDownSink
      ClosedShape
    }
  }

  /**
    * 5. Fast publisher, 2 fast consumers, one consumer which gets slower but has buffer with drop
    * - Result: publisher rate and fast consumer rates stay the same. Slow consumer goes down.
    */
  def scenario5: Graph[ClosedShape, akka.NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // first get the source
      val source = throttledSource(1 second, 20 milliseconds, 9000, "fastProducer")

      // and the sinks
      val fastSink = Sink.actorSubscriber(DelayingActor.props("fastSink", 0l))
      val slowingDownSink = Sink.actorSubscriber(SlowDownActor.props("slowSink", 30l, 0))
      //      val buffer = Flow[Int].buffer(300, OverflowStrategy.dropTail)
      val buffer = Flow[Int].buffer(3500, OverflowStrategy.backpressure)

      // and the broadcast
      val broadcast = builder.add(Broadcast[Int](2))

      // connect source to sink with additional step
      source ~> broadcast ~> fastSink
      broadcast ~> buffer ~> slowingDownSink
      ClosedShape
    }
  }

  /**
    * 6. Fast publisher (50msg/s), 2 consumer which total 70msg/s, one gets slower with balancer
    * - Result: slowly more will be processed by fast one. When fast one can't keep up, publisher
    * will slow down*
    */
  def scenario6: Graph[ClosedShape, akka.NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // first get the source
      val source = throttledSource(1 second, 10 milliseconds, 20000, "fastProducer")

      // and the sin
      val fastSink = Sink.actorSubscriber(DelayingActor.props("fastSinkWithBalancer", 12l))
      val slowingDownSink = Sink.actorSubscriber(SlowDownActor.props("slowingDownWithBalancer", 14l, 1l))
      val balancer = builder.add(Balance[Int](2))

      // connect source to sink with additional step
      source ~> balancer ~> fastSink
      balancer ~> slowingDownSink
      ClosedShape
    }
  }


  //  {
  //    Source[Int]() { implicit b =>
  //      import GraphDSL.Implicits._
  //
  //      // two source
  //      val tickSource = Source.tick(delay, interval, Tick())
  //      val rangeSource = Source(1 to numberOfMessages)
  //
  //      // we collect some metrics during processing so we can count the rate
  //      val sendMap = b.add(Flow[Int].map({ x => StatsdExporter.processCounter(name); x }))
  //
  //      // we use zip to throttle the stream
  //      val zip = b.add(Zip[Tick, Int]())
  //      val unzip = b.add(Flow[(Tick, Int)].map(_._2))
  //
  //      // setup the message flow
  //       tickSource ~> zip.in0
  //      rangeSource ~> zip.in1
  //                     zip.out ~> unzip ~> sendMap
  //
  //      sendMap.outlet
  //    }
  //  }
}


case class event(ts: Long, value: Double)

case class finialize()

class CollectingActor extends Actor {

  import scala.collection.mutable

  val resultCollector = mutable.Map[Long, Double]()

  override def receive: Receive = {
    case event(ts, value) => resultCollector += ts -> value
    case finalize => Util.sendToInfluxDB(Util.convertToInfluxDBJson(s"values-publisher", resultCollector toMap))
  }
}

object DelayingActor {
  def props(name: String, initialDelay: Long) =
    Props(new DelayingActor(name, initialDelay))
}

class DelayingActor(name: String, delay: Long) extends ActorSubscriber with LoggingTrait with TimingTrait with CounterTrait {
  val resultCollector = mutable.Map[Long, Double]()

  actorName = name

  def this(name: String) {
    this(name, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) =>
      Thread.sleep(delay)
    //      println(s"In delaying actor sink $actorName: $msg")
    //      resultCollector += System.currentTimeMillis() -> msg

    case OnComplete =>
      println(s"Completed ${resultCollector.size}")
      Util.sendToInfluxDB(Util.convertToInfluxDBJson(s"values-$actorName", resultCollector toMap))
  }

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy
}

object SlowDownActor {
  def props(name: String, delayPerMsg: Long, initialDelay: Long) =
    Props(new SlowDownActor(name, delayPerMsg, initialDelay))
}

class SlowDownActor(name: String, delayPerMsg: Long, initialDelay: Long) extends ActorSubscriber with LoggingTrait with TimingTrait with CounterTrait {
  // default delay is 0
  var delay = 0l

  // setup actorname to provided name for better tracing of stats
  actorName = name

  def this(name: String) {
    this(name, 0, 0)
  }

  def this(name: String, delayPerMsg: Long) {
    this(name, delayPerMsg, 0)
  }

  override def receive: Receive = {

    case OnNext(msg: Int) =>
      delay += delayPerMsg
      Thread.sleep(initialDelay + (delay / 1000), delay % 1000 toInt)
    case _ =>
  }

  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy
}


