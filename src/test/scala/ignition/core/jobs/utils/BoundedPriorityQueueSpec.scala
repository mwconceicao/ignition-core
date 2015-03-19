package ignition.core.jobs.utils

import org.joda.time.{Duration, Interval, DateTime}
import org.scalatest.{Matchers, FlatSpec}

class BoundedPriorityQueueSpec extends FlatSpec with Matchers {
  "BoundedPriorityQueue" must "respect bound" in {
    val bpq = BoundedPriorityQueue[Int](bound = 3)
    (bpq ++ Seq(2, 3, 4, 5, 6)).toList should contain theSameElementsAs Seq(2, 3, 4)
  }

  it must "discard the biggest value when adding" in {
    val bpq = BoundedPriorityQueue[Int](bound = 3) ++ Seq(4, 5, 6)
    (bpq + 3).toList should contain theSameElementsAs Seq(3, 4, 5)
  }

  it must "join keeping the smallest values" in {
    val bpq1 = BoundedPriorityQueue[Int](bound = 5) ++ Seq(4, 5, 6)
    val bpq2 = BoundedPriorityQueue[Int](bound = 12) ++ Range(1, 15)
    (bpq1 ++ bpq2).toList should contain theSameElementsAs Seq(1, 2, 3, 4, 4)
  }

  it must "deal well with huge inputs" in {
    val bpq = BoundedPriorityQueue[Int](bound = 5) ++ Range(1, 100000000)
    bpq.toList should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
  }

  it should "be faster than sorting for small bounds" in {
    val start1 = DateTime.now()
    val bpq = BoundedPriorityQueue[Int](bound = 5) ++ Range(40000000, 0, -1)
    val end1 = DateTime.now()
    bpq.toList should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
    val d1 = new Duration(start1, end1)
    println(s"Bounded priority queue took $d1 time")
    val start2 = DateTime.now()
    val slist = Range(40000000, 0, -1).sorted.take(5)
    val end2 = DateTime.now()
    val d2 = new Duration(start2, end2)
    println(s"List sorting took $d2 time")
    slist.toList should contain theSameElementsAs Seq(1, 2, 3, 4, 5)


    implicit val durationOrdering: Ordering[Duration] = Ordering.by(_.getMillis)
    d1 should be <= d2
  }

  it should "be faster than sorting for large bounds" in {
    val start1 = DateTime.now()
    val bpq = BoundedPriorityQueue[Int](bound = 1000000) ++ Range(1000000, 0, -1)
    val end1 = DateTime.now()
    val d1 = new Duration(start1, end1)
    println(s"Bounded priority queue took $d1 time")
    val start2 = DateTime.now()
    val slist = Range(1000000, 0, -1).sorted.take(1000000)
    val end2 = DateTime.now()
    val d2 = new Duration(start2, end2)
    println(s"List sorting took $d2 time")


    implicit val durationOrdering: Ordering[Duration] = Ordering.by(_.getMillis)
    d1 should be <= d2
  }

  it should "merge faster than lists" in {
    val v1 = Range(1000000, 0, -2).toList
    val v2 = Range(999999, 0, -2).toList
    val bpq1 = BoundedPriorityQueue[Int](bound = 10000) ++ v1
    val bpq2 = BoundedPriorityQueue[Int](bound = 10000) ++ v2
    val start1 = DateTime.now()
    val bpq = bpq1 ++ bpq2
    val end1 = DateTime.now()
    val d1 = new Duration(start1, end1)
    println(s"Bounded priority queue took $d1 time")
    val start2 = DateTime.now()
    val slist = (v1 ++ v2).sorted.take(10000)
    val end2 = DateTime.now()
    val d2 = new Duration(start2, end2)
    println(s"List sorting took $d2 time")


    implicit val durationOrdering: Ordering[Duration] = Ordering.by(_.getMillis)
    d1 should be <= d2
  }

  it should "accept unusual orderings" in {
    val bpq = BoundedPriorityQueue[String](bound = 5)(Ordering.by(_.length)) ++
      Seq("li", "lin", "elin", "elina", "angelina", "angelina jolie")
    // See http://blog.ricbit.com/2010/01/tatuagens-em-cadeia.html for an explanation of this seq
    bpq.toList should contain theSameElementsAs Seq("li", "lin", "elin", "elina", "angelina")
  }

}
