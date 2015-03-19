package ignition.core.jobs.utils

import scala.collection.mutable

// A priority queue that is bounded in size.
trait BoundedPriorityQueue[T] {
  def +(e: T): BoundedPriorityQueue[T]
  def ++(it: BoundedPriorityQueue[T]): BoundedPriorityQueue[T]
  def ++(it: Iterable[T]): BoundedPriorityQueue[T]
  def toList: List[T]
}

object BoundedPriorityQueue {
  def apply[T](bound: Int)(implicit ordering: Ordering[T]): BoundedPriorityQueue[T] =
    new BoundedPriorityQueueImpl[T](new mutable.PriorityQueue[T]()(ordering), bound)

  private class BoundedPriorityQueueImpl[T](pq: mutable.PriorityQueue[T], bound: Int) extends BoundedPriorityQueue[T] {
    override def +(e: T): BoundedPriorityQueue[T] = {
      val pqClone = pq.clone()+=e
      while (pqClone.size > bound)
        pqClone.dequeue()
      new BoundedPriorityQueueImpl(pqClone, bound)
    }

    override def toList: List[T] = pq.iterator.toList

    override def ++(it: Iterable[T]): BoundedPriorityQueue[T] = {
      val updatedPq = it.foldLeft(pq.clone()) {
        (pqClone, e) => pqClone += e
          while (pqClone.size > bound)
            pqClone.dequeue()
          pqClone
      }
      new BoundedPriorityQueueImpl(updatedPq, bound)
    }

    override def ++(it: BoundedPriorityQueue[T]): BoundedPriorityQueue[T] = ++(it.toList)
  }
}
