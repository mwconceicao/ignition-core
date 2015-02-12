package ignition.core.utils
import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.language.implicitConversions
import scalaz._

object CollectionUtils {

  implicit class RichCollection[A, Repr](xs: IterableLike[A, Repr]){
    def distinctBy[B, That](f: A => B)(implicit cbf: CanBuildFrom[Repr, A, That]) = {
      val builder = cbf(xs.repr)
      val i = xs.iterator
      var set = Set[B]()
      while(i.hasNext) {
        val o = i.next
        val b = f(o)
        if (!set(b)) {
          set += b
          builder += o
        }
      }
      builder.result
    }
  }

  implicit class ValidatedIterableLike[T, R, Repr <: IterableLike[Validation[R, T], Repr]](seq: IterableLike[Validation[R, T], Repr]) {
    def mapSuccess[That](f: T => Validation[R, T])(implicit cbf: CanBuildFrom[Repr, Validation[R, T], That]): That = {
      seq.map({
        case Success(v) => f(v)
        case failure => failure
      })
    }
  }

  implicit class OptionCollection(opt: Option[String]) {
    def isBlank: Boolean = {
      opt.isEmpty || opt.get.trim.isEmpty
    }

    def nonBlank: Boolean = !opt.isBlank

    def noneIfBlank: Option[String] = {
      if (opt.isBlank) None else opt
    }

  }

  // Useful to be called from java code
  def mutableMapToImmutable[K, V](map: scala.collection.mutable.Map[K, V]): Map[K, V] = {
    map.toMap
  }
}
