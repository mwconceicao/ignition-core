package ignition.jobs.utils

import akka.actor.{ActorSystem, ActorRefFactory}

import scala.concurrent.ExecutionContext


class ElasticSearchClient(override val elasticSearchHost: String, override val elasticSearchPort: Int)
                         (implicit _actorFactory: ActorRefFactory, _executionContext: ExecutionContext) extends ElasticSearchApi {
  override implicit def actorFactory: ActorRefFactory = _actorFactory
  override implicit def executionContext: ExecutionContext = _executionContext
}
