import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexRDD, Edge => GXEdge}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.graphx.GraphLoader

val graph = GraphLoader.edgeListFile(sc, "/home/ajkamal/Desktop/zzz/Rbans/soc-Epinions1.txt")
val num_count = graph.vertices.count


case class VertexAttr(srcId: Long, authScore: Double, hubScore:Double)
case class HitsMsg(authScore:Double, hubScore:Double)
def reduction(a:HitsMsg,b:HitsMsg):HitsMsg = HitsMsg(a.authScore + b.authScore, a.hubScore + b.hubScore)
var gx = graph.mapVertices{case (x,y) => VertexAttr(x, 1.0/num_count,1.0/num_count)}
for (ii <- Range(1,25)) 
{
	val message: VertexRDD[HitsMsg] = gx.aggregateMessages(
	itr =>
	{
	itr.sendToDst(HitsMsg(0.0,itr.srcAttr.hubScore));
	itr.sendToSrc(HitsMsg(itr.dstAttr.authScore,0.0))
	}, reduction)
	val authsum = message.map(_._2.authScore).sum()
	val hubsum = message.map(_._2.hubScore).sum()
	gx = gx.outerJoinVertices(message) {
			case (vID, vAttr, optMsg) => {
			val msg = optMsg.getOrElse(HitsMsg(1.0/num_count, 1.0/num_count))
			

			VertexAttr(vAttr.srcId, if (msg.authScore == 0.0) 1.0 else msg.authScore , if (msg.hubScore == 0.0) 1.0 else msg.hubScore)
		}
	}

	///////////////////////////////////////////////////
	// here normalize gx.vertices.values(id,authScore,hubScore)  or gx.vertices.(id,VertexAttr(id,authScore,hubScore)) on authScore
	gx = gx.outerJoinVertices(message) {
			case (vID, vAttr, optMsg) => {
			val msg = optMsg.getOrElse(HitsMsg(1.0/num_count, 1.0/num_count))
			VertexAttr(vAttr.srcId, if (msg.authScore == 0.0) 1.0/num_count else msg.authScore/authsum , if (msg.hubScore == 0.0) 1.0/num_count else msg.hubScore/hubsum)
		}
	}
}


println("*******************************************************************************************************")
println("1. Top PageRank Clusttered Nodes")
println(graph.pageRank(0.0001,0.2).vertices.takeOrdered(5)(Ordering[Double].reverse.on(x=>x._2)).mkString("\n"))
println("*******************************************************************************************************")
println()

println("*******************************************************************************************************")
println("2.A Top AuthScored(HITS) Clusttered Nodes: AuthScore")
println(gx.vertices.takeOrdered(5)(Ordering[Double].reverse.on(x=>x._2.authScore)).mkString("\n"))
println("*******************************************************************************************************")

println()

println("*******************************************************************************************************")
println("2.B Top AuthScored(HITS) Clusttered Nodes: HubScore")
println(gx.vertices.takeOrdered(5)(Ordering[Double].reverse.on(x=>x._2.hubScore)).mkString("\n"))
println("*******************************************************************************************************")

println("*******************************************************************************************************")
println("3. Top SimRank Clusttered Nodes")
println(graph.personalizedPageRank(18, 0.0001).vertices.takeOrdered(6)(Ordering[Double].reverse.on(x=>x._2)).mkString("\n"))
println("*******************************************************************************************************")
println()

