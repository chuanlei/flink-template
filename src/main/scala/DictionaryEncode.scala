import com.google.common.primitives.{UnsignedInteger, UnsignedInts}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class EncodeFunction extends RichMapFunction[Tuple, Int] {
  private var dict : MapState[String,Int] = _
  private var maxIndex: ValueState[Int] = _
  override def map(in: Tuple): Int = {
    val v = in.enc
    val prefix = v.hashCode % 16
    println("prefix is "+prefix)
    if(dict.contains(v)){
      val postfix = dict.get(v) & 0x0fffffff
      val r = ((prefix & 0x0000000f)<<28) | (postfix & 0x0fffffff)
      println(in.enc + " -> " + r)
      r
    } else {
      val postfix = maxIndex.value() + 1
      println("prefix & 0x0000000f is " + (prefix & 0x0000000f))
      println("postfix & 0x0fffffff is " + (postfix & 0x0fffffff))
      val r = ((prefix & 0x0000000f)<<28) | (postfix & 0x0fffffff)
      dict.put(in.enc, r)
      maxIndex.update(postfix)
      println(in.enc + " -> " + r)
      r
    }
  }

  override def open(parameters: Configuration): Unit = {
    dict = getRuntimeContext().getMapState(
      new MapStateDescriptor[String,Int]("dict", createTypeInformation[String],
        createTypeInformation[Int]))
    maxIndex = getRuntimeContext().getState(
      new ValueStateDescriptor[Int]("max", createTypeInformation[Int]))
  }
}

case class Tuple(enc : String)

class Splitter extends FlatMapFunction[String, Tuple]{
  override def flatMap(value: String, out: Collector[Tuple]): Unit = {
    out.collect(Tuple(value))
  }
}

class KeyByGrouping extends KeySelector[Tuple, Int]{
  override def getKey(value: Tuple): Int = {
    value.enc.hashCode % 16
  }
}

object DictionaryEncode {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("client01v.dfs.shbt.qihoo.net", 9999)
    text.flatMap(new Splitter).keyBy(new KeyByGrouping).map(new EncodeFunction()).print()
    env.execute("hh")
  }

}
