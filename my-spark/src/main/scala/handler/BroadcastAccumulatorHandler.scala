package handler

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * 广播变量
 * 当前现状
 * 一个Executor有多个core，所以可以同时执行多个task，当Driver需要传递一个数据量很大的对象时，由于每一个task中都含有这么一个变量，这样一来，数据在executor中就存在多份。
 *
 * --2. 导致问题：
 *    1.在Executor数据冗余
 *    2.Executor内存可能溢出
 *    3.如果存在shuffle阶段，数据传输效率将会非常低。
 * 为了解决出现这种性能的问题，可以将数据独立出来，在executor的内存中只保留一份，防止shuffle操作。
 *
 * --3. 由于数据是保存在task中，如何独立出来呢？
 * 使用广播变量的模式。
 *
 * --4. 什么是广播变量？
 * 分布式共享只读变量
 *    1. 只读：只能被访问，不能被修改
 *    2. 共享：可以被当前executor中所有task访问，还可以被其他的executor访问
 *
 * --5. 广播变量的声明和使用
 * val list = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )
 * --声明广播变量
 * val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)
 * --使用广播变量
 * for ((k, v) <- broadcast.value)
 * ————————————————
 * 版权声明：本文为CSDN博主「Markble」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/m0_48283915/article/details/108430779
 *
 *
 * --------------------
 * 累加器
 * --步骤：
 *      1. 自定义累加器类，继承extends  AccumulatorV2[IN, OUT]
 *      2. IN：累加器输入数据的类型
 * OUT：累加器返回值的数据类型
 * 需指定如上两个参数的数据类型
 *      3. 重写AccumulatorV2中6个方法
 * --方法1：判断当前的累加器是初始化
 * override def isZero: Boolean = ???
 * --方法2：复制一个累加器
 * override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = ???
 * --方法3：重置累加器
 * override def reset(): Unit = ???
 * --方法4：向累加器中增加值
 * override def add(v: String): Unit = ???
 * --方法5：合并当前累加器和其他累加器，两两合并，此方法由Driver端调用，合并由executor返回的多个累加器
 * override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = ???
 * --方法6： 返回当前累加器的值
 * override def value: mutable.Map[String, Int] = ???
 *      4. 在Driver端的代码
 * a、 创建累加器
 * b、 注册累加器
 * c、 使用累加器 （注意:累加器不要写在转换算子里面,转换算子可能被多次执行）
 * d、 获取累加器的值
 *
 * -- 说明：方法1/2/3在闭包检测和序列化时会使用到。依次进行调用，调用的顺序是：
 * copy --> reset --> isZero
 */
object BroadcastAccumulatorHandler {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("BroadcastAccumulatorHandler").setMaster("local[*]")
        val sc = new SparkContext(conf)
        // 广播变量
        val rdd = sc.makeRDD(1 to 10)
        val broadcast = sc.broadcast(3)
        val broadcastV = rdd.map(_ * broadcast.value)
//        broadcastV.collect().foreach(println)

        // 累加器
        val wc = sc.makeRDD(List("hello spark", "hello nanjing", "welcome beijing", "hello beijing"))
        val acc = new WordCountAccumulator
        sc.register(acc)
        wc.flatMap(_.split(" ")).foreach(word => acc.add(word))
        println(acc.value)

        sc.stop()

    }



    class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {
        private var buffer = mutable.Map[String, Int]()
        // 判断当前的累加器是初始化
        override def isZero: Boolean = {
            buffer.isEmpty
        }

        // 复制一个累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
            new WordCountAccumulator
        }

        // 重置累加器
        override def reset(): Unit = {
            buffer.clear()
        }

        // 向累加器中增加值
        override def add(word: String): Unit = {
            buffer(word) = buffer.getOrElse(word, 0) + 1
        }

        // 合并当前累加器和其他累加器，两两合并，此方法由Driver端调用，合并由executor返回的多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
            val map1 = buffer
            val map2 = other.value

            buffer = map1.foldLeft(map2)((map, kv) => {
                map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
                map
            })

        }

        // 返回当前累加器的值
        override def value: mutable.Map[String, Int] = {
            buffer
        }
    }



}
