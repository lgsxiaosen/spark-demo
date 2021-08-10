package handler


/**
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

    }



}
