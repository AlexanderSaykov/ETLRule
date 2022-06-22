
import ApplicationArguments.initialArgs
import com.concurrentthought.cla.{Args, Opt}


class ApplicationArguments(val args: Array[String]) {
  val parsedInitialArgs: Args = initialArgs(args).parse(args)
}

object ApplicationArguments {

  val START_DATE: String = "start_date"
  val start_date: Opt[String] = Opt.string(
    name = START_DATE,
    flags = Seq("-s", "--start", "--start_date"),
    help = "Period start date",
    required = false)

  val END_DATE: String = "end_date"
  val end_date: Opt[String] = Opt.string(
    name = END_DATE,
    flags =
      Seq("-e", "--end", "--end_date"),
    help = "Period end date",
    required = false)


  def initialArgs(argstrings: Seq[String]): Args =
    Args(Seq(
      start_date,
      end_date,
      Args.quietFlag)).parse(argstrings)

  /** Get argument of application */
  def getOrException[T](appArgs: Args, key: String, required: Boolean): T = {
    val x: Option[T] = appArgs.get(key)
    x match {
      case Some(value) => value
      case None => if (required) throw new IllegalArgumentException(s"$key argument is empty or incorrect") else x.getOrElse(null.asInstanceOf[T])
    }
  }

  def apply(args: Array[String]): ApplicationArguments = new ApplicationArguments(args)
}
