package zio.postgres.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.Try
import scala.util.chaining.*

extension (bb: ByteBuffer) {
  def getString: Option[String] = {
    val pos = bb.position()
    def loop(acc: List[Byte]): Option[List[Byte]] = Try(bb.get).toOption.flatMap {
      case 0 => Some(acc)
      case x => loop(x :: acc)
    }

    loop(Nil)
      .map(_.reverse.toArray.pipe(new String(_, UTF_8)))
      .orElse {
        bb.position(pos) // reset position on parse failure
        None
      }
  }

  // zero terminated strings list
  def getStrings: Option[List[String]] = {
    val pos = bb.position()

    def loop(acc: List[String]): Option[List[String]] = {
      val pos = bb.position()
      Try(bb.get).toOption.flatMap {
        case 0 => Some(acc)
        case _ =>
          bb.position(pos)
          getString.map(_ :: acc).flatMap(loop(_))
      }
    }

    loop(Nil)
      .map(_.reverse)
      .orElse {
        bb.position(pos) // reset position on parse failure
        None
      }
  }

  def getShortSafe: Option[Short] = Try(bb.getShort).toOption
  def getIntSafe: Option[Int] = Try(bb.getInt).toOption
  def getLongSafe: Option[Long] = Try(bb.getLong).toOption
  def getByteSafe: Option[Byte] = Try(bb.get).toOption

}
