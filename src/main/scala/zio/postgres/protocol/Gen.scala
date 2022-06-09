package zio.postgres.protocol

import zio.*

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8
import scala.annotation.meta.field
import scala.quoted.*

object Gen {

  def genByteBuffer(
      fields: List[Expr[Field]],
      symtab: Map[Expr[Field], Expr[Array[Byte]]],
      acc: Expr[ByteBuffer],
      length: Expr[Int]
  )(using quotes: Quotes): Expr[ByteBuffer] = {
    import quotes.reflect.report

    fields match {
      case curExpr :: tl =>
        curExpr match {
          case '{ Field.Length } =>
            genByteBuffer(tl, symtab, '{ ${ acc }.putInt($length - ${ acc }.position()) }, length)
          case '{ Field.Type($x) }  => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ x }) }, length)
          case '{ Field.Int32($x) } => genByteBuffer(tl, symtab, '{ ${ acc }.putInt(${ x }) }, length)
          case '{ Field.Int16($x) } => genByteBuffer(tl, symtab, '{ ${ acc }.putShort(${ x }) }, length)

          case '{ Field.Int32s($xs) } =>
            val ee = '{
              ${ xs }.foldLeft(${ acc }) { (bb, i) => bb.putLong(i) }
            }
            genByteBuffer(tl, symtab, ee, length)

          case '{ Field.Byte($x) }   => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ x }) }, length)
          case '{ Field.Bytes($xs) } => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ xs }.toArray) }, length)
          case '{ Field.String($s) } =>
            s.value match {
              case Some(s) =>
                genByteBuffer(
                  tl,
                  symtab,
                  '{ ${ acc }.put(${ Expr(s.getBytes(UTF_8).appended(0: Byte)) }) },
                  length
                )
              case None =>
                symtab.get(curExpr) match {
                  case None =>
                    report.errorAndAbort(s"Byte representation of string field ${curExpr.show} was not found")
                  case Some(strByteExpr) =>
                    genByteBuffer(tl, symtab, '{ ${ acc }.put(${ strByteExpr }).put(0: Byte) }, length)
                }
            }
        }

      case Nil => acc
    }
  }

  def loop(
      exprs: List[Expr[Field]],
      strBytes: Map[Expr[Field], Expr[Array[Byte]]],
      lStaticAcc: Int,
      lExprAcc: Expr[Int]
  )(using quotes: Quotes, ctx: Ctx): Expr[ByteBuffer] = {
    import quotes.reflect.report

    exprs match {
      case cur :: tl =>
        cur match {
          case '{ Field.Length }    => loop(tl, strBytes, lStaticAcc + 4, lExprAcc)
          case '{ Field.Type($_) }  => loop(tl, strBytes, lStaticAcc + 1, lExprAcc)
          case '{ Field.Int32($_) } => loop(tl, strBytes, lStaticAcc + 4, lExprAcc)
          case '{ Field.Int16($_) } => loop(tl, strBytes, lStaticAcc + 4, lExprAcc)

          case '{ Field.Int32s($xs) } =>
            xs.value match {
              case Some(xs) => loop(tl, strBytes, lStaticAcc + xs.length * 4, lExprAcc)
              case None     => loop(tl, strBytes, lStaticAcc, '{ $lExprAcc + ${ xs }.length * 4 })
            }
          case '{ Field.Byte($_) } => loop(tl, strBytes, lStaticAcc + 1, lExprAcc)
          case '{ Field.Bytes($xs) } =>
            xs.value match {
              case Some(xs) => loop(tl, strBytes, lStaticAcc + xs.length, lExprAcc)
              case None     => loop(tl, strBytes, lStaticAcc, '{ $lExprAcc + ${ xs }.length })
            }
          case '{ Field.String($s) } =>
            s.value match {
              case Some(x) => loop(tl, strBytes, lStaticAcc + x.getBytes(UTF_8).length + 1, lExprAcc)
              case None =>
                '{
                  val x = ${ s }.getBytes(UTF_8)
                  ${ loop(tl, strBytes + (cur -> 'x), lStaticAcc + 1, '{ $lExprAcc + x.length }) }
                }
            }
        }
      case Nil =>
        '{
          val length = ${ Expr(lStaticAcc) } + $lExprAcc

          ${
            genByteBuffer(
              ctx.exprs,
              strBytes,
              '{
                ByteBuffer
                  .allocate(length)
                  .order(ByteOrder.BIG_ENDIAN)
              },
              'length
            )
          }.rewind
        }
    }
  }

  final case class Ctx(exprs: List[Expr[Field]])

  def makeImpl(fields: Expr[Seq[Field]])(using Quotes): Expr[ByteBuffer] = {
    import quotes.reflect.report

    fields match {
      case Varargs(fieldExprs) =>
        implicit val ctx = Ctx(fieldExprs.toList)
        loop(ctx.exprs, Map.empty, 0, Expr(0))

      case other =>
        report.errorAndAbort("Expected explicit argument. Notation `args: _*` is not supported.", other)
    }
  }

  inline def make(inline fields: Field*): ByteBuffer = ${ makeImpl('fields) }

}
