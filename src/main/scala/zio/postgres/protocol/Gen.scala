package zio.postgres.protocol

import zio.*

import java.nio.charset.StandardCharsets.UTF_8
import scala.quoted.*
import scala.annotation.meta.field
import java.nio.ByteBuffer
import java.nio.ByteOrder

object Gen {

  def lengthImpl(fields: Seq[(Expr[Field], Int)], symtab: Map[Int, Expr[Array[Byte]]])(using
      quotes: Quotes
  ): Expr[Int] = {
    import quotes.reflect.report

    val (lStatic, lExpr) = fields.foldLeft(0 -> Expr(0)) { case ((acc, lExpr), (curExpr, idx)) =>
      curExpr match {
        case '{ Field.Length }    => acc + 4 -> lExpr
        case '{ Field.Type($_) }  => acc -> lExpr
        case '{ Field.Int32($_) } => acc + 4 -> lExpr
        case '{ Field.Int32s($xs) } =>
          xs.value match {
            case Some(xs) => acc + xs.length * 4 -> lExpr
            case None     => acc -> '{ $lExpr + ${ xs }.length * 4 }
          }
        case '{ Field.Byte($_) } => acc + 1 -> lExpr
        case '{ Field.Bytes($xs) } =>
          xs.value match {
            case Some(xs) => acc + xs.length -> lExpr
            case None     => acc -> '{ $lExpr + ${ xs }.length }
          }
        case '{ Field.String($s) } =>
          s.value match {
            case Some(x) => acc + x.getBytes(UTF_8).length + 1 -> lExpr
            case None =>
              symtab.get(idx) match {
                case None => report.errorAndAbort(s"Byte representation of string field $idx was not found")
                case Some(strByteExpr) =>
                  acc + 1 -> '{ $lExpr + ${ strByteExpr }.length }
              }
          }
      }
    }

    '{
      ${ Expr(lStatic) } + $lExpr
    }
  }

  def genByteBuffer(
      fields: List[(Expr[Field], Int)],
      symtab: Map[Int, Expr[Array[Byte]]],
      acc: Expr[ByteBuffer],
      length: Expr[Int]
  )(using quotes: Quotes): Expr[ByteBuffer] = {
    import quotes.reflect.report

    fields match {
      case (curExpr, idx) :: tl =>
        curExpr match {
          case '{ Field.Length }    => genByteBuffer(tl, symtab, '{ ${ acc }.putInt($length) }, length)
          case '{ Field.Type($x) }  => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ x }) }, length)
          case '{ Field.Int32($x) } => genByteBuffer(tl, symtab, '{ ${ acc }.putInt(${ x }) }, length)

          case '{ Field.Int32s($xs) } =>
            val ee = '{
              ${ xs }.foldLeft(${ acc }) { (bb, i) => bb.putLong(i) }
            }
            genByteBuffer(tl, symtab, ee, length)

          case '{ Field.Byte($x) }   => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ x }) }, length)
          case '{ Field.Bytes($xs) } => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ xs }.toArray) }, length)
          case '{ Field.String($s) } =>
            s.value match {
              case Some(_) => genByteBuffer(tl, symtab, '{ ${ acc }.put(${ s }.getBytes(UTF_8)).put(0: Byte) }, length)
              case None =>
                symtab.get(idx) match {
                  case None => report.errorAndAbort(s"Byte representation of string field $idx was not found")
                  case Some(strByteExpr) =>
                    genByteBuffer(tl, symtab, '{ ${ acc }.put(${ strByteExpr }).put(0: Byte) }, length)
                }
            }
        }

      case Nil => acc
    }
  }

  def genStrBytes(
      fields: List[(Expr[Field], Int)],
      symtab: Map[Int, Expr[Array[Byte]]],
      allFields: List[(Expr[Field], Int)]
  )(using quotes: Quotes): Expr[Chunk[Byte]] = {
    fields match {
      case (curExpr, idx) :: tl =>
        curExpr match {
          case '{ Field.String($s) } =>
            s.value match {
              case None =>
                '{ val x = ${ s }.getBytes(UTF_8); ${ genStrBytes(tl, symtab + (idx -> 'x), allFields) } }

              case _ => genStrBytes(tl, symtab, allFields)
            }
          case _ => genStrBytes(tl, symtab, allFields)
        }
      case _ =>
        '{
          val length = ${ lengthImpl(allFields, symtab) }

          Chunk.fromByteBuffer(
            ${
              genByteBuffer(
                allFields,
                symtab,
                '{
                  ByteBuffer
                    .allocate(length)
                    .order(ByteOrder.BIG_ENDIAN)
                },
                'length
              )
            }
          )
        }
    }
  }

  def makeImpl(fields: Expr[Seq[Field]])(using Quotes): Expr[Chunk[Byte]] = {
    import quotes.reflect.report
    fields match {
      case Varargs(fieldExprs) =>
        val fieldExprsWithIdx = fieldExprs.toList.zipWithIndex
        val e = genStrBytes(fieldExprsWithIdx, Map.empty, fieldExprsWithIdx)

        // sys.error(e.show)

        e

      case other =>
        report.errorAndAbort("Expected explicit argument. Notation `args: _*` is not supported.", other)
    }
  }

  inline def make(inline fields: Field*): Chunk[Byte] = ${ makeImpl('fields) }

}
