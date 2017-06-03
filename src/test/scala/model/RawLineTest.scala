package model

import org.scalatest._

class RawLineTest extends FunSuite {

  test("parseString") {
    assert(model.RawLine.parseString("").isEmpty)
    assert(model.RawLine.parseString(" ").isEmpty)
    assert(model.RawLine.parseString(" \t  ").isEmpty)
    assert(model.RawLine.parseString("a") == Option("a"))
    assert(model.RawLine.parseString("foo") == Option("foo"))
    assert(model.RawLine.parseString(" \tfoo") == Option("foo"))
    assert(model.RawLine.parseString("foo \n\t") == Option("foo"))
    assert(model.RawLine.parseString("  \t foo \n\t") == Option("foo"))
    assert(model.RawLine.parseString("\t\n  foo bar\n\t") == Option("foo bar"))
  }

  test("parseInt") {
    assert(model.RawLine.parseInt("").isEmpty)
    assert(model.RawLine.parseInt(" ").isEmpty)
    assert(model.RawLine.parseInt(" \t  ").isEmpty)
    assert(model.RawLine.parseInt("123") == Option(123))
    assert(model.RawLine.parseInt(" \t123") == Option(123))
    assert(model.RawLine.parseInt("123   \n ") == Option(123))
    assert(model.RawLine.parseInt(" \n123 ") == Option(123))
  }

  test("parseDate") {
    val d = Option(java.sql.Date.valueOf("2017-06-03"))
    assert(model.RawLine.parseDate("").isEmpty)
    assert(model.RawLine.parseDate(" ").isEmpty)
    assert(model.RawLine.parseDate(" \t  ").isEmpty)
    assert(model.RawLine.parseDate("2017-06-03") == d)
    assert(model.RawLine.parseDate(" \t2017-06-03") == d)
    assert(model.RawLine.parseDate("2017-06-03   \n ") == d)
    assert(model.RawLine.parseDate(" \n2017-06-03 ") == d)
  }

}
