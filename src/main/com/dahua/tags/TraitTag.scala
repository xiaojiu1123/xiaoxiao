package com.dahua.tags

/**
 * 特性
 */
trait TraitTag {
  def makeTags(args: Any*): Map[String, Int]
}
