package rings

class Stats {
  var messages: Int = 0
  var allocated: Int = 0
  var checks: Int = 0
  var touches: Int = 0
  var misses: Int = 0
  var errors: Int = 0

  def += (right: Stats): Stats = {
    messages += right.messages
    allocated += right.allocated
    checks += right.checks
    touches += right.touches
    misses += right.misses
    errors += right.errors
	  this
  }

  override def toString(): String = {
    s"Stats msgs=$messages alloc=$allocated checks=$checks touches=$touches miss=$misses err=$errors"
  }
}
