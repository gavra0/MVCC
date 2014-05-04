
// IMPORTANT -- THIS IS INDIVIDUAL WORK. ABSOLUTELY NO COLLABORATION!!!


// implement a (main-memory) data store with MVCC.
// objects are <int, int> key-value pairs.
// if an operation is to be refused by the MVCC protocol,
// undo its xact (what work does this take?) and throw an exception.
// garbage collection of versions is optional.
// Throw exceptions when necessary, such as when we try to execute an operation in
// a transaction that is not running; when we insert an object with an existing
// key; when we try to read or write a nonexisting key, etc.
// You may but do not need to create different exceptions for operations that
// are refused and for operations that are refused and cause the Xact to be
// aborted. Keep it simple!
// Keep the interface, we want to test automatically!

object MVCC {
  /* TODO -- your versioned key-value store data structure */

  private var max_xact: Int = 0

  // key -> rts, map of version - value pairs
  private var versions: Map[Int, (Int, Map[Int, Int])] = Map.empty

  // value k from query, list of transaction IDs
  private var queries: Map[Int, Set[Int]] = Map.empty

  // ID, [x1, x2, ... xn] where x1..xn depend on ID
  private var dependencies: Map[Int, Seq[Int]] = Map.empty
  // ID, numDepend where ID depends on numDepend transactions
  private var numDepend: Map[Int, Int] = Map.empty

  object TransactionStatus extends Enumeration {
    type TransactionStatus = Value
    val RUNNING, COMMIT_STARTED, COMMIT_FINISHED, COMMIT_UNSUCCESSFUL, ROLLBACK_STARTED, ROLLBACK_FINISHED = Value
  }

  import TransactionStatus._

  // all running transactions
  private var transactions: Map[Int, TransactionStatus] = Map.empty


  // returns transaction id == logical start timestamp
  def begin_transaction: Int = {
    max_xact += 1

    transactions += max_xact -> RUNNING
    dependencies += max_xact -> Seq.empty
    numDepend += max_xact -> 0

    max_xact
  }

  // create and initialize new object in transaction xact
  @throws(classOf[Exception])
  def insert(xact: Int, key: Int, value: Int) = {
    println("T(%d):I(%d,%d)".format(xact, key, value))
    checkIfValid(xact)

    // check if tuple with that id exists
    versions.get(key) match {
      case Some(_) => rollback(xact); throw new Exception("T(%d): I(%d,%d) KEY ALREADY EXISTS ".format(xact, key, value))
      case None =>
        // check queries that are 0 mod value, and that have higher ID
        if (checkQueries(xact, value).isEmpty) versions += key ->(xact, Map(xact -> value))
        else {
          rollback(xact)
          throw new Exception("T(%d):I(%d,%d) REJECTED".format(xact, key, value))
        }
    }
  }

  // return value of object key in transaction xact
  @throws(classOf[Exception])
  def read(xact: Int, key: Int): Int = {
    checkIfValid(xact)

    versions.get(key) match {
      case None => rollback(xact); throw new Exception("T(%d):R(%d) => NO SUCH KEY".format(xact, key))
      case Some(x: (Int, Map[Int, Int])) =>
        // find all versions <= xact
        val preVersion = x._2.filter((t: (Int, Int)) => t._1 <= xact)
        if (preVersion.isEmpty) {
          rollback(xact);
          throw new Exception("T(%d):R(%d) => NO VERSION FOUND".format(xact, key))
        }

        // get the earliest version
        val res = preVersion.max
        updateDependencies(xact, res._1)
        versions += key ->(Math.max(xact, x._1), x._2)
        println("T(%d):R(%d) => %d".format(xact, key, res._2))

        res._2
    }
  }

  // write value of existing object identified by key in transaction xact
  @throws(classOf[Exception])
  def write(xact: Int, key: Int, value: Int) {
    checkIfValid(xact)
    println("T(%d):W(%d,%d)".format(xact, key, value))
    versions.get(key) match {
      case None => rollback(xact); throw new Exception("T(%d):W(%d,%d) => NO SUCH KEY".format(xact, key, value))
      case Some(x: (Int, Map[Int, Int])) if xact < x._1 => rollback(xact); throw new Exception("T(%d):W(%d,%d) REJECTED".format(xact, key, value))
      case Some(x: (Int, Map[Int, Int])) =>
        // check against queries
        if (!checkQueries(xact, value).isEmpty) {
          rollback(xact)
          throw new Exception("T(%d):W(%d,%d) REJECTED".format(xact, key, value))
        }

        // if pass all of the checks
        versions += key ->(x._1, x._2 + (xact -> value))
    }
  }

  // Implementing queries is OPTIONAL for bonus points!
  // return the list of keys of objects whose values mod k are zero.
  // this is our only kind of query / bulk read. Your implementation must still
  // guarantee serializability. How do you deal with inserts? By maintaining
  // a history of querys with suitable metadata (xact?). Do you need a form of
  // locking?
  @throws(classOf[Exception])
  def modquery(xact: Int, k: Int): java.util.List[Integer] = {
    checkIfValid(xact)

    // add the query
    queries += k -> (queries.getOrElse(k, Set.empty) + xact)

    // filter only keys that satisfy conditions, and extract key and RTS
    val keyRTS = versions.map((x: (Int, (Int, Map[Int, Int]))) => {
      val posVersions = x._2._2.filter((v: (Int, Int)) => {
        v._1 <= xact
      })

      if (posVersions.isEmpty) (1, -1)
      else {
        val maxVersion = posVersions.max
        if (maxVersion._2 % k == 0) (x._1, maxVersion._1)
        else (1, -1)
      }
    }).filter(_._2 != -1)

    // update rts's
    keyRTS.foreach((x: (Int, Int)) => {
      updateDependencies(xact, x._2)
      versions += x._1 ->(Math.max(x._2, xact), versions(x._1)._2)
    })
    val ret = new java.util.ArrayList[Integer]()
    keyRTS.foreach(x => ret.add(x._1))
    println("T(%d):MODQUERY(%d) => %s".format(xact, k, ret.toString))
    ret
  }

  @throws(classOf[Exception])
  def commit(xact: Int) {
    println("T(%d):COMMIT START".format(xact))

    checkIfValid(xact)
    transactions += xact -> COMMIT_STARTED
    doCommit(xact)
  }

  def doCommit(xact: Int): Any = {
    if (numDepend(xact) == 0) {
      transactions += xact -> COMMIT_FINISHED
      println("T(%d):COMMIT FINISH".format(xact))

      // for each of the dependencies decrease the number of transactions to wait for, and try to commit if needed
      dependencies(xact).foreach((x: Int) => {
        numDepend += x -> (numDepend(x) - 1)
        if (transactions(x) == COMMIT_STARTED) doCommit(x)
      })
    }
  }

  /**
   * Check if transaction is running
   * @param xact transaction id
   */
  def checkIfValid(xact: Int) = if (transactions(xact) != RUNNING) throw new Exception("T(%d): NOT RUNNING ".format(xact))

  def checkAlive(xact: Int) = List(RUNNING, COMMIT_STARTED, ROLLBACK_STARTED).contains(transactions(xact))

  @throws(classOf[Exception])
  def rollback(xact: Int) {
    println("T(%d):ROLLBACK".format(xact))
    if (checkAlive(xact)) {
      transactions += xact -> ROLLBACK_STARTED
      doRollback(xact)
    }
  }

  def doRollback(xact: Int): Any = {
    // rollback every transaction that depended on this one
    dependencies(xact).foreach((x: Int) => {
      if (transactions(x) == COMMIT_STARTED) {
        println("T(%d):COMMIT UNSUCCESSFUL".format(x))
      }
      if (checkAlive(x)) rollback(x)
    })
    // remove the version of the object from the list
    versions = versions.map((x: (Int, (Int, Map[Int, Int]))) => x._1 ->(x._2._1, x._2._2 - xact))

    transactions += xact -> ROLLBACK_FINISHED
  }

  def updateDependencies(xact: Int, readVer: Int) {
    if (xact != readVer && checkAlive(readVer) && !dependencies(readVer).contains(xact)) {
      // add dependency for this transaction
      dependencies += readVer -> (dependencies(readVer) :+ xact)
      numDepend += xact -> (numDepend(xact) + 1)
    }
  }

  def checkQueries(xact: Int, value: Int) = queries.filter(x => value % x._1 == 0 && x._2.foldRight(false)((a: Int, b: Boolean) => b || (a > xact)))
}