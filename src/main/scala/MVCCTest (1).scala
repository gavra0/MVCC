object MVCCTest {
  // This is an example test file. Try others to debug your system!!!
  def main(args: Array[String]) {
    try {
      /* Example schedule:
         T1: I(1) C
         T2:        R(1) W(1)           R(1) W(1) C
         T3:                  R(1) W(1)             C
      */
      val t1: Int = MVCC.begin_transaction
      val t2: Int = MVCC.begin_transaction
      val t3: Int = MVCC.begin_transaction
      MVCC.insert(t1, 1, 13) // create obj1 and initialize with 13
      MVCC.commit(t1)

      MVCC.write(t2, 1, MVCC.read(t2, 1) * 2) // double value of obj1
      MVCC.write(t3, 1, MVCC.read(t3, 1) + 4) // increment value of obj1 by 4
      MVCC.write(t2, 1, MVCC.read(t2, 1) * 2) // double value of obj1
      // or, if you did the OPTIONAL BONUS WORK,
      //MVCC.write(t2, 1, MVCC.modquery(t2, 10).size)
      // obj1 = number of objs for which value mod 10 is zero
      MVCC.commit(t2)
      MVCC.commit(t3)


    }
    catch {
      case e: Exception => {
        println("Exception: "+e.getMessage)
      }
    }
  }
}


