package io.bartlomiejszal

import io.bartlomiejszal.dm.DistributedMonitor

object App {
    @JvmStatic fun main(args: Array<String>) {
        val BUFFER_FULL = "bufferFull"
        val BUFFER = "buffer"

        if (args.size == 1) {
            if (args[0] == "producer") {
                val producer = DistributedMonitor("localhost", 6666)
                val t = Thread {
                    producer.register("localhost", 6667)
                    producer.synchronize {
                        // init
                        println("Initializing monitor variables")
                        producer.setVariable(BUFFER, 0)
                        producer.setVariable(BUFFER_FULL, false)
                    }
                    for (i in 1..10) {
                        producer.acquireMonitor()
                        while (producer.getVariable(BUFFER_FULL) as Boolean) {
                            producer.waitDM()
                        }
                        producer.setVariable(BUFFER, i)
                        producer.setVariable(BUFFER_FULL, true)
                        println("${producer.uid} - produced ${producer.getVariable(BUFFER) as Int}")
                        producer.signalAllDM()
                        producer.freeMonitor()
                    }
                }
                t.start()
            } else if (args[0] == "consumer") {
                val consumer = DistributedMonitor("localhost", 6667)
                val t2 = Thread {
                    consumer.synchronize {
                        if (consumer.getVariable(BUFFER_FULL) == null)
                            consumer.waitDM()
                    }
                    for (i in 1..10) {
                        consumer.acquireMonitor()
                        while (!(consumer.getVariable(BUFFER_FULL) as Boolean)) {
                            consumer.waitDM()
                        }
                        val consumed = consumer.getVariable(BUFFER) as Int
                        consumer.setVariable(BUFFER_FULL, false)
                        consumer.signalAllDM()
                        println("${consumer.uid} - consumed $consumed")
                        consumer.freeMonitor()
                    }
                }
                t2.start()
            }
        }
    }
}

