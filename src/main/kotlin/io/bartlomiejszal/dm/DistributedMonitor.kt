package io.bartlomiejszal.dm

import io.bartlomiejszal.io.bartlomiejszal.dm.Peer
import io.bartlomiejszal.io.bartlomiejszal.dm.PeerMap
import io.bartlomiejszal.io.bartlomiejszal.dm.Ticket
import io.bartlomiejszal.io.bartlomiejszal.dm.TicketQueue
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.HashMap
import kotlin.math.max

open class DistributedMonitor(private val address: String,
                              private val port: Int) {

    val uid = UUID.randomUUID().toString()
    private var lamportTime = AtomicLong(0)
    private var acquiringMonitor = AtomicBoolean(false)

    private val zContext = ZContext()
    private val receiver = this.zContext.createSocket(SocketType.REP)

    private val peers = PeerMap()
    private val ticketQueue = TicketQueue()
    private val state: HashMap<String, Any> = HashMap()

    private var runListener = true
    private var holdingMonitor = false
    private val listener: Thread

    private val monitorEntryLock = ReentrantLock()
    private val monitorEntryCondition = monitorEntryLock.newCondition()
    private val monitorWaitLock = ReentrantLock()
    private val monitorWaitCondition = monitorWaitLock.newCondition()
    private val zmqLock = ReentrantLock()

    init {
        listener = Thread {
            listenerLoop()
        }
        listener.start()
    }

    private fun listenerLoop() {
        println("Starting listener loop...")
        receiver.bind("tcp://$address:$port")
        while (runListener) {
            receive()
        }
    }

    fun register(address: String, port: Int) {
        val newChannel = this.zContext.createSocket(SocketType.REQ)
        newChannel.connect("tcp://$address:$port")
        val msg = DMMessage(uid, MonitorMessage.REGISTER, appData = Peer(uid, this.address, this.port))
        val resp = send(newChannel, msg)
        val newPeer = resp.appData as Peer
        peers.add(newPeer.copy(channel = newChannel))
    }

    private fun send(channel: ZMQ.Socket, message: DMMessage): DMMessage {
        val msg = message.copy(lamportTime = lamportTime.incrementAndGet())
        return channel.reqSend(msg)
    }

    private fun broadcast(message: DMMessage): Pair<DMMessage, List<DMMessage>> {
        val msg = message.copy(lamportTime = lamportTime.incrementAndGet())
        return Pair(msg, peers.getAllPeers().map { p -> p.channel!!.reqSend(msg) })
    }


    private fun receive() {
        val message = deserialize<DMMessage>(receiver.recv()) ?: throw IllegalArgumentException("Message corrupt")
        lamportTime.updateAndGet { max(it, message.lamportTime) + 1 }
        println("$uid received - ${message.monitorMessage} - lamport: ${lamportTime.get()}")
        val resp = processMessage(message).copy(lamportTime = lamportTime.incrementAndGet())
        receiver.send(serialize(resp), ZMQ.DONTWAIT)
    }

    private fun processMessage(message: DMMessage): DMMessage =
        when (message.monitorMessage) {
            MonitorMessage.REGISTER -> handleRegister(message)
            MonitorMessage.ACQUIRE_MONITOR -> handleAcquireMonitor(message)
            MonitorMessage.FREE_MONITOR -> handleFreeMonitor(message)
            MonitorMessage.SIGNAL -> handleSignal(message)
            else -> throw Exception("Unhandled message type ${message.monitorMessage}")
        }

    private fun handleRegister(message: DMMessage): DMMessage {
        val peer = message.appData!! as Peer
        val channel = this.zContext.createSocket(SocketType.REQ)
        channel.connect("tcp://${peer.address}:${peer.port}")
        peers.add(peer.copy(channel = channel))
        return DMMessage(uid, MonitorMessage.REGISTER_RESPONSE, appData = Peer(uid, address, port))
    }

    private fun handleAcquireMonitor(message: DMMessage): DMMessage {
        println("$uid adding ticket ${message.senderUid} ${lamportTime.get()}")
        ticketQueue.add(Ticket(message.senderUid, message.lamportTime))
        return DMMessage(uid, MonitorMessage.ACQUIRE_MONITOR_RESPONSE)
    }

    private fun handleFreeMonitor(message: DMMessage): DMMessage {
        ticketQueue.pop(message.senderUid)
        updateState(message.appData as HashMap<String, Any>)
        monitorEntryLock.lock()
        println("$uid signalling monitor entry condition ${lamportTime.get()}")
        monitorEntryCondition.signalAll()
        monitorEntryLock.unlock()
        return DMMessage(uid, MonitorMessage.FREE_MONITOR_RESPONSE)
    }

    private fun handleSignal(message: DMMessage) : DMMessage {
        println("$uid handling singnal - lamport ${lamportTime.get()}")
        monitorWaitLock.lock()
        println("$uid monitor internal signal ${lamportTime.get()}")
        monitorWaitCondition.signalAll()
        monitorWaitLock.unlock()
        return DMMessage(uid, MonitorMessage.SIGNAL_RESPONSE)
    }

    fun acquireMonitor() {
        if (holdingMonitor) throw IllegalMonitorStateException("Cannot occupy the same monitor more than once")
        acquiringMonitor.set(true)
        val broadcastResult = broadcast(DMMessage(uid, MonitorMessage.ACQUIRE_MONITOR))
        val acquireMonitorRequest = broadcastResult.first
        println("$uid adding ticket ${acquireMonitorRequest.senderUid} ${lamportTime.get()}")
        ticketQueue.add(Ticket(broadcastResult.first.senderUid, acquireMonitorRequest.lamportTime))
        while (!ticketQueue.isFirst(uid)) {
            monitorEntryLock.lock()
            println("$uid cannot acquire monitor. Blocking until monitor is free - lamport: ${lamportTime.get()}")
            monitorEntryCondition.await()
            monitorEntryLock.unlock()
        }
        holdingMonitor = true
        acquiringMonitor.set(false)
    }

    fun waitDM() {
        if (!holdingMonitor) throw IllegalMonitorStateException("Cannot use wait operation when not holding the monitor")
        freeMonitor()
        monitorWaitLock.lock()
        println("$uid monitor internal wait ${lamportTime.get()}")
        monitorWaitCondition.await()
        monitorWaitLock.unlock()
        acquireMonitor()
    }

    fun signalAllDM() {
        if (!holdingMonitor) throw IllegalMonitorStateException("Cannot use signal operation when not holding the monitor")
        val signalMsg = DMMessage(uid, MonitorMessage.SIGNAL)
        broadcast(signalMsg)
    }

    fun freeMonitor() {
        val freeMonitorRequest = DMMessage(uid,MonitorMessage.FREE_MONITOR, appData = state)
        ticketQueue.pop(uid)
        holdingMonitor = false
        broadcast(freeMonitorRequest)
    }

    fun synchronize(synchronizedProcedure: () -> Any) {
        acquireMonitor()
        synchronizedProcedure()
        freeMonitor()
    }

    fun setVariable(key: String, value: Any) {
        if (!holdingMonitor) {
            throw IllegalMonitorStateException("Shared variables can be set only while holding the monitor")
        }
        state[key] = value
    }

    fun getVariable(key: String): Any? {
        if (!holdingMonitor) {
            throw IllegalMonitorStateException("Shared variables can be accessed only while holding the monitor")
        }
        return state[key]
    }

    private fun updateState(newState: HashMap<String, Any>) {
        newState.entries.forEach {
            state[it.key] = it.value
        }
    }

    fun finalize() {
        runListener = false
        listener.join()
    }

    companion object {
        fun <T>serialize(obj: T?) : ByteArray {
            if (obj == null) {
                return ByteArray(0)
            }
            val baos = ByteArrayOutputStream()
            val oos = ObjectOutputStream(baos)
            oos.writeObject(obj)
            oos.close()
            return baos.toByteArray()
        }

        fun <T>deserialize(bytes: ByteArray?) : T? {
            if (bytes == null || bytes.isEmpty()) {
                return null
            }
            val bais = ByteArrayInputStream(bytes)
            val ois = ObjectInputStream(bais)
            return ois.readObject() as T?
        }
    }

    private fun ZMQ.Socket.reqSend(msg: DMMessage): DMMessage {
        try {
            zmqLock.lock()
            this.send(serialize(msg), ZMQ.DONTWAIT)
            println("$uid sent - ${msg.monitorMessage} - lamport: ${msg.lamportTime}")
            val rep = deserialize<DMMessage>(this.recv())
            return rep!!
        } catch (e: Exception) {
            println("Peer $uid exception: ${e.message}. Sending message ${msg.monitorMessage}")
            throw e
        } finally {
            zmqLock.unlock()
        }
    }
}
