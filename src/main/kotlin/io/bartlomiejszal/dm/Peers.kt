package io.bartlomiejszal.io.bartlomiejszal.dm

import org.zeromq.ZMQ
import java.io.Serializable
import java.util.concurrent.ConcurrentHashMap

data class Peer(
    val uid: String,
    val address: String,
    val port: Int,
    val channel: ZMQ.Socket? = null) : Serializable

class PeerMap {
    private val peers: ConcurrentHashMap<String, Peer> = ConcurrentHashMap()

    fun add(peer: Peer) {
        if (!peers.containsKey(peer.uid)) {
            peers[peer.uid] = peer
        }
    }

    fun get(uid: String): Peer {
        return peers[uid]!!
    }

    fun getAllPeers(): List<Peer> {
        return peers.values.toList()
    }
}

data class Ticket(val uid: String, val lamport: Long): Comparable<Ticket> {
    override fun compareTo(other: Ticket): Int {
        val lamportCmp = this.lamport.compareTo(other.lamport)
        return if (lamportCmp == 0)
            this.uid.compareTo(other.uid)
        else
            lamportCmp
    }
}

class TicketQueue {
    private val tickets: MutableList<Ticket> = mutableListOf()

    @Synchronized fun add(ticket: Ticket) {
        tickets.add(ticket)
        tickets.sort()
    }

    @Synchronized fun isFirst(uid: String): Boolean =
        tickets.isNotEmpty() && tickets.first().uid == uid


    @Synchronized fun pop(uid: String) {
        if (tickets.isEmpty()) {
            return
        }
        if (tickets.first().uid == uid) {
            tickets.removeFirst()
        } else {
            throw IllegalMonitorStateException("Peer with no exclusive monitor access tried to release the monitor")
        }
    }

    @Synchronized fun contains(uid: String): Boolean {
        return tickets.firstOrNull { it.uid == uid } != null
    }
}