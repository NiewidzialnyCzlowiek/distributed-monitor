package io.bartlomiejszal.dm

import java.io.Serializable

enum class MonitorMessage {
    REGISTER,
    REGISTER_RESPONSE,
    SIGNAL,
    SIGNAL_RESPONSE,
    ACQUIRE_MONITOR,
    ACQUIRE_MONITOR_RESPONSE,
    FREE_MONITOR,
    FREE_MONITOR_RESPONSE
}

data class DMMessage(
    val senderUid: String,
    val monitorMessage: MonitorMessage,
    var lamportTime: Long = 0,
    val appData: Serializable? = null) : Serializable