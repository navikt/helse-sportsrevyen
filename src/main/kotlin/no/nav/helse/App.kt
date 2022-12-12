package no.nav.helse

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection

fun main() {
    val env = System.getenv()

    RapidApplication.create(env).apply {
        // revurdering_igangsatt
        // vedtaksperiode_endret
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                println("hellow revurdering-monitor")
            }
        })
    }.start()
}