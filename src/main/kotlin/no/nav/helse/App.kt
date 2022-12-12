package no.nav.helse

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("no.nav.helse.sp-revurdering-montior")

fun main() {
    val env = System.getenv()
    Thread.currentThread().setUncaughtExceptionHandler { _, e ->
        log.error("{}", e.message, e)
    }
    launchApp(env)
}

fun launchApp(env: Map<String, String>) {
    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()

    RapidApplication.create(env).apply {
        RevurderingIgangsettelser(this, dataSource)
        // Tilstandsendringer(this, dataSource)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                println("hellow revurdering-monitor")
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}