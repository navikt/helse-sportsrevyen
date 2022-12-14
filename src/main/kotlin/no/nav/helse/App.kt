package no.nav.helse

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("no.nav.helse.sp-revurdering-monitor")

fun main() {
    val env = System.getenv()
    Thread.currentThread().setUncaughtExceptionHandler { _, e ->
        log.error("{}", e.message, e)
    }
    launchApp(env)
}

fun launchApp(env: Map<String, String>) {
    val dataSourceBuilder = DataSourceBuilder(env)

    RapidApplication.create(env).apply {
        RevurderingIgangsettelser(this, dataSourceBuilder::getDataSource)
        VedtaksperiodeUtbetalinger(this, dataSourceBuilder::getDataSource)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}