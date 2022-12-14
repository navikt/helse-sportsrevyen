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
        Godkjenninger(this, dataSourceBuilder::getDataSource)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}

enum class Revurderingstatus {
    IKKE_FERDIG, // <- revurderinger har startet, men det er ting som ikke er ferdig
    FERDIGSTILT_AUTOMATISK, // <- datamaskinen trykker OK
    FERDIGSTILT_MANUELT, // <- saksbehandler trykker OK
    AVVIST_AUTOMATISK, // <- datamaskinen avviser saken
    AVVIST_MANUELT, // <- saksbehandler avviser saken
    FEILET, //<- revurderinger har feilet av andre årsaker
    ERSTATTET; // <- en annen revurdering startet før denne ble ferdig

    companion object {
        fun List<Revurderingstatus>.aggregertStatus() =
        when {
            any { it == IKKE_FERDIG } -> IKKE_FERDIG
            any { it == FEILET } -> FEILET
            any { it == AVVIST_MANUELT } -> AVVIST_MANUELT
            any { it == AVVIST_AUTOMATISK } -> AVVIST_AUTOMATISK
            all { it == ERSTATTET } -> ERSTATTET
            all { it in setOf(FERDIGSTILT_AUTOMATISK, ERSTATTET) } -> FERDIGSTILT_AUTOMATISK
            else -> FERDIGSTILT_MANUELT
        }
    }
}