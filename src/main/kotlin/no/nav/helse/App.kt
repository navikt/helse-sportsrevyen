package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import no.nav.helse.rapids_rivers.RapidApplication
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("no.nav.helse.sportsrevyen")

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
        RevurderingFeilet(this, dataSourceBuilder::getDataSource)
        VedtaksperiodeForkastet(this, dataSourceBuilder::getDataSource)
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
            any { it == FERDIGSTILT_MANUELT } -> FERDIGSTILT_MANUELT
            any { it == FERDIGSTILT_AUTOMATISK } -> FERDIGSTILT_AUTOMATISK
            else -> ERSTATTET
        }
    }
}