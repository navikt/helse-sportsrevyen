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
        GodkjenningerRiver(this, dataSourceBuilder::getDataSource)
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
    /** revurderinger har startet, men det er ting som ikke er ferdig */
    IKKE_FERDIG,
    /** datamaskinen trykker OK */
    FERDIGSTILT_AUTOMATISK,
    /** saksbehandler trykker OK */
    FERDIGSTILT_MANUELT,
    /** datamaskinen avviser saken */
    AVVIST_AUTOMATISK,
    /** saksbehandler avviser saken */
    AVVIST_MANUELT,
    /** revurderinger har feilet av andre årsaker */
    FEILET,
    /** en annen revurdering startet før denne ble ferdig */
    ERSTATTET;

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
