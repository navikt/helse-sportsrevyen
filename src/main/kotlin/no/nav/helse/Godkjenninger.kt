package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.Revurderingsperiode.Companion.alleUferdigeRevurderinger
import no.nav.helse.Revurderingsperiode.Companion.hentUferdigePerioder
import no.nav.helse.Revurderingsperiode.Companion.somRevurderinger
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

// plukker opp godkjenningsbehov som har en løsning, og lukker(?) den relevante revurderingen
class Godkjenninger(rapidApplication: RapidsConnection, private val dataSource: () -> DataSource) :
    River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@final", true)
                it.demandAll("@behov", listOf("Godkjenning"))
                it.require("utbetalingId") { id -> UUID.fromString(id.asText()) }
                it.requireKey("@løsning.Godkjenning.godkjent")
                it.requireKey("@løsning.Godkjenning.automatiskBehandling")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val utbetalingId = UUID.fromString(packet["utbetalingId"].asText())
        dataSource().transactional {
            val vedtaksperioder = hentUferdigePerioder(this, utbetalingId)
            if (vedtaksperioder.isEmpty()) return@transactional // godkjenningsbehovet traff ikke noe vi lagret, f.eks. en førstegangsbehandling
            sikkerlogg.info("håndterer godkjenning for utbetaling $utbetalingId")

            val uferdigRevurdering = alleUferdigeRevurderinger(vedtaksperioder, this)
                .map {
                    it.godkjenning(
                        berørtePerioder = vedtaksperioder,
                        utbetalingGodkjent = packet["@løsning.Godkjenning.godkjent"].asBoolean(),
                        utførtMaskinelt = packet["@løsning.Godkjenning.automatiskBehandling"].asBoolean()
                    )
                }
                .somRevurderinger()
            uferdigRevurdering.forEach { uferdigPeriode ->
                uferdigPeriode.lagreStatus(this, context)
            }
        }
    }



    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
}