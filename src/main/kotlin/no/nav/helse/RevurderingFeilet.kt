package no.nav.helse

import no.nav.helse.Revurderingsperiode.Companion.somRevurderinger
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.util.*
import javax.sql.DataSource

class RevurderingFeilet(rapidApplication: RapidsConnection, private val dataSource: () -> DataSource): River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_endret")
                it.demandValue("gjeldendeTilstand", "REVURDERING_FEILET")
                it.require("vedtaksperiodeId") { id -> UUID.fromString(id.asText()) }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].let { UUID.fromString(it.asText()) }

        dataSource().transactional {
            val uferdigeRevurderinger = Revurderingsperiode.alleUferdigeRevurderinger(listOf(vedtaksperiodeId), this)
                .map { it.feilet(vedtaksperiodeId) }
                .somRevurderinger()

            uferdigeRevurderinger.forEach {
                it.lagreStatus(this, context)
            }
        }
    }
}
