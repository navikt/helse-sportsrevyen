package no.nav.helse

import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.helse.Revurderingsperiode.Companion.alleBerørtePerioder
import no.nav.helse.Revurderingsperiode.Companion.lagreStatus
import no.nav.helse.Revurderingsperiode.Companion.revurderingId
import no.nav.helse.Revurderingstatus.*
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
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

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val utbetalingId = UUID.fromString(packet["utbetalingId"].asText())
        val nyVedtaksperiodestatus = vedtaksperiodeStatus(
            utbetalingGodkjent = packet["@løsning.Godkjenning.godkjent"].asBoolean(),
            utførtMaskinelt = packet["@løsning.Godkjenning.automatiskBehandling"].asBoolean()
        )

        dataSource().transactional {
            val vedtaksperioder = hentUferdigePerioder(utbetalingId, nyVedtaksperiodestatus)
            if (vedtaksperioder.isEmpty()) return@transactional // godkjenningsbehovet traff ikke noe vi lagret, f.eks. en førstegangsbehandling
            sikkerlogg.info("håndterer godkjenning for utbetaling $utbetalingId")

            val revurderingId = vedtaksperioder.revurderingId()

            vedtaksperioder.forEach { uferdigPeriode ->
                uferdigPeriode.lagreStatus(this)
            }

            val alleBerørtePerioder = vedtaksperioder.alleBerørtePerioder(this)
            val nyRevurderingsstatus = alleBerørtePerioder.lagreStatus(this)

            if (nyRevurderingsstatus != IKKE_FERDIG) {
                context.publish(JsonMessage.newMessage("revurdering_ferdigstilt", mapOf(
                    "revurderingId" to revurderingId,
                    "status" to nyRevurderingsstatus,
                    "berørtePerioder" to alleBerørtePerioder.map { it.toJsonMap() }
                )).toJson().also {
                    sikkerlogg.info("publiserer revurdering_ferdigstilt:\n\t$it")
                })
            }

        }
    }

    private fun TransactionalSession.hentUferdigePerioder(
        utbetalingId: UUID?,
        nyVedtaksperiodestatus: Revurderingstatus
    ) = run(
        queryOf(hentUferdigePerioder, mapOf("utbetaling_id" to utbetalingId))
            .map { row ->
                Revurderingsperiode(
                    row.uuid("vedtaksperiode_id"),
                    row.uuid("revurdering_igangsatt_id"),
                    nyVedtaksperiodestatus
                )
            }.asList
    )

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")

        @Language("PostgreSQL")
        private const val hentUferdigePerioder = """
            select u.vedtaksperiode_id, v.revurdering_igangsatt_id from vedtaksperiode_utbetaling u
            join revurdering_vedtaksperiode v on u.vedtaksperiode_id = v.vedtaksperiode_id
            where u.utbetaling_id = :utbetaling_id
            and v.status = 'IKKE_FERDIG'
        """

        private fun vedtaksperiodeStatus(utbetalingGodkjent: Boolean, utførtMaskinelt: Boolean) =
            when (utbetalingGodkjent) {
                true -> when (utførtMaskinelt) {
                    true -> FERDIGSTILT_AUTOMATISK
                    false -> FERDIGSTILT_MANUELT
                }
                false -> when (utførtMaskinelt) {
                    true -> AVVIST_AUTOMATISK
                    false -> AVVIST_MANUELT
                }
            }
    }
}