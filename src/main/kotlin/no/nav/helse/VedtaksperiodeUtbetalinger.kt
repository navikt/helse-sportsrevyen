package no.nav.helse

import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeUtbetalinger(rapidApplication: RapidsConnection, private val dataSource: () -> DataSource): River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_ny_utbetaling")
                it.require("vedtaksperiodeId") { id -> UUID.fromString(id.asText()) }
                it.require("utbetalingId") { id -> UUID.fromString(id.asText()) }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].let { UUID.fromString(it.asText()) }
        val utbetalingId = packet["utbetalingId"].let { UUID.fromString(it.asText()) }

        dataSource().transactional {
            opprettVedtaksperiodeUtbetaling(vedtaksperiodeId, utbetalingId)
        }
    }

    private fun TransactionalSession.erBerørtPeriodeForAktivRevurdering(vedtaksperiodeId: UUID): Boolean {
        return run(queryOf(FINN_BERØRT_PERIODE, mapOf(
            "vedtaksperiodeId" to vedtaksperiodeId
        )).map { it.boolean(1) }.asList).single()
    }

    private fun TransactionalSession.opprettVedtaksperiodeUtbetaling(vedtaksperiodeId: UUID, utbetalingId: UUID) {
        if (!erBerørtPeriodeForAktivRevurdering(vedtaksperiodeId)) return

        run(queryOf(INSERT_VEDTAKSPERIODE_UTBETALING, mapOf(
            "vedtaksperiodeId" to vedtaksperiodeId,
            "utbetalingId" to utbetalingId
        )).asExecute)
    }

    private companion object {
        @Language("PostgreSQL")
        private const val FINN_BERØRT_PERIODE = """
             SELECT EXISTS (SELECT 1 FROM revurdering_vedtaksperiode WHERE vedtaksperiode_id=:vedtaksperiodeId AND status = 'IKKE_FERDIG'::revurderingstatus);
        """
        @Language("PostgreSQL")
        private const val INSERT_VEDTAKSPERIODE_UTBETALING = """
             INSERT INTO vedtaksperiode_utbetaling(vedtaksperiode_id, utbetaling_id)
             VALUES (:vedtaksperiodeId, :utbetalingId)
        """
    }
}
