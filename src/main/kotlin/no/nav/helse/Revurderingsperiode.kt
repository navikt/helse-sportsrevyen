package no.nav.helse

import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.helse.Revurderingsperiode.Companion.aggregertStatus
import no.nav.helse.Revurderingstatus.Companion.aggregertStatus
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*

data class UferdigRevurdering(private val revurdering: UUID, private val perioder: List<Revurderingsperiode>) {
    fun lagreStatus(session: TransactionalSession, context: MessageContext) {
        perioder.forEach { it.lagreStatus(session) }

        val aggregertStatus = perioder.aggregertStatus()
        session.run(
            queryOf(
                settStatusPåRevurdering, mapOf(
                    "status" to aggregertStatus.name,
                    "id" to this.revurdering
                )
            ).asUpdate
        )

        if (aggregertStatus == Revurderingstatus.IKKE_FERDIG) return

        context.publish(JsonMessage.newMessage("revurdering_ferdigstilt", mapOf(
            "revurderingId" to this.revurdering,
            "status" to aggregertStatus,
            "berørtePerioder" to this.perioder.map { it.toJsonMap() }
        )).toJson().also {
            sikkerlogg.info("publiserer revurdering_ferdigstilt:\n\t$it")
        })
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")

        @Language("PostgreSQL")
        private const val settStatusPåRevurdering = """
            update revurdering set status = CAST(:status as revurderingstatus), oppdatert = now() where id = :id
        """
    }
}

data class Revurderingsperiode(
    private val vedtaksperiode: UUID,
    private val revurdering: UUID,
    private val status: Revurderingstatus
) {

    fun toJsonMap() = mapOf(
        "vedtaksperiodeId" to vedtaksperiode,
        "status" to status
    )

    fun lagreStatus(session: TransactionalSession) {
        session.run(
            queryOf(
                settStatusPåVedtaksperiode, mapOf(
                    "status" to status.name,
                    "vedtaksperiode_id" to vedtaksperiode,
                    "revurdering_id" to revurdering
                )
            ).asUpdate
        )
    }

    fun erstattet(berørtePerioder: List<UUID>): Revurderingsperiode {
        if (this.vedtaksperiode !in berørtePerioder) return this
        return this.copy(status = Revurderingstatus.ERSTATTET)
    }

    fun godkjenning(berørtePerioder: List<UUID>, utbetalingGodkjent: Boolean, utførtMaskinelt: Boolean): Revurderingsperiode {
        if (this.vedtaksperiode !in berørtePerioder) return this
        return this.copy(status = vedtaksperiodeStatus(utbetalingGodkjent, utførtMaskinelt))
    }

    fun feilet(vedtaksperiodeId: UUID): Revurderingsperiode {
        if (this.vedtaksperiode != vedtaksperiodeId) return this
        return this.copy(status = Revurderingstatus.FEILET)
    }

    private fun vedtaksperiodeStatus(utbetalingGodkjent: Boolean, utførtMaskinelt: Boolean) =
        when (utbetalingGodkjent) {
            true -> when (utførtMaskinelt) {
                true -> Revurderingstatus.FERDIGSTILT_AUTOMATISK
                false -> Revurderingstatus.FERDIGSTILT_MANUELT
            }
            false -> when (utførtMaskinelt) {
                true -> Revurderingstatus.AVVIST_AUTOMATISK
                false -> Revurderingstatus.AVVIST_MANUELT
            }
        }

    internal companion object {
        fun List<Revurderingsperiode>.aggregertStatus() = map { it.status }.aggregertStatus()

        fun alleUferdigeRevurderinger(berørtePerioder: List<UUID>, session: TransactionalSession): List<Revurderingsperiode> {
            return session.run(
                queryOf(perioderIEnUferdigRevurdering.format(berørtePerioder.joinToString { "?" }), *berørtePerioder.toTypedArray()).map {
                    Revurderingsperiode(
                        vedtaksperiode = it.uuid("vedtaksperiode_id"),
                        revurdering = it.uuid("revurdering_igangsatt_id"),
                        status = Revurderingstatus.valueOf(it.string("status"))
                    )
                }.asList)
        }

        fun hentUferdigePerioder(session: TransactionalSession, utbetalingId: UUID) = session.run(
            queryOf(hentUferdigePerioder, mapOf("utbetaling_id" to utbetalingId))
                .map { row ->
                    row.uuid("vedtaksperiode_id")
                }.asList
        )

        fun List<Revurderingsperiode>.somRevurderinger() =
            this.groupBy { it.revurdering }.map { (key, value) ->
                UferdigRevurdering(key, value)
            }

        @Language("PostgreSQL")
        private const val settStatusPåVedtaksperiode = """
            update revurdering_vedtaksperiode set status = CAST(:status as revurderingstatus), oppdatert = now() 
            where vedtaksperiode_id = :vedtaksperiode_id and revurdering_igangsatt_id = :revurdering_id 
            
        """

        @Language("PostgreSQL")
        private const val perioderIEnUferdigRevurdering = """
            select andre.vedtaksperiode_id, andre.revurdering_igangsatt_id, andre.status
            from revurdering_vedtaksperiode uferdig
            join revurdering_vedtaksperiode andre on uferdig.revurdering_igangsatt_id = andre.revurdering_igangsatt_id
            where uferdig.status = 'IKKE_FERDIG' and uferdig.vedtaksperiode_id in (%s)
            group by andre.vedtaksperiode_id, andre.revurdering_igangsatt_id
        """

        @Language("PostgreSQL")
        private const val hentUferdigePerioder = """
            select u.vedtaksperiode_id, v.revurdering_igangsatt_id from vedtaksperiode_utbetaling u
            join revurdering_vedtaksperiode v on u.vedtaksperiode_id = v.vedtaksperiode_id
            where u.utbetaling_id = :utbetaling_id
            and v.status = 'IKKE_FERDIG'
        """
    }
}