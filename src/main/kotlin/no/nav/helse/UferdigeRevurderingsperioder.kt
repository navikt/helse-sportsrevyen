package no.nav.helse

import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.helse.Revurderingstatus.Companion.aggregertStatus
import org.intellij.lang.annotations.Language
import java.util.*

data class UferdigeRevurderingsperioder(
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

    internal companion object {
        fun List<UferdigeRevurderingsperioder>.alleBerørtePerioder(session: TransactionalSession): List<UferdigeRevurderingsperioder> {
            val revurderingId = revurderingId()
            return session.run(queryOf(finnStatusPåRevurdering, mapOf("revurdering" to revurderingId)).map { row ->
                UferdigeRevurderingsperioder(
                    row.uuid("vedtaksperiode_id"),
                    revurderingId,
                    Revurderingstatus.valueOf(row.string("status"))
                )
            }.asList)
        }

        fun List<UferdigeRevurderingsperioder>.revurderingId(): UUID {
            val unikeRevurderinger = map { it.revurdering }.toSet()
            check(unikeRevurderinger.size == 1) {
                "Klarer ikke å tyde hvilken revurderingId som skal gjelde. Forventet bare én: $unikeRevurderinger"
            }
            return first().revurdering
        }

        fun List<UferdigeRevurderingsperioder>.lagreStatus(session: TransactionalSession): Revurderingstatus {
            val revurderingId = revurderingId()
            val aggregertStatus = map { it.status }.aggregertStatus()
            session.run(
                queryOf(
                    settStatusPåRevurdering, mapOf(
                        "status" to aggregertStatus.name,
                        "id" to revurderingId
                    )
                ).asUpdate
            )
            return aggregertStatus
        }

        @Language("PostgreSQL")
        private const val settStatusPåVedtaksperiode = """
            update revurdering_vedtaksperiode set status = CAST(:status as revurderingstatus), oppdatert = now() 
            where vedtaksperiode_id = :vedtaksperiode_id and revurdering_igangsatt_id = :revurdering_id 
            
        """

        @Language("PostgreSQL")
        private const val settStatusPåRevurdering = """
            update revurdering set status = CAST(:status as revurderingstatus), oppdatert = now() where id = :id
        """

        @Language("PostgreSQL")
        private const val finnStatusPåRevurdering = """
            select vedtaksperiode_id, status from revurdering_vedtaksperiode where revurdering_igangsatt_id = :revurdering
        """
    }
}