package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.TransactionalSession
import kotliquery.queryOf
import no.nav.helse.Revurderingsperiode.Companion.somRevurderinger
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class RevurderingIgangsettelser(rapidApplication: RapidsConnection, private val dataSource: () -> DataSource): River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandAny("@event_name", listOf("revurdering_igangsatt", "overstyring_igangsatt"))
                it.demandValue("typeEndring", "REVURDERING")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("skjæringstidspunkt", JsonNode::asLocalDate)
                it.require("periodeForEndringFom", JsonNode::asLocalDate)
                it.require("periodeForEndringTom", JsonNode::asLocalDate)
                it.require("revurderingId") { id -> UUID.fromString(id.asText()) }
                it.require("kilde") { kilde -> UUID.fromString(kilde.asText()) }
                it.requireKey("fødselsnummer", "aktørId", "årsak")
                it.requireArray("berørtePerioder") {
                    require("vedtaksperiodeId") { vedtaksperiodeId -> UUID.fromString(vedtaksperiodeId.asText()) }
                    require("skjæringstidspunkt", JsonNode::asLocalDate)
                    require("periodeFom", JsonNode::asLocalDate)
                    require("periodeTom", JsonNode::asLocalDate)
                    requireKey("orgnummer")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val id = packet["revurderingId"].let { UUID.fromString(it.asText()) }
        val årsak = packet["årsak"].asText()
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val skjæringstidspunkt = packet["skjæringstidspunkt"].asLocalDate()
        val fødselsnummer = packet["fødselsnummer"].asText()
        val aktørId = packet["aktørId"].asText()
        val kilde = packet["kilde"].let { UUID.fromString(it.asText()) }
        val periodeForEndringFom = packet["periodeForEndringFom"].asLocalDate()
        val periodeForEndringTom = packet["periodeForEndringTom"].asLocalDate()
        val berørtePerioder = packet["berørtePerioder"]

        dataSource().transactional {
            erstatt(context, berørtePerioder.map { UUID.fromString(it["vedtaksperiodeId"].asText()) })
            opprettRevurdering(id, opprettet, kilde, fødselsnummer, aktørId, skjæringstidspunkt, periodeForEndringFom, periodeForEndringTom, årsak)
            opprettVedtaksperioder(id, berørtePerioder)
        }
    }

    private fun TransactionalSession.erstatt(context: MessageContext, berørtePerioder: List<UUID>) {
        val uferdigeRevurderinger = Revurderingsperiode.alleUferdigeRevurderinger(berørtePerioder, this)
            .map { it.erstattet(berørtePerioder) }
            .somRevurderinger()

        uferdigeRevurderinger.forEach {
            it.lagreStatus(this, context)
        }
    }

    private fun TransactionalSession.opprettRevurdering(
        id: UUID,
        opprettet: LocalDateTime,
        kilde: UUID,
        fødselsnummer: String,
        aktørId: String,
        skjæringstidspunkt: LocalDate,
        periodeForEndringFom: LocalDate,
        periodeForEndringTom: LocalDate,
        årsak: String
    ) {
        run(queryOf(INSERT_REVURDERING, mapOf(
            "id" to id,
            "opprettet" to opprettet,
            "kilde" to kilde,
            "fodselsnummer" to fødselsnummer,
            "aktor_id" to aktørId,
            "skjaeringstidspunkt" to skjæringstidspunkt,
            "periode_for_endring_fom" to periodeForEndringFom,
            "periode_for_endring_tom" to periodeForEndringTom,
            "aarsak" to årsak
        )).asExecute)
    }

    private fun TransactionalSession.opprettVedtaksperioder(id: UUID, berørtePerioder: JsonNode) {
        run(queryOf(INSERT_VEDTAKSPERIODE.format(berørtePerioder.joinToString { "(?, ?, ?, ?, ?, ?)" }), *berørtePerioder.flatMap { periode ->
            listOf(
                periode.path("vedtaksperiodeId").let { UUID.fromString(it.asText()) },
                id,
                periode.path("orgnummer").asText(),
                periode.path("periodeFom").asLocalDate(),
                periode.path("periodeTom").asLocalDate(),
                periode.path("skjæringstidspunkt").asLocalDate()
            )
        }.toTypedArray()).asExecute)
    }

    private companion object {
        @Language("PostgreSQL")
        private const val INSERT_REVURDERING = """
             INSERT INTO revurdering(id, opprettet, kilde, fodselsnummer, aktor_id, skjaeringstidspunkt, periode_for_endring_fom, periode_for_endring_tom, aarsak)
             VALUES (:id, :opprettet, :kilde, :fodselsnummer, :aktor_id, :skjaeringstidspunkt, :periode_for_endring_fom, :periode_for_endring_tom, :aarsak)
              """


        @Language("PostgreSQL")
        private const val INSERT_VEDTAKSPERIODE = """
                    INSERT INTO revurdering_vedtaksperiode(vedtaksperiode_id, revurdering_igangsatt_id, orgnummer, periode_fom, periode_tom, skjaeringstidspunkt)
                    VALUES %s
                """
    }
}
