package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class RevurderingIgangsettelser(rapidApplication: RapidsConnection, private val dataSource: () -> DataSource): River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "revurdering_igangsatt") // TODO: dette skal etterhvert hete noe annet
                it.demandValue("typeEndring", "REVURDERING")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("skjæringstidspunkt", JsonNode::asLocalDate)
                it.require("periodeForEndringFom", JsonNode::asLocalDate)
                it.require("periodeForEndringTom", JsonNode::asLocalDate)
                it.require("id") { id -> UUID.fromString(id.asText()) }
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
        val id = packet["id"].let { UUID.fromString(it.asText()) }
        val årsak = packet["årsak"].asText()
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val skjæringstidspunkt = packet["skjæringstidspunkt"].asLocalDate()
        val fødselsnummer = packet["fødselsnummer"].asText()
        val aktørId = packet["aktørId"].asText()
        val kilde = packet["kilde"].let { UUID.fromString(it.asText()) }
        val periodeForEndringFom = packet["periodeForEndringFom"].asLocalDate()
        val periodeForEndringTom = packet["periodeForEndringTom"].asLocalDate()
        val berørtePerioder = packet["berørtePerioder"]

        sessionOf(dataSource()).use {
            it.transaction { session ->
                @Language("PostgreSQL")
                val statement = """
                     INSERT INTO revurdering(id, opprettet, kilde, fodselsnummer, aktor_id, skjaeringstidspunkt, periode_for_endring_fom, periode_for_endring_tom, aarsak)
                     VALUES (:id, :opprettet, :kilde, :fodselsnummer, :aktor_id, :skjaeringstidspunkt, :periode_for_endring_fom, :periode_for_endring_tom, :aarsak)
                      """
                session.run(
                    queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "id" to id,
                            "opprettet" to opprettet,
                            "kilde" to kilde,
                            "fodselsnummer" to fødselsnummer,
                            "aktor_id" to aktørId,
                            "skjaeringstidspunkt" to skjæringstidspunkt,
                            "periode_for_endring_fom" to periodeForEndringFom,
                            "periode_for_endring_tom" to periodeForEndringTom,
                            "aarsak" to årsak
                        )
                    ).asExecute
                )

                @Language("PostgreSQL")
                val statement2 = """
                    INSERT INTO revurdering_vedtaksperiode(vedtaksperiode_id, revurdering_igangsatt_id, orgnummer, periode_fom, periode_tom, skjaeringstidspunkt)
                    VALUES ${berørtePerioder.joinToString { "(?, ?, ?, ?, ?, ?)" }}
                """

                session.run(
                    queryOf(
                        statement = statement2,
                        *berørtePerioder.flatMap { periode ->
                            listOf(
                                periode.path("vedtaksperiodeId").let { UUID.fromString(it.asText()) },
                                id,
                                periode.path("orgnummer").asText(),
                                periode.path("periodeFom").asLocalDate(),
                                periode.path("periodeTom").asLocalDate(),
                                periode.path("skjæringstidspunkt").asLocalDate()
                            )
                        }.toTypedArray()
                    ).asExecute
                )
            }
        }

    }
}
