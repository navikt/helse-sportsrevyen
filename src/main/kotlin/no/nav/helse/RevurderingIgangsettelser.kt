package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class RevurderingIgangsettelser(rapidApplication: RapidsConnection, private val dataSource: DataSource): River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "revurdering_igangsatt")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("id") { id -> UUID.fromString(id.asText()) }
                it.requireValue("typeEndring", "REVURDERING")
                it.requireArray("berørtePerioder") {
                    require("vedtaksperiodeId") { vedtaksperiodeId -> UUID.fromString(vedtaksperiodeId.asText()) }
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val id = packet["id"].let { UUID.fromString(it.asText()) }
        val berørtePerioder = packet["berørtePerioder"]

        sessionOf(dataSource).use {
            it.transaction { session ->
                @Language("PostgreSQL")
                val statement = """
                     INSERT INTO revurdering_igangsatt(id, opprettet)
                     VALUES (:id, :opprettet)
                      """
                session.run(
                    queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "id" to id,
                            "opprettet" to opprettet
                        )
                    ).asExecute
                )

                @Language("PostgreSQL")
                val statement2 = """
                    INSERT INTO revurdering_igangsatt_vedtaksperiode(vedtaksperiode_id, revurdering_igangsatt_id)
                    VALUES ${berørtePerioder.joinToString { "(?, ?)" }}
                """

                session.run(
                    queryOf(
                        statement = statement2,
                        *berørtePerioder.flatMap { periode ->
                            listOf(
                                periode.path("vedtaksperiodeId").let { UUID.fromString(it.asText()) },
                                id
                            )
                        }.toTypedArray()
                    ).asExecute
                )
            }
        }

    }
}
