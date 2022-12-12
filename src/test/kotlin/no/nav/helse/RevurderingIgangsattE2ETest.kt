package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RevurderingIgangsattE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid().apply { RevurderingIgangsettelser(this, dataSource = dataSource) }

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @Test
    fun `lagrer i databasen`() {
        val id = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(id = id))
        val revurderingIgangsatt = tellRevurderingIgangsatt(id)
        assertEquals(1, revurderingIgangsatt)

        val revurderingIgangsattVedtaksperioder = tellRevurderingIgangsattVedtaksperioder(id)
        assertEquals(2, revurderingIgangsattVedtaksperioder)
    }


    private fun tellRevurderingIgangsatt(id: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering_igangsatt WHERE id = '$id'"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun tellRevurderingIgangsattVedtaksperioder(id: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering_igangsatt_vedtaksperiode WHERE revurdering_igangsatt_id = '$id'"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }


    @Language("JSON")
    fun revurderingIgangsatt(
        kilde: UUID = UUID.randomUUID(),
        id: UUID = UUID.randomUUID(),
        årsak: String = "KORRIGERT_SØKNAD"
    ) = """{
        "@event_name":"revurdering_igangsatt",
        "id": "$id",
        "fødselsnummer":"fnr",
        "aktørId":"aktorId",
        "kilde":"$kilde",
        "skjæringstidspunkt":"2022-10-03",
        "periodeForEndringFom":"2022-11-07",
        "periodeForEndringTom":"2022-11-30",
        "årsak":"$årsak",
        "typeEndring": "REVURDERING",
        "berørtePerioder":[
            {
                "vedtaksperiodeId":"c0f78b58-4687-4191-adf8-6588c5982abb",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-07",
                "periodeTom":"2022-11-29",
                "orgnummer":"456",
                "typeEndring": "REVURDERING"
            },            
            {
                "vedtaksperiodeId":"c0c78b58-4687-4191-adf8-6588c5982abb",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-30",
                "periodeTom":"2022-12-15",
                "orgnummer":"456",
                "typeEndring": "REVURDERING"
            }
          ],
        "@id":"69cf0c28-16d9-464e-bc71-bd9eabea22a1",
        "@opprettet":"2022-12-06T15:44:57.01089295"
    }
    """

}