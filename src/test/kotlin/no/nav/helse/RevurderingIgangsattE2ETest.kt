package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Revurderingstatus.*
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RevurderingIgangsattE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid().apply {
        RevurderingIgangsettelser(this, ::dataSource)
        VedtaksperiodeUtbetalinger(this, ::dataSource)
        Godkjenninger(this, ::dataSource)
        RevurderingFeilet(this, ::dataSource)
        VedtaksperiodeForkastet(this, ::dataSource)
    }

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @AfterEach
    fun reset() {
        dataSource.resetDatabase()
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

    @Test
    fun `lagrer vedtaksperiode utbetalinger`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val periode1 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId1,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 7),
            periodeTom = LocalDate.of(2022, 11, 29),
            orgnummer = "456"
        )
        val periode2 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId2,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 30),
            periodeTom = LocalDate.of(2022, 12, 15),
            orgnummer = "456"
        )

        val id = UUID.randomUUID()
        val utbetalingId1 = UUID.randomUUID()
        val kilde = UUID.randomUUID()
        val årsak = "KORRIGERT_SØKNAD"
        val opprettet = LocalDateTime.now()
        river.sendTestMessage(revurderingIgangsatt(id = id, kilde = kilde, årsak = årsak, opprettet = opprettet, berørtePerioder = listOf(periode1, periode2)))
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId1, utbetalingId1))
        river.sendTestMessage(godkjenningsbehov(utbetalingId1, godkjent = true, behandletMaskinelt = true))

        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId1))
        assertEquals(0, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId2))
        assertEquals(IKKE_FERDIG, statusForRevurderingIgangsatt(id))
        statusForBerørteVedtaksperioder(id).also { statuser ->
            assertEquals(FERDIGSTILT_AUTOMATISK, statuser[vedtaksperiodeId1])
            assertEquals(IKKE_FERDIG, statuser[vedtaksperiodeId2])
        }

        val utbetalingId2 = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId2, utbetalingId2))
        river.sendTestMessage(godkjenningsbehov(utbetalingId2, godkjent = true, behandletMaskinelt = false))

        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId1))
        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId2))
        assertEquals(FERDIGSTILT_MANUELT, statusForRevurderingIgangsatt(id))
        statusForBerørteVedtaksperioder(id).also { statuser ->
            assertEquals(FERDIGSTILT_AUTOMATISK, statuser[vedtaksperiodeId1])
            assertEquals(FERDIGSTILT_MANUELT, statuser[vedtaksperiodeId2])
        }

        river.inspektør.also { rapidInspector ->
            val ferdigstiltmelding = rapidInspector.message(rapidInspector.size - 1)
            assertEquals("revurdering_ferdigstilt", ferdigstiltmelding.path("@event_name").asText())
            assertEquals(id.toString(), ferdigstiltmelding.path("revurderingId").asText())
            assertEquals("FERDIGSTILT_MANUELT", ferdigstiltmelding.path("status").asText())
            assertEquals(årsak, ferdigstiltmelding.path("årsak").asText())
            assertEquals(kilde.toString(), ferdigstiltmelding.path("kilde").asText())
            assertEquals(opprettet.withNano(0), ferdigstiltmelding.path("revurderingIgangsatt").asLocalDateTime().withNano(0))
            val berørtPerioder = ferdigstiltmelding.path("berørtePerioder").associate {
                UUID.fromString(it.path("vedtaksperiodeId").asText()) to it.path("status").asText()
            }
            assertEquals(2, berørtPerioder.size)
            assertEquals("FERDIGSTILT_AUTOMATISK", berørtPerioder[vedtaksperiodeId1])
            assertEquals("FERDIGSTILT_MANUELT", berørtPerioder[vedtaksperiodeId2])
        }
    }

    @Test
    fun `erstatter perioder som er del av ny revurdering`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val periode1 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId1,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 7),
            periodeTom = LocalDate.of(2022, 11, 29),
            orgnummer = "456"
        )
        val periode2 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId2,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 30),
            periodeTom = LocalDate.of(2022, 12, 15),
            orgnummer = "456"
        )

        val revurderingId1 = UUID.randomUUID()
        val revurderingId2 = UUID.randomUUID()
        val utbetalingId = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(id = revurderingId1, berørtePerioder = listOf(periode1, periode2)))
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId1, utbetalingId))
        river.sendTestMessage(godkjenningsbehov(utbetalingId, godkjent = true, behandletMaskinelt = true))
        river.sendTestMessage(revurderingIgangsatt(id = revurderingId2, berørtePerioder = listOf(periode2)))

        assertEquals(FERDIGSTILT_AUTOMATISK, statusForRevurderingIgangsatt(revurderingId1))
        assertEquals(IKKE_FERDIG, statusForRevurderingIgangsatt(revurderingId2))
        statusForBerørteVedtaksperioder(revurderingId1).also { statuser ->
            assertEquals(FERDIGSTILT_AUTOMATISK, statuser[vedtaksperiodeId1])
            assertEquals(ERSTATTET, statuser[vedtaksperiodeId2])
        }
        statusForBerørteVedtaksperioder(revurderingId2).also { statuser ->
            assertEquals(IKKE_FERDIG, statuser[vedtaksperiodeId2])
        }
        river.inspektør.also { rapidInspector ->
            val ferdigstiltmelding = rapidInspector.message(rapidInspector.size - 1)
            assertEquals("revurdering_ferdigstilt", ferdigstiltmelding.path("@event_name").asText())
            assertEquals(revurderingId1.toString(), ferdigstiltmelding.path("revurderingId").asText())
            assertEquals("FERDIGSTILT_AUTOMATISK", ferdigstiltmelding.path("status").asText())
            val berørtPerioder = ferdigstiltmelding.path("berørtePerioder").associate {
                UUID.fromString(it.path("vedtaksperiodeId").asText()) to it.path("status").asText()
            }
            assertEquals(2, berørtPerioder.size)
            assertEquals("FERDIGSTILT_AUTOMATISK", berørtPerioder[vedtaksperiodeId1])
            assertEquals("ERSTATTET", berørtPerioder[vedtaksperiodeId2])
        }
    }

    @Test
    fun `erstatter perioder som er del av ny revurdering - og ferdigstiller revurderingen`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val periode1 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId1,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 7),
            periodeTom = LocalDate.of(2022, 11, 29),
            orgnummer = "456"
        )
        val periode2 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId2,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 30),
            periodeTom = LocalDate.of(2022, 12, 15),
            orgnummer = "456"
        )

        val revurderingId1 = UUID.randomUUID()
        val revurderingId2 = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(id = revurderingId1, berørtePerioder = listOf(periode1, periode2)))
        river.sendTestMessage(revurderingIgangsatt(id = revurderingId2, berørtePerioder = listOf(periode2)))

        assertEquals(IKKE_FERDIG, statusForRevurderingIgangsatt(revurderingId1))
        assertEquals(IKKE_FERDIG, statusForRevurderingIgangsatt(revurderingId2))
        statusForBerørteVedtaksperioder(revurderingId1).also { statuser ->
            assertEquals(IKKE_FERDIG, statuser[vedtaksperiodeId1])
            assertEquals(ERSTATTET, statuser[vedtaksperiodeId2])
        }
        statusForBerørteVedtaksperioder(revurderingId2).also { statuser ->
            assertEquals(IKKE_FERDIG, statuser[vedtaksperiodeId2])
        }
    }


    @Test
    fun `revurdering feilet gjør at perioden markeres som feilet`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val periode1 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId1,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 7),
            periodeTom = LocalDate.of(2022, 11, 29),
            orgnummer = "456"
        )
        val periode2 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId2,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 30),
            periodeTom = LocalDate.of(2022, 12, 15),
            orgnummer = "456"
        )

        val id = UUID.randomUUID()
        val utbetalingId1 = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(id = id, berørtePerioder = listOf(periode1, periode2)))
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId1, utbetalingId1))
        river.sendTestMessage(godkjenningsbehov(utbetalingId1, godkjent = true, behandletMaskinelt = true))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId2))

        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId1))
        assertEquals(0, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId2))
        assertEquals(FEILET, statusForRevurderingIgangsatt(id))
        statusForBerørteVedtaksperioder(id).also { statuser ->
            assertEquals(FERDIGSTILT_AUTOMATISK, statuser[vedtaksperiodeId1])
            assertEquals(FEILET, statuser[vedtaksperiodeId2])
        }

        river.inspektør.also { rapidInspector ->
            val ferdigstiltmelding = rapidInspector.message(rapidInspector.size - 1)
            assertEquals("revurdering_ferdigstilt", ferdigstiltmelding.path("@event_name").asText())
            assertEquals(id.toString(), ferdigstiltmelding.path("revurderingId").asText())
            assertEquals("FEILET", ferdigstiltmelding.path("status").asText())
            val berørtPerioder = ferdigstiltmelding.path("berørtePerioder").associate {
                UUID.fromString(it.path("vedtaksperiodeId").asText()) to it.path("status").asText()
            }
            assertEquals(2, berørtPerioder.size)
            assertEquals("FERDIGSTILT_AUTOMATISK", berørtPerioder[vedtaksperiodeId1])
            assertEquals("FEILET", berørtPerioder[vedtaksperiodeId2])
        }
    }

    @Test
    fun `revurdering forkastet gjør at perioden markeres som feilet`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        val periode1 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId1,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 7),
            periodeTom = LocalDate.of(2022, 11, 29),
            orgnummer = "456"
        )
        val periode2 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId2,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 30),
            periodeTom = LocalDate.of(2022, 12, 15),
            orgnummer = "456"
        )

        val id = UUID.randomUUID()
        val utbetalingId1 = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(id = id, berørtePerioder = listOf(periode1, periode2)))
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId1, utbetalingId1))
        river.sendTestMessage(godkjenningsbehov(utbetalingId1, godkjent = true, behandletMaskinelt = true))
        river.sendTestMessage(vedtaksperiodeForkastet(vedtaksperiodeId2))

        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId1))
        assertEquals(0, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId2))
        assertEquals(FEILET, statusForRevurderingIgangsatt(id))
        statusForBerørteVedtaksperioder(id).also { statuser ->
            assertEquals(FERDIGSTILT_AUTOMATISK, statuser[vedtaksperiodeId1])
            assertEquals(FEILET, statuser[vedtaksperiodeId2])
        }

        river.inspektør.also { rapidInspector ->
            val ferdigstiltmelding = rapidInspector.message(rapidInspector.size - 1)
            assertEquals("revurdering_ferdigstilt", ferdigstiltmelding.path("@event_name").asText())
            assertEquals(id.toString(), ferdigstiltmelding.path("revurderingId").asText())
            assertEquals("FEILET", ferdigstiltmelding.path("status").asText())
            val berørtPerioder = ferdigstiltmelding.path("berørtePerioder").associate {
                UUID.fromString(it.path("vedtaksperiodeId").asText()) to it.path("status").asText()
            }
            assertEquals(2, berørtPerioder.size)
            assertEquals("FERDIGSTILT_AUTOMATISK", berørtPerioder[vedtaksperiodeId1])
            assertEquals("FEILET", berørtPerioder[vedtaksperiodeId2])
        }
    }

    @Test
    fun `lagrer ikke vedtaksperiode utbetalinger for perioder som ikke er berørt`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId))
        assertEquals(0, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId))
    }

    @Test
    fun `lagrer ikke vedtaksperiode utbetalinger for perioder som er ferdig`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val periode1 = BerørtPeriode(
            vedtaksperiodeId = vedtaksperiodeId,
            skjæringstidspunkt = LocalDate.of(2022, 10, 3),
            periodeFom = LocalDate.of(2022, 11, 7),
            periodeTom = LocalDate.of(2022, 11, 29),
            orgnummer = "456"
        )

        val id = UUID.randomUUID()
        val utbetalingId = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(id = id, berørtePerioder = listOf(periode1)))
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId, utbetalingId))

        river.sendTestMessage(godkjenningsbehov(utbetalingId, godkjent = true, behandletMaskinelt = true))

        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeUtbetaling(vedtaksperiodeId, UUID.randomUUID()))
        assertEquals(1, tellVedtaksperiodeUtbetalinger(vedtaksperiodeId))
    }

    private fun tellRevurderingIgangsatt(id: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering WHERE id = '$id'"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun statusForRevurderingIgangsatt(id: UUID): Revurderingstatus {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT status FROM revurdering WHERE id = '$id'"
            valueOf(session.run(queryOf(query).map { row -> row.string(1) }.asList).single())
        }
    }

    private fun tellRevurderingIgangsattVedtaksperioder(id: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering_vedtaksperiode WHERE revurdering_igangsatt_id = '$id'"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun statusForBerørteVedtaksperioder(id: UUID): Map<UUID, Revurderingstatus> {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT vedtaksperiode_id, status FROM revurdering_vedtaksperiode WHERE revurdering_igangsatt_id = '$id'"
            session.run(queryOf(query).map { row ->
                UUID.fromString(row.string(1)) to valueOf(row.string(2))
            }.asList).toMap()
        }
    }

    private fun tellVedtaksperiodeUtbetalinger(vedtaksperiodeId: UUID): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM vedtaksperiode_utbetaling WHERE vedtaksperiode_id = '$vedtaksperiodeId'"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }


    @Language("JSON")
    private fun revurderingIgangsatt(
        kilde: UUID = UUID.randomUUID(),
        id: UUID = UUID.randomUUID(),
        årsak: String = "KORRIGERT_SØKNAD",
        opprettet: LocalDateTime = LocalDateTime.now(),
        berørtePerioder: List<BerørtPeriode> = listOf(
            BerørtPeriode(
                vedtaksperiodeId = UUID.randomUUID(),
                skjæringstidspunkt = LocalDate.of(2022, 10, 3),
                periodeFom = LocalDate.of(2022, 11, 7),
                periodeTom = LocalDate.of(2022, 11, 29),
                orgnummer = "456"
            ),
            BerørtPeriode(
                vedtaksperiodeId = UUID.randomUUID(),
                skjæringstidspunkt = LocalDate.of(2022, 10, 3),
                periodeFom = LocalDate.of(2022, 11, 30),
                periodeTom = LocalDate.of(2022, 12, 15),
                orgnummer = "456"
            )
        )
    ) = """{
        "@event_name":"overstyring_igangsatt",
        "revurderingId": "$id",
        "fødselsnummer":"fnr",
        "aktørId":"aktorId",
        "kilde":"$kilde",
        "skjæringstidspunkt":"2022-10-03",
        "periodeForEndringFom":"2022-11-07",
        "periodeForEndringTom":"2022-11-30",
        "årsak":"$årsak",
        "typeEndring": "REVURDERING",
        "berørtePerioder": ${berørtePerioder.map { it.toJsonString() }},
        "@id":"69cf0c28-16d9-464e-bc71-bd9eabea22a1",
        "@opprettet":"$opprettet"
    }
    """

    @Language("JSON")
    private fun vedtaksperiodeUtbetaling(
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        utbetalingId: UUID = UUID.randomUUID()
    ) = """{
        "@event_name":"vedtaksperiode_ny_utbetaling",
        "@id": "${UUID.randomUUID()}",
        "@opprettet": "${LocalDateTime.now()}",
        "fødselsnummer": "foo",
        "aktørId": "bar",
        "organisasjonsnummer": "baz",
        "vedtaksperiodeId": "$vedtaksperiodeId",
        "utbetalingId": "$utbetalingId"
    }
    """
    @Language("JSON")
    private fun godkjenningsbehov(
        utbetalingId: UUID = UUID.randomUUID(),
        godkjent: Boolean,
        behandletMaskinelt: Boolean
    ) = """{
        "@event_name":"behov",
        "@id": "${UUID.randomUUID()}",
        "@opprettet": "${LocalDateTime.now()}",
        "@behov": ["Godkjenning"],
        "@final": true,
        "@besvart": "${LocalDateTime.now()}",
        "utbetalingId": "$utbetalingId",
        "@løsning": {
          "Godkjenning": {
            "godkjent": $godkjent,
            "automatiskBehandling": $behandletMaskinelt
          }
        }
    }
    """
    @Language("JSON")
    private fun vedtaksperiodeEndret(
        vedtaksperiodeId: UUID,
        tilstand: String = "REVURDERING_FEILET"
    ) = """{
        "@event_name":"vedtaksperiode_endret",
        "@id": "${UUID.randomUUID()}",
        "@opprettet": "${LocalDateTime.now()}",
        "vedtaksperiodeId": "$vedtaksperiodeId",
        "gjeldendeTilstand": "$tilstand"
    }
    """
    @Language("JSON")
    private fun vedtaksperiodeForkastet(
        vedtaksperiodeId: UUID,
    ) = """{
        "@event_name":"vedtaksperiode_forkastet",
        "@id": "${UUID.randomUUID()}",
        "@opprettet": "${LocalDateTime.now()}",
        "vedtaksperiodeId": "$vedtaksperiodeId"
    }
    """


    private class BerørtPeriode(
        private val vedtaksperiodeId: UUID,
        private val skjæringstidspunkt: LocalDate,
        private val periodeFom: LocalDate,
        private val periodeTom: LocalDate,
        private val orgnummer: String,
        private val typeEndring: String = "REVURDERING"
    ) {
        @Language("JSON")
        fun toJsonString() = """
            {
                "vedtaksperiodeId":"$vedtaksperiodeId",
                "skjæringstidspunkt":"$skjæringstidspunkt",
                "periodeFom":"$periodeFom",
                "periodeTom":"$periodeTom",
                "orgnummer":"$orgnummer",
                "typeEndring": "$typeEndring"
            }
        """
    }
}