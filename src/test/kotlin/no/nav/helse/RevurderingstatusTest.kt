package no.nav.helse

import no.nav.helse.Revurderingstatus.*
import no.nav.helse.Revurderingstatus.Companion.aggregertStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class RevurderingstatusTest {

    @Test
    fun `ikke ferdig`() {
        assertEquals(IKKE_FERDIG, listOf(FERDIGSTILT_AUTOMATISK, FEILET, AVVIST_AUTOMATISK, ERSTATTET, IKKE_FERDIG).aggregertStatus()) {
            "så lenge minst én er ikke ferdig, skal aggregert status være ikke ferdig"
        }
    }

    @Test
    fun feilet() {
        assertEquals(FEILET, listOf(FERDIGSTILT_AUTOMATISK, AVVIST_AUTOMATISK, FEILET, ERSTATTET).aggregertStatus()) {
            "så lenge revurderingen er ferdig, og minst én er feilet, skal aggregert status være feilet"
        }
    }

    @Test
    fun avvist() {
        assertEquals(AVVIST_AUTOMATISK, listOf(FERDIGSTILT_AUTOMATISK, AVVIST_AUTOMATISK, FERDIGSTILT_MANUELT, ERSTATTET).aggregertStatus()) {
            "så lenge revurderingen er ferdig og ikke feilet, og minst én er avvist, skal aggregert status være avvist automatisk dersom alle avvisningene er automatisk"
        }
        assertEquals(AVVIST_MANUELT, listOf(FERDIGSTILT_AUTOMATISK, AVVIST_AUTOMATISK, AVVIST_MANUELT, ERSTATTET).aggregertStatus()) {
            "så lenge revurderingen er ferdig og ikke feilet, og minst én er avvist, skal aggregert status være avvist manuelt dersom én av avvisningene er manuelle"
        }
    }

    @Test
    fun ferdigstilt() {
        assertEquals(FERDIGSTILT_AUTOMATISK, listOf(FERDIGSTILT_AUTOMATISK, FERDIGSTILT_AUTOMATISK, FERDIGSTILT_AUTOMATISK, ERSTATTET).aggregertStatus()) {
            "så lenge revurderingen er ferdig og ikke feilet, og alle er ferdigstilt automatisk, skal aggregert status være ferdigstilt automatisk"
        }
        assertEquals(FERDIGSTILT_MANUELT, listOf(FERDIGSTILT_AUTOMATISK, FERDIGSTILT_AUTOMATISK, FERDIGSTILT_MANUELT, ERSTATTET).aggregertStatus()) {
            "så lenge revurderingen er ferdig og ikke feilet, og minst én er ferdigstilt manuelt, skal aggregert status være ferdigstilt manuelt"
        }
    }

    @Test
    fun erstattet() {
        assertEquals(IKKE_FERDIG, listOf(IKKE_FERDIG, ERSTATTET, ERSTATTET).aggregertStatus()) {
            "erstattet påvirker ikke status dersom revurderingen er uferdig"
        }
        assertEquals(FERDIGSTILT_AUTOMATISK, listOf(FERDIGSTILT_AUTOMATISK, ERSTATTET).aggregertStatus()) {
            "erstattet påvirker ikke status dersom revurderingen er ferdig"
        }
        assertEquals(FEILET, listOf(FEILET, ERSTATTET).aggregertStatus()) {
            "erstattet påvirker ikke status dersom revurderingen er ferdig"
        }
        assertEquals(AVVIST_AUTOMATISK, listOf(AVVIST_AUTOMATISK, FERDIGSTILT_AUTOMATISK, ERSTATTET).aggregertStatus()) {
            "erstattet påvirker ikke status dersom revurderingen er ferdig"
        }
        assertEquals(ERSTATTET, listOf(ERSTATTET, ERSTATTET).aggregertStatus()) {
            "revurderingen er erstattet dersom revurderingen er ferdig, og alle er erstattet"
        }
    }
}