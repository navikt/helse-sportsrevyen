package no.nav.helse

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers

val databaseContainer = DatabaseContainers.container("sportsrevyen", CleanupStrategy.tables("revurdering,revurdering_vedtaksperiode,vedtaksperiode_utbetaling"))
