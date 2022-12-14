package no.nav.helse

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RevurderingerTest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)

    @AfterAll
    fun tearDown() {
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @AfterEach
    fun reset() {
        dataSource.resetDatabase()
    }

    @Test
    fun testIt() {
        dataSource.transactional {
            UferdigRevurdering.alleUferdigeRevurderinger(listOf(UUID.randomUUID(), UUID.randomUUID()), this)
        }
    }
}