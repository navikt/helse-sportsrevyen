package no.nav.helse

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.testcontainers.containers.PostgreSQLContainer
import java.util.*
import javax.sql.DataSource

fun embeddedPostgres() = PostgreSQLContainer<Nothing>("postgres:14").also { it.start() }


internal fun setupDataSourceMedFlyway(postgres: PostgreSQLContainer<Nothing>): DataSource {
    val hikariConfig = HikariConfig().apply {
        jdbcUrl = postgres.jdbcUrl
        username = postgres.username
        password = postgres.password
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
        initializationFailTimeout = 10000
    }

    val dataSource = HikariDataSource(hikariConfig)

    Flyway.configure()
        .dataSource(dataSource)
        .placeholders(mapOf("spesialist_oid" to UUID.randomUUID().toString()))
        .load()
        .migrate()

    dataSource.createTruncateFunction()
    return dataSource
}

private fun DataSource.createTruncateFunction() {
    sessionOf(this).use {
        @Language("PostgreSQL")
        val query = """
            CREATE OR REPLACE FUNCTION truncate_tables() RETURNS void AS $$
            DECLARE
            truncate_statement text;
            BEGIN
                SELECT 'TRUNCATE ' || string_agg(format('%I.%I', schemaname, tablename), ',') || ' CASCADE'
                    INTO truncate_statement
                FROM pg_tables
                WHERE schemaname='public'
                AND tablename not in ('flyway_schema_history');
                EXECUTE truncate_statement;
            END;
            $$ LANGUAGE plpgsql;
        """
        it.run(queryOf(query).asExecute)
    }

}

fun DataSource.resetDatabase() {
    sessionOf(this).use {
        it.run(queryOf("SELECT truncate_tables()").asExecute)
    }
}
