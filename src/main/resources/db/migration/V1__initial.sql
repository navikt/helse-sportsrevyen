CREATE TABLE revurdering_igangsatt
(
    id          UUID PRIMARY KEY,
    opprettet   TIMESTAMP NOT NULL,
    ferdigstilt TIMESTAMP,
    utfall      VARCHAR
);

CREATE TABLE revurdering_igangsatt_vedtaksperiode
(
    vedtaksperiode_id        UUID NOT NULL,
    revurdering_igangsatt_id UUID REFERENCES revurdering_igangsatt (id),
    tilstand                 VARCHAR,
    oppdatert                TIMESTAMP,
    CONSTRAINT pk PRIMARY KEY (vedtaksperiode_id, revurdering_igangsatt_id)
)
