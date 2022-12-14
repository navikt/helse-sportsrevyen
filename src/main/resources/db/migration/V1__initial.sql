CREATE TYPE revurderingstatus AS ENUM ('IKKE_FERDIG', 'FERDIGSTILT_AUTOMATISK', 'FERDIGSTILT_MANUELT', 'AVVIST_AUTOMATISK', 'AVVIST_MANUELT', 'FEILET', 'ERSTATTET');

CREATE TABLE revurdering
(
    id                      UUID PRIMARY KEY,
    opprettet               TIMESTAMP         NOT NULL,
    kilde                   UUID              NOT NULL,
    fodselsnummer           VARCHAR           NOT NULL,
    aktor_id                VARCHAR           NOT NULL,
    skjaeringstidspunkt     DATE              NOT NULL,
    periode_for_endring_fom DATE              NOT NULL,
    periode_for_endring_tom DATE              NOT NULL,
    aarsak                  VARCHAR           NOT NULL,
    status                  revurderingstatus NOT NULL DEFAULT 'IKKE_FERDIG'::revurderingstatus,
    oppdatert               TIMESTAMP
);

CREATE INDEX revurdering_fnr ON revurdering(fodselsnummer);

CREATE TABLE revurdering_vedtaksperiode
(
    vedtaksperiode_id        UUID    NOT NULL,
    revurdering_igangsatt_id UUID REFERENCES revurdering (id),
    orgnummer                VARCHAR NOT NULL,
    periode_fom              DATE    NOT NULL,
    periode_tom              DATE    NOT NULL,
    skjaeringstidspunkt      DATE    NOT NULL,
    oppdatert                TIMESTAMP,
    status                   revurderingstatus NOT NULL DEFAULT 'IKKE_FERDIG'::revurderingstatus,
    CONSTRAINT pk_revurdering_vedtaksperiode PRIMARY KEY (vedtaksperiode_id, revurdering_igangsatt_id)
);

CREATE TABLE vedtaksperiode_utbetaling
(
    vedtaksperiode_id UUID NOT NULL,
    utbetaling_id     UUID NOT NULL,
    CONSTRAINT pk_vedtaksperiode_utbetaling PRIMARY KEY (utbetaling_id, vedtaksperiode_id)
);
