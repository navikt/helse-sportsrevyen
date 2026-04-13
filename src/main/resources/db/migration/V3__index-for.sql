create index on revurdering_vedtaksperiode (vedtaksperiode_id, revurdering_igangsatt_id)
    where status = 'IKKE_FERDIG'
