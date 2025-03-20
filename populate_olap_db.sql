-- Populate produto
INSERT INTO c##dw_olap.produto (
    modelo,
    marca
)
    SELECT
        mo.descricao,
        ma.descricao
    FROM
             c##dw_oltp.modelo mo
        JOIN c##dw_oltp.marca ma USING ( id_marca );

-- Populate cliente
INSERT INTO c##dw_olap.cliente ( estado_civil )
    SELECT DISTINCT
        oltp_cl.estado_civil
    FROM
        c##dw_oltp.cliente oltp_cl;

-- Populate local
INSERT INTO c##dw_olap.local (
    estado,
    cidade
)
    SELECT
        es.descricao,
        ci.descricao
    FROM
             c##dw_oltp.cidade ci
        JOIN c##dw_oltp.estado es USING ( id_estado );

-- populate tempo
INSERT INTO c##dw_olap.tempo (
    ano,
    quadrimestre
)
    SELECT DISTINCT
        EXTRACT(YEAR FROM data)      AS ano,
        c##dw_olap.get_quarter(data) AS quadrimestre
    FROM
        c##dw_oltp.compra;

-- populate venda
INSERT INTO c##dw_olap.venda (
    id_produto,
    id_tempo,
    id_local,
    id_cliente,
    quantidade,
    valor
)
    SELECT
        olap_pr.id_produto,
        olap_te.id_tempo,
        olap_lo.id_local,
        olap_cl.id_cliente,
        SUM(oltp_ca.quantidade),
        SUM(oltp_ca.valor)
    FROM
             c##dw_oltp.carrinho oltp_ca
        JOIN c##dw_oltp.compra   oltp_co ON ( oltp_ca.id_compra = oltp_co.id_compra )
        JOIN c##dw_olap.tempo    olap_te ON ( EXTRACT(YEAR FROM oltp_co.data) = olap_te.ano
                                           AND c##dw_olap.get_quarter(oltp_co.data) = olap_te.quadrimestre )
        JOIN c##dw_oltp.cliente  oltp_cl ON ( oltp_co.id_cliente = oltp_cl.id_cliente )
        JOIN c##dw_olap.cliente  olap_cl ON ( oltp_cl.estado_civil = olap_cl.estado_civil )
        JOIN c##dw_oltp.cidade   oltp_ci ON ( oltp_cl.id_cidade = oltp_ci.id_cidade )
        JOIN c##dw_oltp.estado   oltp_es ON ( oltp_ci.id_estado = oltp_es.id_estado )
        JOIN c##dw_olap.local    olap_lo ON ( olap_lo.cidade = oltp_ci.descricao
                                           AND olap_lo.estado = oltp_es.descricao )
        JOIN c##dw_oltp.aparelho oltp_ap ON ( oltp_ca.id_aparelho = oltp_ap.id_aparelho )
        JOIN c##dw_oltp.modelo   oltp_mo ON ( oltp_ap.id_modelo = oltp_mo.id_modelo )
        JOIN c##dw_oltp.marca    oltp_ma ON ( oltp_mo.id_marca = oltp_ma.id_marca )
        JOIN c##dw_olap.produto  olap_pr ON ( olap_pr.modelo = oltp_mo.descricao
                                             AND olap_pr.marca = oltp_ma.descricao )
    GROUP BY
        olap_pr.id_produto,
        olap_te.id_tempo,
        olap_lo.id_local,
        olap_cl.id_cliente;

-- COMMIT;