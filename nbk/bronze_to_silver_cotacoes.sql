
WITH
  desired_subset AS 
  (
    SELECT
      row_number() OVER (ORDER BY c.data_pesquisa) AS rn,
      'COTACOES' AS nm_origem,
      data_pesquisa AS dt_referencia,
      c.*
    FROM 
      precos_pmc.bronze.cotacoes c
    WHERE
      data_pesquisa != '2023-07-03' -- Problem with encoding before 2023-07-01 and at 2023-07-01 is ambiguous 
      AND data_pesquisa IN ({list_of_quoted_date_references})
  ), 

  encoding_fixes AS 
  (
    SELECT
      ds.rn,
      UPPER((
      CASE 
        WHEN ds.data_pesquisa < '2023-07-03' 
          THEN decode(encode(ds.rede, 'ISO-8859-1'), 'UTF-8') 
          ELSE ds.rede 
      END
      )) AS rede,
      REPLACE(UPPER(((
        CASE 
          WHEN ds.data_pesquisa < '2023-07-03' 
            THEN decode(encode(ds.endereco_completo, 'ISO-8859-1'), 'UTF-8') 
            ELSE ds.endereco_completo 
        END
      ))), ' - CURITIBA/PR', '') AS endereco_completo,
      UPPER(((
        CASE 
          WHEN ds.data_pesquisa < '2023-07-03' 
            THEN decode(encode(ds.descricao, 'ISO-8859-1'), 'UTF-8') 
            ELSE ds.descricao 
        END
      ))) AS descricao
    FROM desired_subset ds
  ),

  accentuation_removal AS 
  (

    SELECT
      ef.rn,
      translate(
        ef.rede,  
        'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 
        'AAAAAEEEEIIIIOOOOOUUUUC'
      )
      AS rede,
      translate(
        ef.endereco_completo, 
        'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 
        'AAAAAEEEEIIIIOOOOOUUUUC'
      )
      AS endereco_completo,
      translate(
        ef.descricao, 
        'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 
        'AAAAAEEEEIIIIOOOOOUUUUC'
      ) AS descricao
    FROM encoding_fixes ef
  ),

  text_replacing AS
  (
    SELECT
      ar.rn,
      TRIM(
        REPLACE(REPLACE(REPLACE(REPLACE(
          COALESCE(GET(split(ar.endereco_completo, ' \\- '), 0), 'NÃO INFORMADO') 
        ,'RUA', 'R.'), 'AVENIDA', 'AV.'), 'RODOVIA', ''), ', ,', ', ')
      ) AS endereco,
      COALESCE(GET(split(ar.endereco_completo, ' \\- '), 1), 'NÃO INFORMADO') as bairro,
      COALESCE(GET(split(ar.descricao, ' \\-'), 0), 'NÃO INFORMADO') as produto,
      COALESCE(
        (
        CASE 
          WHEN length(ar.descricao) - length(regexp_replace(ar.descricao, ' \\-', '')) > 2 
            THEN GET(split(ar.descricao, ' \\-'), 1) 
        END
        )
      , 'NÃO INFORMADO') as marca,
      COALESCE(ELEMENT_AT(split(ar.descricao, ' \\-'), -1), 'NÃO INFORMADO') as tipo
    FROM accentuation_removal ar
  ),

  address_split AS
  (
    SELECT
      tr.rn,
      TRIM(regexp_replace(ELEMENT_AT(split(regexp_replace(tr.endereco, '[^0-9\\s]', ''), ' '), -1),'\\s+', ' ')) AS numero,
      TRIM(regexp_replace(REPLACE(REPLACE(
        endereco,
        ELEMENT_AT(split(regexp_replace(tr.endereco, '[^0-9\\s]', ''), ' '), -1),
        ''), ',', ''), '\\s+', ' ')) AS logradouro
    FROM text_replacing tr
  ),

  text_handling AS
  (
    SELECT
      ds.rn,
      ar.rede AS nm_rede,
      ar.descricao AS nm_descricao_original,
      ad.numero AS nm_endereco_numero,
      ad.logradouro AS nm_endereco_logradouro,
      tr.bairro AS nm_bairro,
      tr.produto nm_produto,
      tr.marca AS nm_marca,
      tr.tipo AS nm_tipo
    FROM desired_subset ds
    LEFT JOIN accentuation_removal ar ON ds.rn = ar.rn
    LEFT JOIN text_replacing tr ON ds.rn = tr.rn
    LEFT JOIN address_split ad ON ds.rn = ad.rn
  ),

  numeric_handling AS
  (
    SELECT
      ds.rn,
      CAST(ds.id_empresa AS INT) AS cd_empresa,
      COALESCE(TRY_CAST(ds.codigo_categoria AS INT), 0) AS cd_categoria,
      CAST(ds.id_produto AS BIGINT) AS cd_produto,
      CAST(ds.preco_regular AS NUMERIC(10, 2)) AS vl_preco_regular,
      CAST(ds.preco_atacado AS NUMERIC(10, 2)) AS vl_preco_atacado,
      CAST(ds.preco_atacado_qtd AS NUMERIC(10, 0)) AS vl_preco_atacado_quantidade,
      CAST(ds.preco_promocao AS NUMERIC(10, 2)) AS vl_preco_promocao,
      CAST(ds.preco_fidelidade AS NUMERIC(10, 2)) AS vl_preco_fidelidade
    FROM desired_subset ds
  )

SELECT
    ds.dt_referencia,
    ds.nm_origem,
    nh.cd_empresa,
    th.nm_rede,
    th.nm_bairro,
    th.nm_endereco_logradouro,
    th.nm_endereco_numero,
    nh.cd_categoria,
    nh.cd_produto,
    th.nm_produto,
    th.nm_marca,
    th.nm_tipo,
    th.nm_descricao_original,
    MIN(nh.vl_preco_regular) AS vl_preco_regular,
    MIN(nh.vl_preco_atacado) AS vl_preco_atacado,
    MIN(nh.vl_preco_atacado_quantidade) AS vl_preco_atacado_quantidade,
    MIN(nh.vl_preco_promocao) AS vl_preco_promocao,
    MIN(nh.vl_preco_fidelidade) AS vl_preco_fidelidade
FROM desired_subset ds
LEFT JOIN text_handling th ON (ds.rn = th.rn)
LEFT JOIN numeric_handling nh ON (ds.rn = nh.rn)
GROUP BY
    ds.dt_referencia,
    ds.nm_origem,
    nh.cd_empresa,
    th.nm_rede,
    th.nm_bairro,
    th.nm_endereco_logradouro,
    th.nm_endereco_numero,
    nh.cd_categoria,
    nh.cd_produto,
    th.nm_produto,
    th.nm_marca,
    th.nm_tipo,
    th.nm_descricao_original
