
WITH 
  most_fresh_database AS 
  (
    SELECT
      MAX(make_date(year, month, day)) AS dt_referencia
    FROM precos_pmc.bronze.base_incremental
  ), 
  desired_subset AS 
  (
    SELECT
      row_number() OVER (ORDER BY bi.data_pesquisa) AS rn,
      'BASE COMPLETA' AS nm_origem,
      data_pesquisa AS dt_referencia,
      bi.*
    FROM precos_pmc.bronze.base_incremental bi
    INNER JOIN most_fresh_database fd ON make_date(bi.year, bi.month, bi.day) = fd.dt_referencia
    WHERE 1=1
    AND bi.data_pesquisa IN ({list_of_quoted_date_references})
    AND bi.data_pesquisa > '2022-09-26' -- There was a problem with data before of it on schema
  ), 
  text_handleds AS 
  (
    SELECT
      ds.rn,
      TRANSLATE(UPPER(ds.rede), 'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC') AS nm_rede, 
      TRANSLATE(UPPER(ds.bairro), 'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC') AS nm_bairro,
      TRANSLATE(REPLACE(REPLACE(REPLACE(
        UPPER(ds.endereco_rua)
      , 'RUA', 'R.'), 'AVENIDA', 'AV.'), 'ROD. ', '')
      , 'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ', 'AAAAAEEEEIIIIOOOOOUUUUC') AS nm_endereco_logradouro,
      ds.endereco_numero AS nm_endereco_numero,
      UPPER(ds.produto) AS nm_produto,
      'NÃO INFORMADA' as nm_marca,
      CONCAT(ds.produto, ' -', ds.qtd_embalagem, ' ', ds.unidade_sigla) as nm_descricao_original,
      REPLACE(CONCAT(ds.qtd_embalagem, ' ', ds.unidade_sigla), '.0', '') AS nm_tipo
    FROM desired_subset ds

  ), values_handleds AS 
  (
    SELECT
      ds.rn,
      CAST(ds.id_empresa AS INT) AS cd_empresa,
      COALESCE(TRY_CAST(ds.id_produto_classificacao AS BIGINT), 0) AS cd_categoria,
      COALESCE(TRY_CAST(ds.id_produto AS BIGINT), 0) AS cd_produto,
      CAST(ROUND(COALESCE(TRY_CAST(REPLACE(TRIM(preco_encontrado), ',', '.') AS DECIMAL(11, 2)), 0), 2) AS DECIMAL(11, 2)) AS vl_preco_regular,
      CAST(0.0 AS DECIMAL(10, 2)) AS vl_preco_atacado,
      CAST(0.0 AS DECIMAL(10, 0)) vl_preco_atacado_quantidade,
      CAST(0.0 AS DECIMAL(10, 2)) AS vl_preco_promocao,
      CAST(0.0 AS DECIMAL(10, 2)) AS vl_preco_fidelidade
    FROM desired_subset ds
  )
SELECT
  ds.dt_referencia,
  ds.nm_origem,
  vh.cd_empresa,
  th.nm_rede,
  th.nm_bairro,
  th.nm_endereco_logradouro,
  th.nm_endereco_numero,
  vh.cd_categoria,
  vh.cd_produto,
  th.nm_produto,
  th.nm_marca,
  th.nm_tipo,
  th.nm_descricao_original,
  vh.vl_preco_regular,
  vh.vl_preco_atacado,
  vh.vl_preco_atacado_quantidade,
  vh.vl_preco_promocao,
  vh.vl_preco_fidelidade
FROM 
  desired_subset ds
  LEFT JOIN text_handleds th ON (ds.rn = th.rn) 
  LEFT JOIN values_handleds vh ON (ds.rn = vh.rn)
WHERE
  vh.vl_preco_regular > 0
