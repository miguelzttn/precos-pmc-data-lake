-- d_produtos
-- Snapshot dimension

WITH mais_recentes AS 
(
  SELECT 
    cd_produto,
    FIRST(nm_produto) AS nm_produto,
    FIRST(nm_tipo) AS nm_tipo, 
    FIRST(nm_marca) AS nm_marca,
    FIRST(nm_descricao_original) as nm_descricao_original,
    dt_referencia AS dt_ultima_atualizacao
  FROM precos_pmc.silver.cotacoes
  GROUP BY cd_produto, dt_referencia
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cd_produto ORDER BY dt_referencia DESC) = 1
), menores_precos AS 
(
  SELECT
    c.cd_produto,
    MIN(
      CASE
        WHEN c.vl_preco_atacado > 0 THEN c.vl_preco_atacado
        WHEN c.vl_preco_promocao > 0 THEN c.vl_preco_promocao
        WHEN c.vl_preco_fidelidade > 0 THEN c.vl_preco_fidelidade
        ELSE c.vl_preco_regular
      END
    ) AS vl_ultimo_preco_mais_baixo
  FROM precos_pmc.silver.cotacoes c
  LEFT JOIN mais_recentes mr ON c.cd_produto = mr.cd_produto AND c.dt_referencia = mr.dt_ultima_atualizacao
  GROUP BY c.cd_produto
)
SELECT
  mr.cd_produto,
  mr.nm_produto,
  mr.nm_tipo,
  mr.nm_marca,
  mr.nm_descricao_original,
  mr.dt_ultima_atualizacao,
  mp.vl_ultimo_preco_mais_baixo,
  pi.nm_url_thumbnail,
  pi.nm_source_name,
  pi.nm_source_link
FROM mais_recentes mr
LEFT JOIN menores_precos mp ON mp.cd_produto = mr.cd_produto
LEFT JOIN precos_pmc.silver.produtos_imagens pi ON mr.cd_produto = pi.cd_produto
ORDER BY mr.nm_produto