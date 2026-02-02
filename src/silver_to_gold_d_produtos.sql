-- d_produtos
-- Snapshot dimension

WITH mais_recentes AS 
(
  SELECT 
    cd_produto,
    nm_produto,
    nm_tipo, 
    nm_marca,
    nm_descricao_original,
    dt_referencia AS dt_ultima_atualizacao,
    MIN(
      CASE
        WHEN vl_preco_atacado > 0 THEN vl_preco_atacado
        WHEN vl_preco_promocao > 0 THEN vl_preco_promocao
        WHEN vl_preco_fidelidade > 0 THEN vl_preco_fidelidade
        ELSE vl_preco_regular
      END
    ) AS vl_ultimo_preco_mais_baixo
  FROM precos_pmc.silver.cotacoes
  GROUP BY
    cd_produto,
    nm_produto,
    nm_tipo, 
    nm_marca,
    nm_descricao_original,
    dt_referencia
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cd_produto ORDER BY dt_referencia DESC) = 1
)
SELECT
  mr.cd_produto,
  mr.nm_produto,
  mr.nm_tipo,
  mr.nm_marca,
  mr.nm_descricao_original,
  mr.dt_ultima_atualizacao,
  mr.vl_ultimo_preco_mais_baixo,
  pi.nm_url_thumbnail,
  pi.nm_source_name,
  pi.nm_source_link
FROM mais_recentes mr
LEFT JOIN precos_pmc.silver.produtos_imagens pi ON mr.cd_produto = pi.cd_produto
ORDER BY mr.nm_produto