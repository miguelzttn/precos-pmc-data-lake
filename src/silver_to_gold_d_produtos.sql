-- d_produtos
-- Snapshot dimension

SELECT DISTINCT
  cd_produto,
  nm_produto,
  nm_tipo,
  nm_marca,
  MAX(nm_descricao_original) as nm_descricao_original,
  MAX(dt_referencia) AS dt_ultima_atualizacao
FROM precos_pmc.silver.cotacoes
GROUP BY
  cd_produto,
  nm_produto,
  nm_tipo,
  nm_marca
ORDER BY nm_produto