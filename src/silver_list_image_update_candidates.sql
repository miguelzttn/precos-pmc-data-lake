-- This lists all candidates of image update

WITH produtos AS 
(
  -- Coleta a descricao mais nova do produto
  SELECT
    cd_produto,
    FIRST(nm_produto) AS nm_produto,
    FIRST(nm_descricao_original) AS nm_descricao_original
  FROM precos_pmc.silver.cotacoes
  WHERE dt_referencia >= '2025-01-01'
  GROUP BY cd_produto, dt_referencia
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cd_produto ORDER BY dt_referencia DESC) = 1
)
SELECT
  p.cd_produto, 
  (
    CASE WHEN p.cd_produto > 100000000000 -- IS BAR CODE 
      THEN CONCAT(p.cd_produto, ' ', p.nm_produto)
      ELSE REPLACE(REPLACE(p.nm_descricao_original, '-( + ) BARATO ', ''), '-', '')
    END
  ) AS query
FROM produtos p
WHERE p.cd_produto NOT IN (
  SELECT pi.cd_produto 
  FROM precos_pmc.silver.produtos_imagens pi
)
ORDER BY p.cd_produto