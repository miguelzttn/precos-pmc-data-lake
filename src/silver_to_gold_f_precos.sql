
-- f_precos
-- This is a snapshot fact

WITH ultimas_atualizacoes AS
  (
    SELECT
      nm_rede, 
      nm_bairro,
      cd_produto,
      MAX(dt_referencia) AS dt_ultima_atualizacao
    FROM precos_pmc.silver.cotacoes
    GROUP BY
      nm_rede, 
      nm_bairro,
      cd_produto
  )
SELECT
  c.nm_rede,
  c.nm_bairro,
  c.cd_produto,
  ua.dt_ultima_atualizacao,
  (
    CASE
      WHEN vl_preco_promocao > 0 THEN 'PROMOCAO'
      WHEN vl_preco_fidelidade > 0 THEN 'FIDELIDADE'
      ELSE 'REGULAR'
    END
  ) AS nm_tipo_preco_varejo,
  (
    CASE
      WHEN vl_preco_promocao > 0 THEN vl_preco_promocao
      WHEN vl_preco_fidelidade > 0 THEN vl_preco_fidelidade
      ELSE vl_preco_regular
    END
  ) AS vl_preco_varejo,
  (
    CASE
      WHEN 
        vl_preco_atacado > 0 AND vl_preco_atacado_quantidade > 0 AND vl_preco_atacado NOT IN (vl_preco_regular, vl_preco_promocao, vl_preco_fidelidade)THEN 'LEVANDO ' || vl_preco_atacado_quantidade
      WHEN vl_preco_atacado > 0 AND vl_preco_atacado_quantidade == 0 AND vl_preco_atacado NOT IN (vl_preco_regular, vl_preco_promocao, vl_preco_fidelidade) THEN 'LEVANDO QUANTIDADE NÃO INFORMADA'
      ELSE 'MESMO DE VAREJO'
    END
  ) AS nm_tipo_preco_atacado,
  (
    CASE
      WHEN vl_preco_atacado > 0 THEN vl_preco_atacado
      WHEN vl_preco_promocao > 0 THEN vl_preco_promocao
      WHEN vl_preco_fidelidade > 0 THEN vl_preco_fidelidade
      ELSE vl_preco_regular
    END
  ) AS vl_preco_atacado
FROM precos_pmc.silver.cotacoes c
INNER JOIN ultimas_atualizacoes ua ON (
  c.nm_rede = ua.nm_rede 
  AND c.nm_bairro = ua.nm_bairro
  AND c.cd_produto = ua.cd_produto
  AND c.dt_referencia = ua.dt_ultima_atualizacao
)