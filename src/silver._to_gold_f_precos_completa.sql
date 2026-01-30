
-- f_precos_completa
-- This is a full fact

WITH base_completa AS
  (
    SELECT
      *
    FROM precos_pmc.silver.cotacoes
  )
SELECT
  c.nm_rede,
  c.nm_bairro,
  c.cd_produto,
  c.dt_referencia,
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
FROM base_completa c

