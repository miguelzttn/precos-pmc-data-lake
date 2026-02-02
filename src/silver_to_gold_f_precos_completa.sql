
-- f_precos_completa
-- This is a full fact for the last year

WITH base_completa AS
  (
    SELECT
      *
    FROM precos_pmc.silver.cotacoes co
    WHERE co.dt_referencia >= date_sub(current_date(), (30 * 12)) -- Ultimo ano
  )
SELECT
  c.cd_produto,
  c.dt_referencia,
  c.nm_rede,
  c.nm_bairro,
  (
    CASE
      WHEN c.vl_preco_promocao > 0 THEN 'PROMOCAO'
      WHEN c.vl_preco_fidelidade > 0 THEN 'FIDELIDADE'
      ELSE 'REGULAR'
    END
  ) AS nm_tipo_preco_varejo,
  (
    CASE
      WHEN c.vl_preco_promocao > 0 THEN c.vl_preco_promocao
      WHEN c.vl_preco_fidelidade > 0 THEN c.vl_preco_fidelidade
      ELSE c.vl_preco_regular
    END
  ) AS vl_preco_varejo,
  (
    CASE
      WHEN 
        c.vl_preco_atacado > 0 
        AND c.vl_preco_atacado_quantidade > 0 
        AND c.vl_preco_atacado NOT IN (c.vl_preco_regular, c.vl_preco_promocao, c.vl_preco_fidelidade)
          THEN 'LEVANDO ' || c.vl_preco_atacado_quantidade
      WHEN 
        c.vl_preco_atacado > 0 
        AND c.vl_preco_atacado_quantidade == 0 
        AND c.vl_preco_atacado NOT IN (c.vl_preco_regular, c.vl_preco_promocao, c.vl_preco_fidelidade) 
          THEN 'LEVANDO QUANTIDADE NÃO INFORMADA'
      ELSE 'MESMO DE VAREJO'
    END
  ) AS nm_tipo_preco_atacado,
  (
    CASE
      WHEN c.vl_preco_atacado > 0 THEN c.vl_preco_atacado
      WHEN c.vl_preco_promocao > 0 THEN c.vl_preco_promocao
      WHEN c.vl_preco_fidelidade > 0 THEN c.vl_preco_fidelidade
      ELSE c.vl_preco_regular
    END
  ) AS vl_preco_atacado
FROM base_completa c
QUALIFY row_number() OVER (PARTITION BY c.cd_produto, c.nm_rede, c.nm_bairro ORDER BY c.dt_referencia DESC) = 1
