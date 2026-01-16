
-- d_empresas
-- Live snapshot dimension

WITH 
  ultimas_aparicoes AS
  (
    SELECT 
      cd_empresa,
      nm_rede,
      nm_bairro,
      MAX(dt_referencia) AS dt_ultima_atualizacao
    FROM precos_pmc.silver.cotacoes
    GROUP BY
      cd_empresa,
      nm_rede,
      nm_bairro 
  )
SELECT
 c.nm_rede,
 c.nm_bairro,
 c.nm_endereco_logradouro,
 c.nm_endereco_numero,
 MAX(c.dt_referencia) AS dt_ultima_atualizacao
FROM precos_pmc.silver.cotacoes c
INNER JOIN ultimas_aparicoes ua 
  ON (
    c.nm_rede = ua.nm_rede 
    AND c.nm_bairro = c.nm_bairro 
    AND c.dt_referencia = ua.dt_ultima_atualizacao
  )
GROUP BY 
 c.nm_rede,
 c.nm_bairro,
 c.nm_endereco_logradouro,
 c.nm_endereco_numero
ORDER BY 
  c.nm_rede, 
  c.nm_bairro
