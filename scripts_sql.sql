-- Atualização da funcao "ROUND" para tipos de dados float
CREATE FUNCTION ROUND(float,int) RETURNS NUMERIC AS $$
    SELECT ROUND($1::numeric,$2);
 $$ language SQL IMMUTABLE;

-- Item 2
-- Calcular a média das notas por estado dos alunos não "treineiros".
-- Obs: Como temos alguns atributos relacionados ao estado, estou considerando o atributo 
-- "SG_UF_RESIDENCIA" (sigla da UF da residencia do paraticipante).
select COALESCE("SG_UF_RESIDENCIA",'-')as "UF" , ROUND(avg("NU_NOTA_CH"),2)as "MEDIA_CH"
,ROUND(avg("NU_NOTA_CN"),2) as "MEDIA_CN", ROUND(avg("NU_NOTA_LC"),2)as "MEDIA_LC"
,ROUND(avg("NU_NOTA_MT"),2) as "MEDIA_MT",ROUND(avg("NU_NOTA_REDACAO"),2)as "MEDIA_REDACAO"
from "MICRODADOS" 
where "IN_TREINEIRO" = 0
group by "SG_UF_RESIDENCIA"

-- Item 3
-- Calcular o percentual de acerto de cada habilidade das questões de "Ciências da Natureza", 
-- limitando aos 5 estados com maior quantidade de inscritos.
-- Obs: UF utilizada é a UF de residencia do participante

-- Utilizando CTEs, primeiro, selecionando os 5 estados com maior qtd de inscritos
with TOP_UF as (select count("NU_INSCRICAO")as "QTD","SG_UF_RESIDENCIA" 
from "MICRODADOS" m 
group by "SG_UF_RESIDENCIA" 
order by "QTD" desc
limit 5)

-- Selecionando qtd de itens por "SG_AREA", filtrando por "CN"
, total_itens as(select count("CO_ITEM") as "QTD" ,ip."SG_AREA"
from "MICRODADOS" m 
inner join TOP_UF t on t."SG_UF_RESIDENCIA" = m."SG_UF_RESIDENCIA" 
inner join "ITENS_PROVA" ip on ip."CO_PROVA" = m."CO_PROVA_CN" 
where ip."SG_AREA" ='CN'
group by ip."SG_AREA")

-- Selecionando a qtd de itens e calculando percentual de acerto por habilidade
select ip."CO_HABILIDADE" as "HABILIDADE", count(ip."CO_ITEM") as "QTD_ITEM"
, ROUND( (count(ip."CO_ITEM")/tit."QTD"::float ),3) as "PERC"
from "MICRODADOS" m 
inner join TOP_UF t on t."SG_UF_RESIDENCIA" = m."SG_UF_RESIDENCIA" 
inner join "ITENS_PROVA" ip on ip."CO_PROVA" = m."CO_PROVA_CN" 
inner join total_itens tit on tit."SG_AREA"= ip."SG_AREA"
where ip."SG_AREA" ='CN'
group by ip."CO_HABILIDADE", tit."QTD"
order by ip."CO_HABILIDADE"
