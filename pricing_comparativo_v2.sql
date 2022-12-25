CREATE OR REPLACE VIEW SNP_SANDBOX.PRICING_COMPARISON AS
(
WITH cruce_scrapping as (
    -- Se cruzan las tablas de scrapping de VEA Y TOTTUS con las tabla de SKUS de productos NBO de FAVO
    select pes.SKU_FAVO,
           pes.DESC_FAVO,
           pes.COMPETENCIA,
           -- se crean columnas dummy para hacer el pivot
           concat(pes.COMPETENCIA, '1')                     As COMPETENCIA1,
           concat(pes.COMPETENCIA, '2')                     As COMPETENCIA2,
           coalesce(vea.PRECIO_REGULAR, tot.PRECIO_REGULAR) as PRECIO_REGULAR,
           coalesce(vea.PRECIO_ACTUAL, tot.PRECIO_ACTUAL)   as PRECIO_ACTUAL,
           coalesce(vea.PRECIO_TARJETA, tot.PRECIO_TARJETA) as PRECIO_TARJETA,
           coalesce(vea.EFFECTIVE_DAY, tot.EFFECTIVE_DAY)   as EFFECTIVE_DAY
    from favodata.SNP_UNTRUSTED.PESQUISA as pes
             -- se debe generar el cruce a través de los links debido a que no existe sku key
             LEFT JOIN FAVODATA.COMMERCIAL_UNTRUSTED.PLAZAVEA_PRODUCTS_WEBSCRAPING as vea
                       ON pes.LINK_COMPETENCIA = vea.LINK_PRODUCTO
             left join FAVODATA.COMMERCIAL_UNTRUSTED.TOTTUS_PRODUCTS_WEBSCRAPING as tot
                       ON pes.LINK_COMPETENCIA = concat(tot.LINK_PRODUCTO, '/')
         -- la tabla de tottus tiene un extra /
    order by pes.SKU_FAVO, EFFECTIVE_DAY),

     pivot_scrapping as (select *
                                -- se pivotea la tabla para generar una columna por cada precio de la competencia
                         from cruce_scrapping
                                  pivot ( MAX(PRECIO_REGULAR) for COMPETENCIA in ('Vea', 'Tottus'))
                                  as p
                                  pivot ( MAX(PRECIO_ACTUAL) for COMPETENCIA1 in ('Vea1', 'Tottus1'))
                                  as p2
                                  pivot ( MAX(PRECIO_TARJETA) for COMPETENCIA2 in ('Vea2', 'Tottus2'))
                                  as p3 (SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY, PRECIO_REGULAR_VEA, PRECIO_REGULAR_TOTTUS,
                                         PRECIO_ACTUAL_VEA, PRECIO_ACTUAL_TOTTUS, PRECIO_TARJETA_VEA,
                                         PRECIO_TARJETA_TOTTUS)
                         order by SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY)
        ,

     scrapping_final as (
         -- para eliminar duplicados y entradas nulas con precios, se agrupa por dia y sku
         SELECT SKU_FAVO,
                DESC_FAVO,
                EFFECTIVE_DAY,
                MAX(PRECIO_REGULAR_VEA)    AS PRECIO_REGULAR_VEA,
                MAX(PRECIO_REGULAR_TOTTUS) AS PRECIO_REGULAR_TOTTUS,
                MAX(PRECIO_ACTUAL_VEA)     AS PRECIO_ACTUAL_VEA,
                MAX(PRECIO_ACTUAL_TOTTUS)  AS PRECIO_ACTUAL_TOTTUS,
                MAX(PRECIO_TARJETA_VEA)    AS PRECIO_TARJETA_VEA,
                MAX(PRECIO_TARJETA_VEA)    AS PRECIO_TARJETA_TOTTUS
         FROM pivot_scrapping
         GROUP BY 1, 2, 3
         ORDER BY 1, 2, 3)
        ,

     matching_final as (SELECT SKU_FAVO,
                               DESC_FAVO,
                               EFFECTIVE_DAY,
                               MAX(PRECIO_REGULAR_VEA)    AS PRECIO_REGULAR_VEA,
                               MAX(PRECIO_REGULAR_TOTTUS) AS PRECIO_REGULAR_TOTTUS,
                               MAX(PRECIO_ACTUAL_VEA)     AS PRECIO_ACTUAL_VEA,
                               MAX(PRECIO_ACTUAL_TOTTUS)  AS PRECIO_ACTUAL_TOTTUS,
                               MAX(PRECIO_TARJETA_VEA)    AS PRECIO_TARJETA_VEA,
                               MAX(PRECIO_TARJETA_VEA)    AS PRECIO_TARJETA_TOTTUS
                        FROM (select *
                                     -- se pivotea la tabla para generar una columna por cada precio de la competencia
                              from (-- cruce de los productos de la tabla matching
                                       select mat.SKU_FAVO,
                                              mat.DESC_FAVO,
                                              mat.COMPETENCIA,
                                              -- se crean columnas dummy para hacer el pivot
                                              concat(mat.COMPETENCIA, '1')                     As COMPETENCIA1,
                                              concat(mat.COMPETENCIA, '2')                     As COMPETENCIA2,
                                              coalesce(vea.PRECIO_REGULAR, tot.PRECIO_REGULAR) as PRECIO_REGULAR,
                                              coalesce(vea.PRECIO_ACTUAL, tot.PRECIO_ACTUAL)   as PRECIO_ACTUAL,
                                              coalesce(vea.PRECIO_TARJETA, tot.PRECIO_TARJETA) as PRECIO_TARJETA,
                                              coalesce(vea.EFFECTIVE_DAY, tot.EFFECTIVE_DAY)   as EFFECTIVE_DAY
                                       from (select *
                                             from (select *, row_number() over (partition by SKU_FAVO order by diff asc) rn
                                                   from (select *
                                                         from (SELECT DISTINCT SKU_FAVO,
                                                                               DESCRIPTION_FAVO                                      as               DESC_FAVO,
                                                                               SKU_TOTTUS                                            AS               SKU_COMP,
                                                                               DESCRIPTION_TOTTUS                                    AS               DESC_COMPETENCIA,
                                                                               case when LINK_PRODUCTO like '%plazavea%' then 'VEA' ELSE 'TOTTUS' END COMPETENCIA,
                                                                               LINK_PRODUCTO,
                                                                               SIMILARITY_SCORE,
                                                                               LENGTH(DESCRIPTION_FAVO) - LENGTH(DESCRIPTION_TOTTUS) AS               DIFF,
                                                                               maes.STATUS_SKU,
                                                                               sku_conv.qty
                                                               FROM (select *
                                                                     from COMMERCIAL_UNTRUSTED.FAVO_TOTTUS_MATCHING
                                                                     WHERE SIMILARITY_SCORE IS NOT NULL
                                                                     UNION ALL
                                                                     (SELECT *
                                                                      FROM COMMERCIAL_UNTRUSTED.FAVO_PLAZAVEA_MATCHING
                                                                      WHERE SIMILARITY_SCORE IS NOT NULL)) as cru
                                                                        left join favodata.SNP_SANDBOX.MAESTRA as maes
                                                                                  on maes.sku = cru.SKU_FAVO
                                                                        LEFT JOIN FAVODATA.SNP_SANDBOX.PACKS_CONVERSION AS sku_conv
                                                                                  on sku_conv.value = cru.SKU_FAVO
                                                               WHERE maes.STATUS_SKU NOT IN ('YAPA', 'PACK')
                                                                 and sku_conv.qty is null
                                                                 and cru.DESCRIPTION_FAVO not like '%YAPA%'
                                                                 and cru.DESCRIPTION_FAVO not like '%PACK%'
                                                                 and cru.DESCRIPTION_FAVO not like '%+%')
                                                         UNION ALL
                                                         (SELECT DISTINCT SKU_FAVO,
                                                                          DESCRIPTION_FAVO                                      as               DESC_FAVO,
                                                                          SKU_TOTTUS                                            AS               SKU_COMP,
                                                                          DESCRIPTION_TOTTUS                                    AS               DESC_COMPETENCIA,
                                                                          case when LINK_PRODUCTO like '%plazavea%' then 'VEA' ELSE 'TOTTUS' END COMPETENCIA,
                                                                          LINK_PRODUCTO,
                                                                          SIMILARITY_SCORE,
                                                                          LENGTH(DESCRIPTION_FAVO) - LENGTH(DESCRIPTION_TOTTUS) AS               DIFF,
                                                                          maes.STATUS_SKU,
                                                                          sku_conv.qty
                                                          FROM (select *
                                                                from COMMERCIAL_UNTRUSTED.FAVO_TOTTUS_MATCHING
                                                                WHERE SIMILARITY_SCORE IS NOT NULL
                                                                UNION ALL
                                                                (SELECT *
                                                                 FROM COMMERCIAL_UNTRUSTED.FAVO_PLAZAVEA_MATCHING
                                                                 WHERE SIMILARITY_SCORE IS NOT NULL)) as cru
                                                                   left join favodata.SNP_SANDBOX.MAESTRA as maes
                                                                             on maes.sku = cru.SKU_FAVO
                                                                   LEFT JOIN FAVODATA.SNP_SANDBOX.PACKS_CONVERSION AS sku_conv
                                                                             on sku_conv.value = cru.SKU_FAVO
                                                          WHERE maes.STATUS_SKU NOT IN ('YAPA', 'PACK')
                                                            and sku_conv.qty is null
                                                            and cru.DESCRIPTION_FAVO not like '%YAPA%'
                                                            and (cru.DESCRIPTION_FAVO LIKE '%DOYPACK%' AND
                                                                 cru.DESCRIPTION_FAVO NOT LIKE '%+%'))

                                                         ORDER BY SKU_FAVO)
                                                   ORDER BY SKU_FAVO, rn)
                                             where rn = 1
                                             order by sku_favo) as mat

                                                -- se debe generar el cruce a través de los links debido a que no existe sku key
                                                LEFT JOIN FAVODATA.COMMERCIAL_UNTRUSTED.PLAZAVEA_PRODUCTS_WEBSCRAPING as vea
                                                          ON mat.LINK_PRODUCTO = vea.LINK_PRODUCTO
                                                left join FAVODATA.COMMERCIAL_UNTRUSTED.TOTTUS_PRODUCTS_WEBSCRAPING as tot
                                                          ON mat.LINK_PRODUCTO = tot.LINK_PRODUCTO
                                            -- la tabla de tottus tiene un extra /
                                       order by mat.SKU_FAVO, EFFECTIVE_DAY)
                                       pivot ( MAX(PRECIO_REGULAR) for COMPETENCIA in ('VEA', 'TOTTUS'))
                                       as p
                                       pivot ( MAX(PRECIO_ACTUAL) for COMPETENCIA1 in ('VEA1', 'TOTTUS1'))
                                       as p2
                                       pivot ( MAX(PRECIO_TARJETA) for COMPETENCIA2 in ('VEA2', 'TOTTUS2'))
                                       as p3 (SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY, PRECIO_REGULAR_VEA,
                                              PRECIO_REGULAR_TOTTUS,
                                              PRECIO_ACTUAL_VEA, PRECIO_ACTUAL_TOTTUS, PRECIO_TARJETA_VEA,
                                              PRECIO_TARJETA_TOTTUS)
                              order by SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY)
                        GROUP BY 1, 2, 3
                        ORDER BY 1, 2, 3),

     cruce_competencia as (SELECT SKU_FAVO,
                                  DESC_FAVO,
                                  EFFECTIVE_DAY,
                                  MAX(PRECIO_REGULAR_VEA)    AS PRECIO_REGULAR_VEA,
                                  MAX(PRECIO_REGULAR_TOTTUS) AS PRECIO_REGULAR_TOTTUS,
                                  MAX(PRECIO_ACTUAL_VEA)     AS PRECIO_ACTUAL_VEA,
                                  MAX(PRECIO_ACTUAL_TOTTUS)  AS PRECIO_ACTUAL_TOTTUS,
                                  MAX(PRECIO_TARJETA_VEA)    AS PRECIO_TARJETA_VEA,
                                  MAX(PRECIO_TARJETA_VEA)    AS PRECIO_TARJETA_TOTTUS
                           from (select *
                                 from scrapping_final
                                 union all
                                 (select * from matching_final)
                                 order by SKU_FAVO)
                           GROUP BY 1, 2, 3
                           ORDER BY 1, 2, 3),

     promo_acc as (
         -- cruce con la tabla de promos
         select distinct cast(p.date as date)                                                     as date,
                         country,
                         state,
                         sku,
                         first_value(campaign)
                                     over (partition by date, country, state, sku order by date ) as campaign,
                         first_value(regular_price)
                                     over (partition by date, country, state, sku order by date ) as regular_price
         from snp_untrusted.promo_tracking p
         where state in ('LIMA', 'TJL')
           and lower(sku) not like '%nuevo%'
           and lower(sku) not like '%crear%'
         -- usamos el primer valor de campaña y precio regular del día y usamos distinct para remover duplicados
     ),

     promo_acc2 as
         -- cruce con la tabla de promos
         (select distinct cast(effectiveday as date)                                                                                as date,
                          country,
                          state,
                          sku,
                          first_value(campaignname)
                                      over (partition by effectiveday, country, state, sku order by effectiveday )                  as campaign,
                          cast(first_value(regularprice)
                                           over (partition by effectiveday, country, state, sku order by effectiveday ) as decimal) as regular_price
          from commercial_untrusted.offer_history),

     base_costo AS (SELECT CASE
                               WHEN CD = 'TRUJILLO' THEN 'TRUJILLO'
                               WHEN CD = 'LIMA' THEN 'LIMA'
                               ELSE NULL
                               END                                              AS CD,
                           TAX,
                           SKU,
                           CAST(REGISTRATION_DATE AS DATE)                      AS DATA_REGISTRO,
                           case
                               when LAG(CAST(REGISTRATION_DATE AS DATE), 1)
                                        OVER (PARTITION BY CD, SKU ORDER BY CAST(REGISTRATION_DATE AS DATE) ASC) is null
                                   then
                                   cast(
                                           COALESCE(DATEADD('day', 1, LAG(CAST(REGISTRATION_DATE AS DATE), 1)
                                                                          OVER (PARTITION BY CD, SKU ORDER BY CAST(REGISTRATION_DATE AS DATE) ASC)),
                                                    '2021-01-01') as date)
                               else DATA_REGISTRO END                           as DATA_REGISTRO_START,
                           cast(COALESCE(DATEADD('day', -1, LEAD(CAST(REGISTRATION_DATE AS DATE), 1)
                                                                 OVER (PARTITION BY CD, SKU ORDER BY CAST(REGISTRATION_DATE AS DATE) ASC)),
                                         CAST(CURRENT_DATE() AS DATE)) as date) AS DATA_REGISTRO_END,
                           SKU_COST
                    FROM EXPANSION_UNTRUSTED.PRODUCT_COST
                    WHERE CD = 'LIMA')
        ,

     base_favo_1 as
         -- creamos la base principal del query donde estaran los atributos principales

         (SELECT CAST(base.CREATE_DATE_TIME_TZ AS DATE)                                             as ORDER_DAY,       -- dia de creacion de la orden
                 coalesce(sku_conv.VALUE, base.sku)                                                 as sku_final,       -- coalesce para asignar el valor de la otra tabla si resulta null
                 base.SKU                                                                           as sku_pre,
                 coalesce(mc.category, base.category)                                               as category,        --ml.category
                 coalesce(mc.description, base.description)                                         as sku_description, --,ml.description
                 coalesce(mc.family, null)                                                          as family,          --ml.family
                 coalesce(mc.brand, null)                                                           as brand,           --ml.brand
                 IFF(ent.TIER LIKE 'mayorista%', 'Mayorista', 'No Mayorista')                          Cluster,
                 IFF(coalesce(acc.campaign, acc2.campaign) is not null, 1, 0)                       as in_promo,        -- Promo del Producto
                 coalesce(acc.campaign, acc2.campaign, 'sin_promo')                                 as promo_campaign,  -- Campaña de la promocion

                 IFF(ss.NBO IS NULL, 0, 1)                                                             flag_NBO,        -- Flag del producto si es NBO

                 ss.ABC_REGULAR_NOT_PROMO,                                                                              -- Curva ABC

                 Case
                     when sku_conv.SKU_PACK IS NULL and base.ITEM_PACK_SKU is null THEN 0
                     ELSE 1 END                                                                        flag_pack,       -- El sku es un pack

                 IFNULL(SKU_CONV.IS_COMBO, ifnull(sku_ip.IS_COMBO, 0))                              AS IS_COMBO,        -- dentro del pack, hay mas de un sku

                 IFNULL(sku_conv.QTY, ifnull(sku_ip.QTY, 1))                                           factor_sku,      -- factor de conversion del pack de sku a unitario sku
                 coalesce(acc.regular_price, acc2.regular_price)                                    as regular_price,   -- precio regular del sku


                 SUM(QUANTITY * IFNULL(sku_conv.QTY, 1))                                            as unidades,        -- Unidades de SKU dentro de las ordenes creadas de ese dia

                 sum(co.SKU_COST * base.QUANTITY)                                                      costo,

                 SUM(case
                         when coupon like any ('POC-MAY-%', 'MAY-%', 'POC-FAV-%', 'FAV-%') then net_value
                         else gross_value end)                                                         venta,           -- Si es que el sku tiene un cupon darle la venta neta, sino gross
                 round(div0(SUM(case when flag_pack = 0 then venta else 0 end)
                                over (partition by sku_final, date_trunc('month', ORDER_DAY)), --
                            SUM(case when flag_pack = 0 then unidades else 0 end)
                                over (partition by sku_final, date_trunc('month', ORDER_DAY))), 2)     precio_mes_sku,


                 round(div0(SUM(case when flag_pack = 0 then costo else 0 end)
                                over (partition by sku_final, date_trunc('month', ORDER_DAY)), --
                            SUM(case when flag_pack = 0 then unidades else 0 end)
                                over (partition by sku_final, date_trunc('month', ORDER_DAY))), 2)     costo_mes_sku,


                 round(SUM(base.NET_VALUE) / unidades, 2)                                           as precio_unitario_actual,

                 round(precio_unitario_actual * 0.92, 2)                                            as precio_unitario_implicito,

                 --CASE WHEN in_promo = 1 THEN coalesce(acc.regular_price, acc2.regular_price)
                 --ELSE precio_unitario_actual END                                 precio_regular,

                 round(SUM(base.GROSS_VALUE), 2)                                                    as venta_gross,

                 round(SUM(base.NET_VALUE), 2)                                                      AS venta_net,

                 round(coalesce(acc.regular_price, acc2.regular_price) * unidades, 2)               as venta_regular,

                 round(coalesce(acc.regular_price, acc2.regular_price) - precio_unitario_actual, 2) as descuento_FAVO,


                 round(descuento_FAVO * unidades, 2)                                                as descuento_FAVO_neto_valorizado

          FROM JOURNEY.BASE as BASE
                   LEFT JOIN FAVODATA.SNP_SANDBOX.PACKS_CONVERSION AS sku_conv
                             ON base.SKU = sku_conv.SKU_PACK
                   LEFT JOIN FAVODATA.SNP_SANDBOX.PACKS_CONVERSION AS sku_ip
                             ON base.ITEM_PACK_SKU = sku_ip.SKU_PACK
                   LEFT JOIN FAVODATA.SNP_SANDBOX.MAESTRA mc
                             on mc.sku = coalesce(sku_conv.VALUE, base.sku)
              --LEFT JOIN FAVODATA.SNP_SANDBOX.MAESTRA_LOGISTICA ml
              --  on ml.sku::varchar= coalesce(sku_conv.VALUE,base.sku)
                   LEFT JOIN FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST as ent
                             ON ent.DYNAMO_LEADER_ID = base.DYNAMO_LEADER_ID AND
                                ent.CALENDAR_MONTH =
                                DATEFROMPARTS(YEAR(base.CREATE_DATE_TIME_TZ), MONTH(base.CREATE_DATE_TIME_TZ), 1)
                   LEFT JOIN PROMO_ACC as acc
                             on acc.date = base.CREATE_DATE_TIME_TZ::date
                                 and acc.sku = coalesce(base.item_pack_sku, base.sku)
                                 and LOWER(base.CD) = LOWER(acc.state)
                   LEFT JOIN PROMO_ACC2 as acc2
                             on acc2.date = base.CREATE_DATE_TIME_TZ::date and
                                acc2.sku = coalesce(base.item_pack_sku, base.sku) and
                                LOWER(base.CD) = LOWER(acc2.state)
                   LEFT JOIN favodata.snp_sandbox.ct_supply_stock as ss
                             ON CAST(base.CREATE_DATE_TIME_TZ AS DATE) = ss.CALENDAR_DATE
                                 AND ss.SKU = coalesce(sku_conv.VALUE, base.sku)
                                 AND ss.FACILITY_NAME = base.CD
                   left join base_costo co
                             on base.sku = co.SKU
                                 and create_date_time_tz::date between co.DATA_REGISTRO_START and co.DATA_REGISTRO_END

          WHERE base.COUNTRY = 'PE'
            AND IFNULL(BASE.CD, 'LIMA') = 'LIMA'
            AND ORDER_STATUS != 'CANCEL'
            AND ORDER_DAY >= '2021-11-01'

          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, acc.regular_price, acc2.regular_price
          order by order_day asc),
     base_favo_2 as (select *,
                            ifnull(LAG(CASE WHEN in_promo = 0 THEN precio_unitario_actual else null end)
                                       ignore nulls over (partition by sku_final, Cluster order by order_day),
                                   IFNULL(LEAD(CASE WHEN in_promo = 0 THEN precio_unitario_actual else null end)
                                               ignore nulls over (partition by sku_final, Cluster order by order_day),
                                          precio_regular_final))  precio_regular_simulado,
                            precio_regular_simulado * unidades as venta_regular_simulada
                     from (SELECT ORDER_DAY,
                                  sku_final,
                                  sku_pre,
                                  category,
                                  sku_description,
                                  family,
                                  brand,
                                  Cluster,
                                  in_promo,
                                  factor_sku,
                                  regular_price,
                                  promo_campaign,
                                  flag_NBO,
                                  ABC_REGULAR_NOT_PROMO,
                                  flag_pack,
                                  IS_COMBO,
                                  unidades,
                                  round(venta, 2)                                         as venta,    -- venta sin cupones seleccionados
                                  round(costo, 2)                                         as costo,
                                  precio_mes_sku,
                                  costo_mes_sku,
                                  case
                                      when IS_COMBO = 1 then
                                          div0(precio_mes_sku * factor_sku, sum(precio_mes_sku * factor_sku)
                                                                                over (partition by sku_pre, ORDER_DAY, Cluster))
                                      else 1 end                                             ratio,

                               case
                                      when IS_COMBO = 1 then
                                          div0(costo_mes_sku * factor_sku, sum(costo_mes_sku * factor_sku)
                                                                                over (partition by sku_pre, ORDER_DAY, Cluster))
                                      else 1 end                                             ratio_costo,

                                  round(venta * ratio, 2)                                 as venta_PL, --venta considerando el factor de conversion y ratio de proporcion de venta de pack  sku
                                  round(costo * ratio_costo, 2)                                 as costo_PL,
                                  round(venta_PL / unidades, 2)                           as precio_unitario_actual,
                                  round(costo_PL / unidades, 2)                           as costo_unitario_actual,

                                  CASE
                                      WHEN in_promo = 1 THEN div0(regular_price * ratio, factor_sku)
                                      else round(venta_PL / unidades, 2) end                 precio_regular_final,


                                  round(precio_regular_final * unidades, 2)               as venta_regular,

                                  round(precio_regular_final - precio_unitario_actual, 2) as descuento_FAVO,


                                  round(descuento_FAVO * unidades, 2)                     as descuento_FAVO_neto_valorizado
                           FROM base_favo_1)),

     base_competencia as (
         SELECT *,
                                 round(PRECIO_REGULAR_VEA * unidades, 2)                 as venta_VEA_regular,
                                 round(PRECIO_ACTUAL_VEA * unidades, 2)                  as venta_VEA_actual,
                                 round(PRECIO_TARJETA_VEA * unidades, 2)                 as venta_VEA_tarjeta,
                                 round(PRECIO_REGULAR_TOTTUS * unidades, 2)              as venta_TOTTUS_regular,
                                 round(PRECIO_ACTUAL_TOTTUS * unidades, 2)               as venta_TOTTUS_actual,
                                 round(PRECIO_TARJETA_TOTTUS * unidades, 2)              as venta_TOTTUS_tarjeta,
                                 round(PRECIO_REGULAR_VEA - PRECIO_ACTUAL_VEA, 2)        as dcto_VEA_actual,
                                 round(PRECIO_REGULAR_VEA - PRECIO_TARJETA_VEA, 2)       as dcto_VEA_tarjeta,
                                 round(PRECIO_REGULAR_TOTTUS - PRECIO_ACTUAL_TOTTUS, 2)  as dcto_TOTTUS_actual,
                                 round(PRECIO_REGULAR_TOTTUS - PRECIO_TARJETA_TOTTUS, 2) as dcto_TOTTUS_tarjeta,
                                 1 - (round(descuento_FAVO /
                                            NULLIF(round(PRECIO_REGULAR_VEA - PRECIO_ACTUAL_VEA, 2), 0),
                                            2))                                                    as indice_dcto_actual_vea,
                                 1 - (round(descuento_FAVO /
                                            NULLIF(round(PRECIO_REGULAR_VEA - PRECIO_TARJETA_VEA, 2), 0),
                                            2))                                                    as indice_dcto_tarjeta_vea,
                                 1 - (round(descuento_FAVO /
                                            NULLIF(round(PRECIO_REGULAR_TOTTUS - PRECIO_ACTUAL_TOTTUS, 2), 0),
                                            2))                                                    as indice_dcto_actual_tottus,
                                 1 - (round(descuento_FAVO /
                                            NULLIF(round(PRECIO_REGULAR_TOTTUS - PRECIO_TARJETA_TOTTUS, 2),
                                                   0),
                                            2))                                                    as indice_dcto_tarjeta_tottus
         FROM
         (SELECT favo.*,
                                  case when precio_unitario_actual between 0.5* comp.PRECIO_REGULAR_VEA and 2* comp.PRECIO_REGULAR_VEA then comp.PRECIO_REGULAR_VEA else null end PRECIO_REGULAR_VEA,
                                  case when precio_unitario_actual between 0.5* comp.PRECIO_REGULAR_TOTTUS and 2* comp.PRECIO_REGULAR_TOTTUS then comp.PRECIO_REGULAR_TOTTUS else null end PRECIO_REGULAR_TOTTUS,
                                  case when precio_unitario_actual between 0.5* comp.PRECIO_ACTUAL_VEA and 2* comp.PRECIO_ACTUAL_VEA then comp.PRECIO_ACTUAL_VEA else null end PRECIO_ACTUAL_VEA,
                                  case when precio_unitario_actual between 0.5* comp.PRECIO_ACTUAL_TOTTUS and 2* comp.PRECIO_ACTUAL_TOTTUS then comp.PRECIO_ACTUAL_TOTTUS else null end PRECIO_ACTUAL_TOTTUS,
                                  case when precio_unitario_actual between 0.5* comp.PRECIO_TARJETA_VEA and 2* comp.PRECIO_TARJETA_VEA then comp.PRECIO_TARJETA_VEA else null end PRECIO_TARJETA_VEA,
                                  case when precio_unitario_actual between 0.5* comp.PRECIO_TARJETA_TOTTUS and 2* comp.PRECIO_TARJETA_TOTTUS then comp.PRECIO_TARJETA_TOTTUS else null end PRECIO_TARJETA_TOTTUS
                                 FROM base_favo_2 as favo
                                   LEFT JOIN cruce_competencia as comp
                                             ON favo.ORDER_DAY = comp.EFFECTIVE_DAY and favo.sku_final = comp.SKU_FAVO)

                          ORDER BY 2, 1),

    /*

     BASES DE ANALYTICS PARA VER LOS PRODUCTOS MAS BUSCADOS DENTRO DE LA APP EN UN PERIODO DE D-30

     */

     search_raw as (select distinct event_params_flatten:ga_session_id::text as ga_id,
                                    CASE
                                        WHEN platform = 'IOS' OR platform = 'ANDROID' THEN 'APP'
                                        ELSE platform
                                        END                                  AS platforma,
                                    CASE
                                        WHEN geo_country = 'Peru' AND geo_region = 'La Libertad' THEN 6
                                        WHEN geo_country = 'Peru' AND geo_region <> 'La Libertad' THEN 2
                                        ELSE null
                                        END                                  AS facility_id,
                                    EVENT_TIMESTAMP_UTC,
                                    regexp_replace(upper(strip_accents(trim(event_params_flatten:search_term::text))),
                                                   '[^a-zA-Z0-9" "]+')       as search_term
                    from FAVODATA.CUSTOMER_GA_RAW.GA4_APP_EVENTS
                    where event_name = 'search'
                      and event_date_utc between ((current_date() - 1) - 30) and (current_date() - 1)
                      and platforma = 'APP'
                      and facility_id is not null

                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ga_id,platform,search_term ORDER BY EVENT_TIMESTAMP_UTC DESC) =
                            1
                    order by event_timestamp_utc asc)                              --select count(*) from search_raw ;
        ,
     search_trusted as (select *,
                               lead(search_term) over (partition by ga_id order by EVENT_TIMESTAMP_UTC) as lead_search,
                               case
                                   when regexp_substr(lead_search, search_term) is null
                                       then coalesce(regexp_substr(search_term, lead_search), null)
                                   else regexp_substr(lead_search, search_term)
                                   end                                                                  as match_lead
                        from search_raw)                                           --select * from search_trusted limit 100;
        ,
     search_info AS (select ga_id,
                            event_timestamp_utc,
                            search_term,
                            platforma as platform,
                            facility_id
                     from search_trusted
                     where match_lead is null)                                     --select * from search_info where facility_id=6;
        ,
     add_to_cart_info AS (select event_params_flatten:ga_session_id::text as ga_id,

                                 CASE
                                     WHEN platform = 'IOS' OR platform = 'ANDROID' THEN 'APP'
                                     ELSE platform
                                     END                                  AS platform,

                                 items,
                                 TRIM(ITEMS.value:item_id)                AS sku,
                                 UPPER(TRIM(ITEMS.value:item_name))       AS product,
                                 EVENT_TIMESTAMP_UTC,
                                 TRIM(ITEMS.value:quantity)               as quantity
                          from FAVODATA.CUSTOMER_GA_RAW.GA4_APP_EVENTS,
                               LATERAL FLATTEN(input => items) AS items
                          where event_name = 'add_to_cart'
                            and event_date_utc between ((current_date() - 1) - 30) and (current_date() - 1)
                            and platform <> 'WEB')                                 --select * from add_to_cart_info limit 10;
        ,
     join_add_to_cart_search AS (select distinct s.ga_id,
                                                 s.platform,
                                                 s.search_term,
                                                 s.EVENT_TIMESTAMP_UTC,
                                                 ac.sku,
                                                 s.facility_id,
                                                 regexp_replace(upper(strip_accents(trim(lsku.description))),
                                                                '[^a-zA-Z0-9" "]+') as product,
                                                 ac.quantity,

                                                 p2.campaign,

                                                 CASE
                                                     WHEN p2.campaign IS NOT NULL THEN 'Yes'
                                                     ELSE 'No'
                                                     END                            AS is_promo

                                 from search_info as s
                                          left join add_to_cart_info as ac
                                                    on s.ga_id = ac.ga_id
                                          left join logistics_raw.sku as lsku
                                                    on lsku.externalid = ac.sku

                                          left join
                                      (select distinct cast(effectiveday as date)                                                                                   as date,
                                                       country,
                                                       state,
                                                       sku,
                                                       first_value(campaignname)
                                                                   over (partition by effectiveday, country, state, sku order by effectiveday asc)                  as campaign,
                                                       cast(first_value(regularprice)
                                                                        over (partition by effectiveday, country, state, sku order by effectiveday asc) as decimal) as regular_price
                                       from commercial_untrusted.offer_history) p2
                                      on p2.date = s.EVENT_TIMESTAMP_UTC::DATE and p2.sku = ac.sku and
                                         LOWER(case
                                                   when s.facility_id = 6 then 'TRJ'
                                                   else 'LIMA' end) = LOWER(p2.state)

                                 where lsku.companyid = 'FAVOPE'
         --  and (is_promo = '{{ Promo }}' or '{{ Promo }}' = 'ALL')
         --and is_promo = 'No'
     )                                                                             -- select * from join_add_to_cart_search limit 10;
        ,
     match_search_cart_info as (select *,
                                       regexp_substr(product, search_term) as match
                                from join_add_to_cart_search
                                where match is not null
                                order by search_term asc, event_timestamp_utc asc) --select distinct sku, product from match_search_cart_info where ga_id = '1662041881' limit 100;
        ,
     count_search_terms as (select distinct search_term,
                                            facility_id,
                                            count(search_term) as count_search_term
                            from match_search_cart_info
                            group by 1, 2)                                         --select * from count_search_terms limit 100;
        ,
     cart_add_total_search as (select distinct a.search_term,
                                               b.count_search_term,
                                               a.sku,
                                               a.product,
                                               a.facility_id,
                                               sum(a.quantity) over (partition by a.sku,a.facility_id) as total_quantity
                               from match_search_cart_info a
                                        inner join count_search_terms b
                                                   on a.search_term = b.search_term and a.facility_id = b.facility_id
         --where date between '2022-11-01' and '2022-11-14'
     ),                                                                            --

     most_search as (select product, sku, '1' as most_search, total_estimated_search
                     from (select *,
                                  SUM(per_search)
                                      OVER (ORDER BY total_estimated_search DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acc_sum
                           from (select product,
                                        sku,
                                        round(sum(estimated_search_sku), 2)             as total_estimated_search,
                                        sum(total_estimated_search) over ()             as total_search,
                                        round(total_estimated_search / total_search, 4) as per_search
                                 from (select *,
                                              round(total_quantity / total_qty_ST, 2) as share,
                                              round(count_search_term * share, 2)     as estimated_search_sku
                                       from (select *,
                                                    sum(total_quantity) over (partition by search_term) as total_qty_ST
                                             from cart_add_total_search))
                                 group by 1, 2
                                 order by total_estimated_search desc))
                     where acc_sum <= 0.8)


    /*
     AQUI TERMINA ESE APARTADO
     --------------------------------------------------------------------------------------------------

     */

SELECT bp.*, coalesce(ms.most_search, 0) as most_search, ms.total_estimated_search
       FROM
(
select distinct order_day,
                sku_final,
                sku_pre,
                category,
                sku_description,
                concat(sku_description,' (', sku_final,')') sku_util,
                family,
                brand,
                cluster,
                in_promo,
                factor_sku,
                regular_price,
                promo_campaign,
                flag_nbo,
                ABC_REGULAR_NOT_PROMO,
                flag_pack,
                is_Combo,
                unidades,
                venta,
                precio_mes_sku,
                ratio,
                venta_pl,
                costo_pl,
                venta_regular_simulada,
                precio_unitario_actual,
                precio_regular_final,
                costo_unitario_actual,
                case when precio_regular_simulado > precio_regular_final then precio_regular_final else precio_regular_simulado end  as precio_regular_simulado,
                venta_regular,
                descuento_favo,
                descuento_FAVO_neto_valorizado,
                PRECIO_REGULAR_VEA,
                PRECIO_REGULAR_tottus,
                PRECIO_actual_vea,
                PRECIO_actual_tottus,
                precio_tarjeta_vea,
                precio_tarjeta_tottus,
                venta_vea_regular,
                venta_VEA_actual,
                venta_VEA_tarjeta,
                venta_tottus_regular,
                venta_tottus_actual,
                venta_tottus_tarjeta,
                dcto_vea_actual,
                dcto_vea_tarjeta,
                dcto_tottus_actual,
                dcto_tottus_tarjeta,
                indice_dcto_actual_vea,
                indice_dcto_tarjeta_vea,
                indice_dcto_actual_tottus,
                indice_dcto_tarjeta_tottus,
                precio_anterior_lw,
                precio_anterior_lm,
                costo_anterior_lw,
                costo_anterior_lm,
                round(precio_anterior_lw* unidades,2) as valorizado_LW,
                round(precio_anterior_lm* unidades,2) as valorizado_LM,
                round(costo_anterior_lw* unidades,2) as costo_valorizado_LW,
                round(costo_anterior_lm* unidades,2) as costo_valorizado_LM,
                MONTH(order_day) as month,
                week(order_day) as week
    from (
            select base.*, inf.ORDER_DAY as COMPARISON_DAY,
                   last_value(case when inf.order_day <= DATEADD(day , -7,base.ORDER_DAY)
                       then inf.precio_unitario_actual else null end)
                       ignore nulls over (partition by  base.sku_final, inf.Cluster,inf.flag_pack, inf.IS_COMBO order by inf.ORDER_DAY) precio_anterior_LW,
                    last_value(case when inf.order_day <= DATEADD(month , -1,base.ORDER_DAY)
                        then inf.precio_unitario_actual else null end)
                        ignore nulls over (partition by  base.sku_final, inf.Cluster,inf.flag_pack, inf.IS_COMBO order by inf.ORDER_DAY) precio_anterior_LM,

                    last_value(case when inf.order_day <= DATEADD(day , -7,base.ORDER_DAY)
                       then inf.costo_unitario_actual else null end)
                       ignore nulls over (partition by  base.sku_final, inf.Cluster,inf.flag_pack, inf.IS_COMBO order by inf.ORDER_DAY) costo_anterior_LW,

                    last_value(case when inf.order_day <= DATEADD(month , -1,base.ORDER_DAY)
                           then inf.costo_unitario_actual else null end)
                           ignore nulls over (partition by  base.sku_final, inf.Cluster,inf.flag_pack, inf.IS_COMBO order by inf.ORDER_DAY) costo_anterior_LM
            from base_competencia as base
            JOIN base_favo_2 as inf
            ON base.sku_final = inf.sku_final and inf.Cluster = base.Cluster and inf.flag_pack = base.flag_pack and inf.IS_COMBO = base.IS_COMBO and inf.ORDER_DAY between dateadd(day, -60, base.ORDER_DAY) and base.ORDER_DAY
            ORDER BY base.sku_final, base.ORDER_DAY desc, inf.ORDER_DAY Desc, base.Cluster
            )
    WHERE ORDER_DAY >= '2022-01-01'
    order by order_day, sku_final
    ) as bp
    LEFT JOIN most_search as ms
    ON bp.sku_final = ms.sku
    order by order_day, sku_final
    );



SELECT * FROM PRICING_COMPARISON
where sku_final = '2880';