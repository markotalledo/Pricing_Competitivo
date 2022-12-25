CREATE OR REPLACE VIEW SNP_SANDBOX.PRICING_COMPARISON AS (
WITH cruce_scrapping as (
        -- Se cruzan las tablas de scrapping de VEA Y TOTTUS con las tabla de SKUS de productos NBO de FAVO
        select pes.SKU_FAVO,
                 pes.DESC_FAVO,
                 pes.COMPETENCIA,
                 -- se crean columnas dummy para hacer el pivot
                 concat(pes.COMPETENCIA,'1') As COMPETENCIA1,
                 concat(pes.COMPETENCIA,'2') As COMPETENCIA2,
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
          order by pes.SKU_FAVO, EFFECTIVE_DAY
),

pivot_scrapping as (select *
    -- se pivotea la tabla para generar una columna por cada precio de la competencia
      from cruce_scrapping
               pivot ( MAX(PRECIO_REGULAR) for COMPETENCIA in ('Vea', 'Tottus'))
               as p
               pivot ( MAX(PRECIO_ACTUAL) for COMPETENCIA1 in ('Vea1', 'Tottus1'))
               as p2
               pivot ( MAX(PRECIO_TARJETA) for COMPETENCIA2 in ('Vea2', 'Tottus2'))
               as p3 (SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY, PRECIO_REGULAR_VEA, PRECIO_REGULAR_TOTTUS,
                      PRECIO_ACTUAL_VEA, PRECIO_ACTUAL_TOTTUS, PRECIO_TARJETA_VEA, PRECIO_TARJETA_TOTTUS)
      order by SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY
      ),

scrapping_final as (
        -- para eliminar duplicados y entradas nulas con precios, se agrupa por dia y sku
        SELECT SKU_FAVO, DESC_FAVO, EFFECTIVE_DAY,
               MAX(PRECIO_REGULAR_VEA) AS PRECIO_REGULAR_VEA, MAX(PRECIO_REGULAR_TOTTUS) AS PRECIO_REGULAR_TOTTUS,
               MAX(PRECIO_ACTUAL_VEA) AS PRECIO_ACTUAL_VEA, MAX(PRECIO_ACTUAL_TOTTUS) AS PRECIO_ACTUAL_TOTTUS,
               MAX(PRECIO_TARJETA_VEA) AS PRECIO_TARJETA_VEA, MAX(PRECIO_TARJETA_VEA) AS PRECIO_TARJETA_TOTTUS
        FROM pivot_scrapping
        GROUP BY 1,2,3
        ORDER BY 1,2,3),

promo_acc as (
        -- cruce con la tabla de promos
        select distinct cast(p.date as date) as date, country, state, sku,
            first_value(campaign) over (partition by date, country, state, sku order by date ) as campaign,
            first_value(regular_price) over (partition by date, country, state, sku order by date ) as regular_price
            from snp_untrusted.promo_tracking p
            where state in ('LIMA', 'TJL') and lower(sku) not like '%nuevo%' and lower(sku) not like '%crear%'
        -- usamos el primer valor de campaña y precio regular del día y usamos distinct para remover duplicados
	),

promo_acc2 as
        -- cruce con la tabla de promos
        (select distinct cast(effectiveday as date) as date, country, state, sku,
        first_value(campaignname) over (partition by effectiveday, country, state, sku order by effectiveday ) as campaign,
        cast(first_value(regularprice) over (partition by effectiveday, country, state, sku order by effectiveday ) as decimal) as regular_price
        from commercial_untrusted.offer_history
        ),


base_favo_1  as
        -- creamos la base principal del query donde estaran los atributos principales
        (SELECT
            CAST(base.CREATE_DATE_TIME_TZ AS DATE)                       as ORDER_DAY, -- dia de creacion de la orden
               coalesce(sku_conv.VALUE,base.sku)                         as sku_final, -- coalesce para asignar el valor de la otra tabla si resulta null
                base.SKU                                                 as sku_pre,
               coalesce(mc.category,ml.category,base.category)           as category,
               coalesce(mc.description,ml.description,base.description)  as sku_description,
               coalesce(mc.family,ml.family)                             as family,
               coalesce(mc.brand,ml.brand)                               as brand,
            IFF(ent.TIER LIKE 'mayorista%', 'Mayorista', 'No Mayorista')    Cluster,
            IFF(coalesce(acc.campaign, acc2.campaign) is not null, 1, 0) as in_promo,           -- Promo del Producto
            coalesce(acc.campaign,acc2.campaign,'sin_promo')             as promo_campaign,     -- Campaña de la promocion

            IFF(ss.NBO IS NULL, 0, 1)                                       flag_NBO,  -- Flag del producto si es NBO

            ss.ABC_REGULAR_NOT_PROMO,                                                  -- Curva ABC

            Case when sku_conv.SKU_PACK IS NULL and base.ITEM_PACK_SKU is null THEN 0 ELSE 1 END flag_pack,           -- El sku es un pack

            IFNULL(SKU_CONV.IS_COMBO, ifnull(sku_ip.IS_COMBO, 0))  AS IS_COMBO,                                 -- dentro del pack, hay mas de un sku

            IFNULL(sku_conv.QTY, ifnull(sku_ip.QTY, 1)) factor_sku,                                         -- factor de conversion del pack de sku a unitario sku
            coalesce(acc.regular_price, acc2.regular_price) as regular_price,          -- precio regular del sku

            SUM(QUANTITY * IFNULL(sku_conv.QTY,1)) as unidades,                        -- Unidades de SKU dentro de las ordenes creadas de ese dia

           SUM(case when coupon like any ('POC-MAY-%','MAY-%','POC-FAV-%','FAV-%') then net_value else gross_value end) venta,                  -- Si es que el sku tiene un cupon darle la venta neta, sino gross
           round(div0(SUM(case when flag_pack  = 0 then venta else 0 end) over (partition by sku_final, date_trunc('month', ORDER_DAY)),        --
           SUM(case when flag_pack  = 0 then unidades else 0 end) over (partition by sku_final, date_trunc('month', ORDER_DAY))),2) precio_mes_sku,


            round(SUM(base.NET_VALUE)/unidades,2)                        as precio_unitario_actual,

            round(precio_unitario_actual * 0.92,2) as                       precio_unitario_implicito,

            --CASE WHEN in_promo = 1 THEN coalesce(acc.regular_price, acc2.regular_price)
            --ELSE precio_unitario_actual END                                 precio_regular,

            round(SUM(base.GROSS_VALUE),2) as venta_gross,

            round(SUM(base.NET_VALUE),2) AS venta_net,

            round(coalesce(acc.regular_price, acc2.regular_price) * unidades,2) as venta_regular,

            round(coalesce(acc.regular_price, acc2.regular_price) - precio_unitario_actual,2) as descuento_FAVO,


            round(descuento_FAVO * unidades,2) as descuento_FAVO_neto_valorizado

        FROM JOURNEY.BASE as BASE
        LEFT JOIN FAVODATA.SNP_SANDBOX.PACKS_CONVERSION AS sku_conv
        ON base.SKU = sku_conv.SKU_PACK
        LEFT JOIN FAVODATA.SNP_SANDBOX.PACKS_CONVERSION AS sku_ip
        ON base.ITEM_PACK_SKU = sku_ip.SKU_PACK
        LEFT JOIN FAVODATA.SNP_SANDBOX.MAESTRA mc
            on mc.sku= coalesce(sku_conv.VALUE,base.sku)
        LEFT JOIN FAVODATA.SNP_SANDBOX.MAESTRA_LOGISTICA ml
            on ml.sku::varchar= coalesce(sku_conv.VALUE,base.sku)
        LEFT JOIN FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST as ent
            ON ent.DYNAMO_LEADER_ID = base.DYNAMO_LEADER_ID AND
               ent.CALENDAR_MONTH = DATEFROMPARTS(YEAR(base.CREATE_DATE_TIME_TZ),MONTH(base.CREATE_DATE_TIME_TZ),1)
        LEFT JOIN PROMO_ACC  as acc
            on acc.date = base.CREATE_DATE_TIME_TZ::date
                and acc.sku = coalesce(base.item_pack_sku,base.sku)
                and LOWER(base.CD) = LOWER(acc.state)
        LEFT JOIN PROMO_ACC2 as acc2
            on acc2.date= base.CREATE_DATE_TIME_TZ::date and acc2.sku=coalesce(base.item_pack_sku,base.sku) and
            LOWER(base.CD)= LOWER(acc2.state)
        LEFT JOIN favodata.snp_sandbox.ct_supply_stock as ss
            ON CAST(base.CREATE_DATE_TIME_TZ AS DATE) = ss.CALENDAR_DATE
            AND ss.SKU = coalesce(sku_conv.VALUE,base.sku)
            AND ss.FACILITY_NAME = base.CD
        LEFT JOIN scrapping_final as ps
            ON coalesce(sku_conv.VALUE,base.sku)   = ps.SKU_FAVO and CAST(base.CREATE_DATE_TIME_TZ AS DATE) = ps.EFFECTIVE_DAY



        WHERE base.COUNTRY = 'PE' AND IFNULL(BASE.CD,'LIMA') = 'LIMA' AND ORDER_STATUS != 'CANCEL'
        AND ORDER_DAY >= '2021-11-01'

        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, acc.regular_price, acc2.regular_price
        order by order_day asc
        ) ,

base_favo_2 as (
select *,
       ifnull(LAG(CASE WHEN in_promo = 0 THEN precio_unitario_actual else null end) ignore nulls over (partition by sku_final, Cluster order by order_day),
           IFNULL(LEAD(CASE WHEN in_promo = 0 THEN precio_unitario_actual else null end) ignore nulls over (partition by sku_final, Cluster order by order_day), precio_regular_final)) precio_regular_simulado,
    precio_regular_simulado * unidades as venta_regular_simulada
       from(
SELECT
    ORDER_DAY,
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
round(venta,2) as venta, -- venta sin cupones seleccionados
precio_mes_sku,
case when IS_COMBO = 1 then
            div0(precio_mes_sku*factor_sku,sum(precio_mes_sku*factor_sku) over (partition by sku_pre, ORDER_DAY, Cluster))
            else 1 end ratio,

            round(venta * ratio,2) as venta_PL, --venta considerando el factor de conversion y ratio de proporcion de venta de pack  sku
round(venta_PL/unidades,2)                        as precio_unitario_actual,

CASE WHEN in_promo = 1 THEN div0(regular_price*ratio, factor_sku) else round(venta_PL/unidades,2) end precio_regular_final,


            round(precio_regular_final * unidades,2) as venta_regular,

            round(precio_regular_final - precio_unitario_actual,2) as descuento_FAVO,


            round(descuento_FAVO * unidades,2) as descuento_FAVO_neto_valorizado
FROM base_favo_1

)),

base_competencia as (
        SELECT favo.*, comp.PRECIO_REGULAR_VEA, comp.PRECIO_REGULAR_TOTTUS,comp.PRECIO_ACTUAL_VEA,comp.PRECIO_ACTUAL_TOTTUS,
               comp.PRECIO_TARJETA_VEA, comp.PRECIO_TARJETA_TOTTUS,
               round(comp.PRECIO_REGULAR_VEA * favo.unidades,2) as venta_VEA_regular,
               round(comp.PRECIO_ACTUAL_VEA * favo.unidades,2) as venta_VEA_actual,
               round(comp.PRECIO_TARJETA_VEA * favo.unidades,2) as venta_VEA_tarjeta,
               round(comp.PRECIO_REGULAR_TOTTUS * favo.unidades,2) as venta_TOTTUS_regular,
               round(comp.PRECIO_ACTUAL_TOTTUS * favo.unidades,2) as venta_TOTTUS_actual,
               round(comp.PRECIO_TARJETA_TOTTUS * favo.unidades,2) as venta_TOTTUS_tarjeta,
               round(comp.PRECIO_REGULAR_VEA - comp.PRECIO_ACTUAL_VEA, 2) as dcto_VEA_actual,
               round(comp.PRECIO_REGULAR_VEA - comp.PRECIO_TARJETA_VEA, 2) as dcto_VEA_tarjeta,
               round(comp.PRECIO_REGULAR_TOTTUS - comp.PRECIO_ACTUAL_TOTTUS, 2) as dcto_TOTTUS_actual,
               round(comp.PRECIO_REGULAR_TOTTUS - comp.PRECIO_TARJETA_TOTTUS, 2) as dcto_TOTTUS_tarjeta,
               1 - (round(favo.descuento_FAVO / NULLIF(round(comp.PRECIO_REGULAR_VEA - comp.PRECIO_ACTUAL_VEA, 2),0),2)) as indice_dcto_actual_vea,
               1 - (round(favo.descuento_FAVO / NULLIF(round(comp.PRECIO_REGULAR_VEA - comp.PRECIO_TARJETA_VEA, 2),0),2)) as indice_dcto_tarjeta_vea,
               1 - (round(favo.descuento_FAVO / NULLIF(round(comp.PRECIO_REGULAR_TOTTUS - comp.PRECIO_ACTUAL_TOTTUS, 2),0),2)) as indice_dcto_actual_tottus,
               1 - (round(favo.descuento_FAVO / NULLIF(round(comp.PRECIO_REGULAR_TOTTUS - comp.PRECIO_TARJETA_TOTTUS, 2),0),2)) as indice_dcto_tarjeta_tottus
                -- 0 en dscto es cuando el dcto favo es 0
                -- null en dscto es cuando no existe descuento en tootus o vea, o son 0
                -- Si el indice es menor a 0, implica que el dcto de favo es *indice*% menor que el descuento de competencia [menor descuento]
                -- Si el indice es mayor a 0, implica que el dcto de faco es *indice*% mayor que el descuento de competencia [mayor descuento]
        FROM base_favo_2 as favo
        LEFT JOIN scrapping_final as comp
        ON favo.ORDER_DAY = comp.EFFECTIVE_DAY and favo.sku_final = comp.SKU_FAVO
        -- WHERE comp.SKU_FAVO is not null                                         -- este filtro es para solo mostrar los match del cruce
        ORDER BY 2,1)


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
                venta_regular_simulada,
                precio_unitario_actual,
                precio_regular_final,
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
                round(precio_anterior_lw* unidades,2) as valorizado_LW,
                round(precio_anterior_lm* unidades,2) as valorizado_LM,
                MONTH(order_day) as month,
                week(order_day) as week
    from (
            select base.*, inf.ORDER_DAY as COMPARISON_DAY,
                   last_value(case when inf.order_day <= DATEADD(day , -7,base.ORDER_DAY) then inf.precio_unitario_actual else null end) ignore nulls over (partition by  base.sku_final, inf.Cluster,inf.flag_pack, inf.IS_COMBO order by inf.ORDER_DAY) precio_anterior_LW,
                    last_value(case when inf.order_day <= DATEADD(month , -1,base.ORDER_DAY) then inf.precio_unitario_actual else null end) ignore nulls over (partition by  base.sku_final, inf.Cluster,inf.flag_pack, inf.IS_COMBO order by inf.ORDER_DAY) precio_anterior_LM
            from base_competencia as base
            JOIN base_favo_2 as inf
            ON base.sku_final = inf.sku_final and inf.Cluster = base.Cluster and inf.flag_pack = base.flag_pack and inf.IS_COMBO = base.IS_COMBO and inf.ORDER_DAY between dateadd(day, -60, base.ORDER_DAY) and base.ORDER_DAY
            ORDER BY base.sku_final, base.ORDER_DAY desc, inf.ORDER_DAY Desc, base.Cluster
            )
    WHERE ORDER_DAY >= '2022-01-01'
    order by order_day, sku_final);


select * from journey.base limti 10;






