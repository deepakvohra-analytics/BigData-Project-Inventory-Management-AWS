-- test Athena with train table
CREATE TABLE train
(
    Semana            VARCHAR(20),
    Agencia_ID        VARCHAR(20),
    Canal_ID          VARCHAR(20),
    Ruta_SAK          VARCHAR(20),
    Cliente_ID        VARCHAR(20),
    Producto_ID       VARCHAR(20),
    Venta_uni_hoy     VARCHAR(20),
    Dev_uni_proxima   VARCHAR(20),
    Dev_proxima       VARCHAR(20),
    Venta_hoy         VARCHAR(20),
    Demanda_uni_equil VARCHAR(20)
);


/*big table*/
CREATE TABLE bimbo.bimbo_sales
  WITH (
      FORMAT='TEXTFILE',
      external_location='s3://query-results-bucket-bimbo/new_table/'
  ) AS
SELECT (t.semana - 2) AS week,
       t.cliente_id AS client_id,
       c.nombrecliente AS client,
       t.producto_id AS product_id,
       p.product_name AS product,
       t.agencia_id AS agency_id,
       town.town,
       town.state,
       t.canal_id AS channel_id,
       t.Venta_uni_hoy AS sales_unit,
       t.Venta_hoy AS sales,
       t.Dev_uni_proxima AS return_unit,
       t.dev_proxima AS return_value
  FROM train t
       JOIN cliente_tabla c USING (cliente_id)
       JOIN product p USING (producto_id)
       JOIN town_state town USING (agencia_id);


/*After crawling table of `forecast` located in S3 using AWS Glue*/
INSERT INTO bimbo_sales
SELECT (f.semana - 2) AS week,
       f.cliente_id AS client_id,
       c.nombrecliente AS client,
       f.producto_id AS product_id,
       p.product_name AS product,
       f.agencia_id AS agency_id,
       town.town,
       town.state,
       f.canal_id AS channel_id,
       f.demanda_uni_equil AS sales_unit,
       NULL AS sales,
       NULL AS return_unit,
       NULL AS return_value
  FROM forecast f
       JOIN cliente_tabla c USING (cliente_id)
       JOIN product p USING (producto_id)
       JOIN town_state town USING (agencia_id);


CREATE OR REPLACE VIEW "weekly_sales" AS
SELECT week,
       sales_weekly,
       (sales_weekly / nullif(lag(sales_weekly) OVER (ORDER BY week), 0)) -
       1 AS sales_change,
       units_weekly,
       (units_weekly / nullif(lag(units_weekly) OVER (ORDER BY week), 0)) -
       1 AS units_change,
       return_value_weekly,
       (return_value_weekly /
        nullif(lag(return_value_weekly, 1) OVER (ORDER BY week), 0)) -
       1 AS return_value_change,
       return_units_weekly,
       (return_units_weekly /
        nullif(lag(return_units_weekly, 1) OVER (ORDER BY week), 0)) -
       1 AS return_units_change
  FROM (SELECT week,
               sum(sales) AS sales_weekly,
               sum(sales_unit) AS units_weekly,
               sum(return_value) AS return_value_weekly,
               sum(return_unit) AS return_units_weekly
          FROM bimbo_sales
         GROUP BY week
         ORDER BY week) tbl;
