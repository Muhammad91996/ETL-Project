######## test
SELECT
  DATE(i.transaction_date) AS trx_date,
  i.main_category,
  i.sub_category,
  CASE 
  WHEN i.seller_name = "raneen" THEN "Raneen"
  WHEN i.seller_name != "raneen" THEN "MarketPlace"
  ELSE i.seller_name
  END AS "Seller_name",
  COUNT(*) AS items_cnt,
  COUNT(DISTINCT i.order_number) AS orders_cnt,
  SUM(i.quantity) AS qty_sum,
  SUM(i.total_price) AS revenue_sum,
  SUM(i.item_discount) AS discount_sum,
    CASE 
  WHEN i.seller_name = "raneen" THEN SUM(i.retail_margin)
  WHEN i.seller_name != "raneen" THEN SUM(i.marketplace_commission_amount)
  END AS "margin_sum"
FROM orders_items_denorm i
WHERE i.transaction_date >= '2026-01-01 00:00:01'
GROUP BY
  DATE(i.transaction_date),
  i.main_category,
  i.sub_category,
  i.seller_name
ORDER BY trx_date ASC;