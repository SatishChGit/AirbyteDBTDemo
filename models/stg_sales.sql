{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    tags = [ "top-level-intermediate" ]
) }}
select
    CAST (_airbyte_data.JSONExtract('$.Description')  AS VARCHAR(2000))  as Description,
    CAST (_airbyte_data.JSONExtract('$.Country')   AS VARCHAR(200)) as Country,
     CAST (_airbyte_data.JSONExtract('$.Item Type')    AS VARCHAR(200))  as ItemType,
     CAST (_airbyte_data.JSONExtract('$.Sales Channel')  AS VARCHAR(200)) as SalesChannel,
	 CAST (_airbyte_data.JSONExtract('$.Order Priority')  AS VARCHAR(5)) as OrderPriority,
	 CAST (_airbyte_data.JSONExtract('$.Order Date')  AS VARCHAR(200)) as OrderDate,
	 CAST (_airbyte_data.JSONExtract('$.Order ID')  AS VARCHAR(200)) as OrderID,
	 CAST (_airbyte_data.JSONExtract('$.Units Sold')  AS VARCHAR(200)) as UnitsSold,
	 CAST (_airbyte_data.JSONExtract('$.Unit Price')  AS VARCHAR(200)) as UnitPrice,
	 CAST (_airbyte_data.JSONExtract('$.Unit Cost')  AS VARCHAR(200)) as UnitCost,
	 CAST (_airbyte_data.JSONExtract('$.Total Revenue')  AS VARCHAR(200)) as TotalRevenue,
	 CAST (_airbyte_data.JSONExtract('$.Total Cost')  AS VARCHAR(200)) as TotalCost,
	 CAST (_airbyte_data.JSONExtract('$.Total Profit')  AS VARCHAR(200)) as TotalProfit,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('demo_sales', '_airbyte_raw_sales') }} as table_alias
-- data_stream
where 1 = 1
