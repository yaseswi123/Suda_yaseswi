WITH CTE_USERS_TRADE AS 
(select 
CAST(t.open_time AS DATE) as date_report,
u.login_hash,
u.server_hash,
u.country_hash,
t.symbol,
u.currency,
 date_part('month', open_time),
 date_part('year', open_time),
 t.open_time
from public.users u inner join public.trades t on 
u.login_hash=t.login_hash
AND u.server_hash=t.server_hash)
--AND u.country_hash=t.country_hash) 
SELECT
id,
CAST(date_report AS DATE),
CAST(login_hash AS VARCHAR),
CAST(server_hash AS VARCHAR),
CAST(symbol AS VARCHAR),
CAST(currency AS VARCHAR),
CAST(sum_prev_prev_7d AS DOUBLE),
CAST(sum_prev_prev_all AS DOUBLE),
CAST(rank_volume_symbol_prev_7d AS INT),
CAST(rank_count_prev_7d AS INT),
CAST(sum_volume_2020_08 AS DOUBLE),
CAST(date_first_trade AS TIMESTAMP),
CAST(row_number AS int) from (
SELECT  
row_number() over(order  by CTE_USERS_TRADE.date_report , CTE_USERS_TRADE.login_hash,CTE_USERS_TRADE.server_hash,CTE_USERS_TRADE.country_hash,
				  CTE_USERS_TRADE.symbol,CTE_USERS_TRADE.currency, CTE_USERS_TRADE. Month,CTE_USERS_TRADE.Year, CTE_USERS_TRADE.open_time)as id,
CTE_USERS_TRADE.date_report as date_report,
CTE_USERS_TRADE.login_hash,
CTE_USERS_TRADE.server_hash,
CTE_USERS_TRADE.country_hash,
CTE_USERS_TRADE.symbol,
CTE_USERS_TRADE.currency,
sum(CTE_USERS_TRADE.volume) 
over(partition by login_hash,server_hash,symbol order by CTE_USERS_TRADE.date_report ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
AS sum_prev_prev_7d,

sum(volume) 
over(partition by login_hash,server_hash,symbol order by CTE_USERS_TRADE.date_report ROWS BETWEEN  UNBOUNDED PRECEDING AND CURRENT ROW)
AS sum_prev_prev_all,

dense_rank() over(partition by login_hash,Symbol order by volume desc ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
AS rank_volume_symbol_prev_7d,

rank() over (Partition  by (count(symbol) over( partition by login_hash) )order by date_report ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
AS  rank_count_prev_7d,

case when Month='08'  and Year='2020' then sum(volume) 
over (partition by login,server,symbol,Month,Year ORDER BY date_report ROWS BETWEEN UNBOUNDED PRECEDING AND  CURRENT ROW ) else 0 end  as sum_volume_2020_08,

min(open_time) over(partition by login_hash,server_hash,symbol order by date_report ) as date_first_trade,

row_number() over(order by date_report,login_hash,server_hash,symbol) as row_number
from CTE_USERS_TRADE) S