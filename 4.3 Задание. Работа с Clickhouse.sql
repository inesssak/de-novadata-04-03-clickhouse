
-- Создаем таблицу для хранения сырых логов событий
DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events (
	user_id UInt32 Nullable,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time::DATE + INTERVAL 30 DAY -- срок хранения 30 дней
;

-- Создаем агрегированную таблицу для трендового анализа
DROP TABLE IF EXISTS agg_events;
CREATE TABLE agg_events (
	event_date Date,
	event_type String,
	unique_users AggregateFunction(uniq, UInt32), -- уникальные пользователи
	total_spent AggregateFunction(sum, UInt32), -- сумма потраченных баллов
	total_actions AggregateFunction(count, UInt32) -- количество действий
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY -- срок хранения 180 дней
;

-- Создаем MV, которая при вставке данных в таблицу сырых логов событий будет обновлять агрегированную таблицу, используя sumState, uniqState, countState
CREATE MATERIALIZED VIEW agg_events_mv TO agg_events AS
SELECT
	event_time::DATE as event_date,
	event_type,
	uniqState(user_id) as unique_users,
	sumState(points_spent) as total_spent,
	countState() as total_actions
FROM user_events
GROUP BY
	event_time::DATE,
	event_type
;

-- Вставляем значения
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),

(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),

(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


-- Запрос, показывающий, сколько пользователей вернулись в течение следующих 7 дней
-- Формат результата: total_users_day_0|returned_in_7_days|retention_7d_percent|
with uniq_event_dates as (
	select
		distinct
		user_id,
		event_time::DATE event_date
	from user_events
),
first_event_date as (
	select
		user_id,
		min(event_date) as first_date
	from uniq_event_dates
	group by user_id
)
select
	fed.first_date as cohort_date,
	countDistinct(fed.user_id) as total_users_day_0,
	countDistinctIf(ued.user_id, ued.user_id != 0) as returned_in_7_days,
	countDistinctIf(ued.user_id, ued.user_id != 0)/countDistinct(fed.user_id)*100 AS retention_7d_percent
from first_event_date fed
left join uniq_event_dates ued
	on ued.user_id = fed.user_id
	and ued.event_date > fed.first_date
	and ued.event_date <= fed.first_date + INTERVAL 7 DAY
group by fed.first_date
;

-- Запрос с группировками по быстрой аналитике по дням
SELECT
	event_date,
	event_type,
	uniqMerge(unique_users) unique_users,
	sumMerge(total_spent) total_spent,
	countMerge(total_actions) total_actions
 from agg_events
 group by
 	event_date,
	event_type
;
