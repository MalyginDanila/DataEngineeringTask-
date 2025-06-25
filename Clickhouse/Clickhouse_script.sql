CREATE TABLE user_events (
  user_id UInt32,
  event_type String,
  points_spent UInt32,
  event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY DELETE;

CREATE TABLE user_events_agg (
  event_date Date,
  event_type String,
  user_unique AggregateFunction(uniq, UInt32),
  points_spent_sum AggregateFunction(sum, UInt32),
  actions_count AggregateFunction(count, UInt32)
) ENGINE = SummingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW user_events_mv TO user_events_agg AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS user_unique,
    sumState(points_spent) AS points_spent_sum,
    countState(*) AS actions_count
FROM user_events
GROUP BY event_date, event_type;

WITH
-- Пользователи первого дня (day0)
day0_users AS (
    SELECT
        toDate(event_time) AS event_date,
        user_id
    FROM user_events
    GROUP BY event_date, user_id
),

-- Пользователи, активные в следующие 7 дней после day0
next7days_users AS (
    SELECT
        d0.event_date AS day0,
        ue.user_id
    FROM day0_users d0
    JOIN user_events ue ON ue.user_id = d0.user_id
        AND toDate(ue.event_time) > d0.event_date
        AND toDate(ue.event_time) <= d0.event_date + INTERVAL 7 DAY
    GROUP BY day0, ue.user_id
),

-- Считаем количество уникальных пользователей в day0 и вернувшихся в 7 дней
retention AS (
    SELECT
        day0,
        uniqExact(user_id) AS total_users_day_0,
        (SELECT uniqExact(user_id) FROM next7days_users WHERE day0 = r.day0) AS returned_in_7_days
    FROM day0_users r
    GROUP BY day0
)

SELECT
    day0 AS total_users_day_0_date,
    total_users_day_0,
    returned_in_7_days,
    roundIf(total_users_day_0 > 0, returned_in_7_days * 100.0 / total_users_day_0, 0) AS retention_7d_percent
FROM retention
ORDER BY total_users_day_0_date;

SELECT
    event_date,
    event_type,
    uniqMerge(user_unique) AS unique_users,
    sumMerge(points_spent_sum) AS total_spent,
    countMerge(actions_count) AS total_actions
FROM user_events_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
