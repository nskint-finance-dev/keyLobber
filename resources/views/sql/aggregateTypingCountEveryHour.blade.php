SELECT
  user
  , array_join(array_agg(hour), ',') AS hour
  , array_join(array_agg(cnt), ',') AS cnt
FROM
  (
    SELECT
      user
      , format_datetime(info.timestamp,'H') AS hour
      , COUNT(*) AS cnt
    FROM
      itsolution.key_history_json
    GROUP BY
      user
      , format_datetime(info.timestamp,'H')
  )
GROUP BY
  user
ORDER BY
  user