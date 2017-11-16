SELECT
  user,
  truncate(AVG(interval)) interval_average,
  truncate((1000 / AVG(interval)) * 60) minuts,
  COUNT(*) total
FROM
  (
    SELECT
      user,
      info.key,
      date_diff('millisecond', info.timestamp, LEAD(info.timestamp, 1) OVER (PARTITION BY user ORDER BY info.timestamp)) AS interval
    FROM
      itsolution.key_history_json
    WHERE
       info.keyStatus = 'Down'
  ) diff
WHERE
  10 <= diff.interval
  AND  diff.interval <= 1000
GROUP BY
  user
ORDER BY
  user