SELECT
    user
    , array_join(array_agg(word), ',')
    , array_join(array_agg(frequency), ',')
FROM
    (
        SELECT
            user
            , word
            , frequency
            , rank() OVER(PARTITION BY user ORDER BY frequency DESC) AS rank
        FROM
            (
                SELECT
                    user
                    , word
                    , COUNT(*) AS frequency
                FROM
                    (
                        SELECT
                            user
                            , regexp_extract_all(array_join(array_agg(IF(info.key IN ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','OemMinus'), IF(info.key = 'OemMinus', '-', info.key), ' ')), ''), '\S+') AS words
                        FROM
                            itsolution.key_history_json
                        WHERE
                            info.keyStatus = 'Down'
                        GROUP BY
                            user
                    )
                    CROSS JOIN UNNEST(words) AS t(word)
                WHERE
                    LENGTH(word) > {{ $lowerLimitLength }}
                GROUP BY
                    user
                    , word
            )
    )
WHERE
    rank < 10
GROUP BY
    user
ORDER BY
    user