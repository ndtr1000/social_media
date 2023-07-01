SELECT action AS action,
       first_action AS first_action,
       min(rt) AS "MIN(rt)"
FROM
  (select toString(first_action) as first_action,
          toString(action) as action,
          week_number,
          rt
   from
     (select first_action,
             action,
             toInt64((action - first_action) / 7) as week_number,
             count(user_id) as rt
      from
        (select distinct user_id,
                         min(toMonday(time)) over(partition by user_id) as first_action,
                         toMonday(time) as action
         from simulator_20230520.feed_actions
         where source='ads' ) as t1
      group by first_action,
               action) as t2
   order by first_action,
            action) AS virtual_table
GROUP BY action,
         first_action
LIMIT 1000;