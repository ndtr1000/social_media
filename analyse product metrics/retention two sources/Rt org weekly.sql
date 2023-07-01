SELECT action AS action,
       first_action AS first_action,
       min(rt) AS "MIN(rt)"
FROM
  (select toString(first_action) as first_action,
          toString(action) as action,
          count(user_id) as rt
   from
     (select distinct user_id,
                      min(toMonday(time)) over(partition by user_id) as first_action,
                      toMonday(time) as action
      from simulator_20230520.feed_actions
      where source='organic' ) as t1
   group by first_action,
            action
   order by first_action,
            action) AS virtual_table
GROUP BY action,
         first_action
