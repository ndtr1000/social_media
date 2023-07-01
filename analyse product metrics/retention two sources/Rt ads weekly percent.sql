SELECT action AS action,
       first_action AS first_action,
       min(percent_rt) AS "MIN(percent_rt)"
FROM
  (select first_action,
          action,
          round(rt*100/max_rt, 1) as percent_rt
   from
     (select toString(first_action) as first_action,
             toString(action) as action,
             rt,
             max(rt) over(partition by first_action) as max_rt
      from
        (select first_action,
                action,
                count(user_id) as rt
         from
           (select distinct user_id,
                            min(toMonday(time)) over(partition by user_id) as first_action,
                            toMonday(time) as action
            from simulator_20230520.feed_actions
            where source='ads' ) as t1
         group by first_action,
                  action) as t2) as t3) AS virtual_table
GROUP BY action,
         first_action
LIMIT 1000;