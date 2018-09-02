



----------------***************---------------------
WITH Vod_Account_Joied_Table as((select aa.acc_bb_acct_id,vod.event_timestamp_tz,vod.playback_end_time,vod.playback_start_time,vod.content_id from summary.dly_base_vod_usage_vw vod
                        JOIN atm_ev.dim_account_vw  aa
                         ON vod.bb_acct_id = aa.acc_bb_acct_id
                        --JOIN  summary.dim_content_detail con on vod.content_id = con.content_id
                                where aa.con_contact_activation_date > dateadd(day, -180, CURRENT_DATE)
                                )),

    Single_Values_Table as  (SELECT acc_bb_acct_id,
                        DATE(event_timestamp_tz)                                 as activity_date,
                        (playback_end_time - playback_start_time)            as played_duration,
                        content_id,
                       -- (con.duration * 60)                                          as movie_length,
                         DATEPART(hour, event_timestamp_tz)                       AS hour_day,
                       (DATEPART(weekday, DATE(event_timestamp_tz)) + 1) % 7    AS threshold,
                        case WHEN threshold < 2 then 1 ELSE 0 END                    as "Weekend",
                        case WHEN threshold >= 2 then 1 ELSE 0 END                   as "Weekday",
                        case when hour_day >= 6 and hour_day < 12 then 1 ELSE 0 END  as "Morning",
                        case when hour_day >= 12 and hour_day < 16 then 1 ELSE 0 END as "Afternoon",
                        case when hour_day >= 16 and hour_day < 19 then 1 ELSE 0 END as "Evening",
                        case when hour_day >= 19 and hour_day < 22 then 1 ELSE 0 END as "Prime_Time",
                        case when hour_day >= 22 or hour_day < 6 then 1 ELSE 0 END   as "Night"
                        from Vod_Account_Joied_Table

               group by acc_bb_acct_id,
                        event_timestamp_tz,
                        content_id,
                        playback_start_time,
                        playback_end_time),
    Content_Wise_Values_Table as (select acc_bb_acct_id,
                                                                                                                                              activity_date,
                                                                                                                                              content_id,
                                                                                                                                             -- movie_length,
                                                                                                                                              Weekend,
                                                                                                                                              Weekday,
                                                                                                                                              Morning,
                                                                                                                                              Afternoon,
                                                                                                                                              Evening,
                                                                                                                                              Prime_Time,
                                                                                                                                              Night,
                                                                                                                                              extract(minute from sum(played_duration))    as play_duration_bytime,
                                                                                                                                              rank() over (PARTITION BY acc_bb_acct_id, activity_date, content_id ORDER BY play_duration_bytime DESC) as rank
                                                                                                                                       from Single_Values_Table


                                                                                                                                       group by acc_bb_acct_id,
                                                                                                                                                activity_date,
                                                                                                                                               -- movie_length,
                                                                                                                                                Weekend,
                                                                                                                                                Weekday,
                                                                                                                                                Morning,
                                                                                                                                                Afternoon,
                                                                                                                                                Evening,
                                                                                                                                                Prime_Time,
                                                                                                                                                Night,
                                                                                                                                                content_id)
select acc_bb_acct_id,activity_date,SUM(Weekend) AS weekend,SUM (Weekday) AS weekday,SUM (Morning) AS morning,SUM (Afternoon) as afternoon,SUM(Evening) as evening,SUM(Prime_Time) as prime_time,SUM(Night) as night from Content_Wise_Values_Table

--where acc_bb_acct_id='180510052720509274675615'
group by acc_bb_acct_id,activity_date,rank
having rank = 1;


