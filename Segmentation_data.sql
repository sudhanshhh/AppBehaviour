

 ---------------***********************------------------
 -----------------App Behaviour--------------------------
 ---------------***********************------------------

                      select
  (row_number() over (order by true))::int as n
into numbers
from atomic.atm_segment_events_all
limit 100;

     WITH Segmentevents_Account_Joied_Table AS (select aa.acc_bb_acct_id , seg.event_date AS activity_date,seg.event_name,seg.source_event_name,seg.properties_filter_content_type,seg.properties_filter_genre
                                        from atomic.atm_segment_events_all  seg
                                         JOIN atm_ev.dim_account_vw aa ON seg.bb_acct_id = aa.acc_bb_acct_id
                                        WHERE aa.con_contact_activation_date >DATEADD(DAY, -180, CURRENT_DATE)),

   Filter_Type AS (
    SELECT distinct acc_bb_acct_id,activity_date, properties_filter_content_type
                                                                      from (select acc_bb_acct_id,activity_date,
                                                                                  first_value(properties_filter_content_type) over (PARTITION BY acc_bb_acct_id ORDER BY count(*) DESC rows between unbounded preceding and unbounded following) as properties_filter_content_type
                                                                           from Segmentevents_Account_Joied_Table
                                                                           where properties_filter_content_type NOTNULL
                                                                        and properties_filter_content_type <> ''
                                                                            group by acc_bb_acct_id,activity_date,properties_filter_content_type



                                                           )
    -- where acc_bb_acct_id='5cbf2011-e285-4f22-846e-c08af842bd1e'
     group by acc_bb_acct_id,activity_date,properties_filter_content_type

         ),

           Genre_Type AS (


                                                select distinct event_date, bb_acct_id,
                                                     first_value(properties_filter_genre) over (PARTITION BY bb_acct_id,event_date ORDER BY sum(count) DESC
                                                       rows between unbounded preceding and unbounded following
                                                       ) as properties_filter_genre from(
                                                         select count(*) as count,
                                                         bb_acct_id, event_date,
                                                         split_part(seg.properties_filter_genre, ',', n) as properties_filter_genre
                                                         from
                                                         atomic.atm_segment_events_all seg
                                                         cross join
                                                         numbers
                                                         where
                                                         split_part(seg.properties_filter_genre, ',', n) is not null
                                                         and split_part(seg.properties_filter_genre, ',', n) != ''
                                                         --AND bb_acct_id='0a9681c0-88eb-467d-8489-855d2849765b'
                                                         group by bb_acct_id, properties_filter_genre, n, event_date)
          group by event_date, bb_acct_id,properties_filter_genre

         ),

Single_value_Table as
(SELECT acc_bb_acct_id, activity_date,
              CASE WHEN event_name = 'page_load_appstartup' THEN  COUNT(*) ELSE NULL END AS "App_Login", -- No Of Times user logged in to app
              CASE WHEN event_name = 'page_load_discover' THEN  COUNT(*) ELSE NULL END AS "Discover", --No Of Times user loads Discover page
              CASE WHEN event_name = 'button_tap_applyfilter' THEN  COUNT(*) ELSE NULL END AS "Apply_Filter",
              CASE WHEN event_name = 'button_tap_castconnect' THEN  COUNT(*) ELSE NULL END AS "Chomecast_connect", --no of times liveTV connected
              CASE WHEN event_name = 'button_tap_channel' THEN  COUNT(*) ELSE NULL END AS "channel_liveTV", --No of times clicked on Channel from liveTV
              CASE WHEN event_name = 'button_tap_content_reco' THEN  COUNT(*) ELSE NULL END AS "clicked_recommended", --WHEN user clicks on any recommended titles in the content detail
              CASE WHEN event_name = 'button_tap_downloadstart' THEN  COUNT(*) ELSE NULL END AS "download", -- No of times clicked on download button
              CASE WHEN event_name = 'button_tap_downloadquality' THEN  COUNT(*) ELSE NULL END AS "download_quality", -- no of times changed quality
              CASE WHEN event_name = 'button_tap_favorite' THEN  COUNT(*) ELSE NULL END AS "favorite", --no of times selected favorite
              CASE WHEN event_name = 'button_tap_favoriteremove' THEN  COUNT(*) ELSE NULL END AS "remove_favorite",
              CASE WHEN event_name = 'button_tap_genre' THEN  COUNT(*) ELSE NULL END AS "button_tap_genre", --selected genre,maybe favourite genre are therefor engaged users
              CASE WHEN event_name = 'button_tap_paywall_plan' THEN  COUNT(*) ELSE NULL END AS "button_tap_paywall_plan", --user interested in byuing a plan
              CASE WHEN event_name = 'button_tap_playcollection' THEN  COUNT(*) ELSE NULL END AS "button_tap_playcollection", --clicks on any collection
              CASE WHEN event_name = 'button_tap_playdetails' THEN  COUNT(*) ELSE NULL END AS "play_content", --play content
              CASE WHEN event_name = 'button_tap_playdiscover' THEN  COUNT(*) ELSE NULL END AS "button_tap_playdiscover", --play from discover page
              CASE WHEN event_name = 'button_tap_playdownload' THEN  COUNT(*) ELSE NULL END AS "button_tap_playdownload", ----play from downloaded
              CASE WHEN event_name = 'button_tap_playepisode' THEN  COUNT(*) ELSE NULL END AS "button_tap_playepisode", --plays an episode
              CASE WHEN event_name = 'button_tap_playlastwatched' THEN  COUNT(*) ELSE NULL END AS "button_tap_playlastwatched", --WHEN the user clicks play button from last watched at the top of the feed
              CASE WHEN event_name = 'button_tap_rental_playdetails' THEN  COUNT(*) ELSE NULL END AS "button_tap_rental_playdetails", --play from TVOD
              CASE WHEN event_name = 'rental_purchase_success' THEN  COUNT(*) ELSE NULL END AS "rental_purchase", -- purchased TVOD
              CASE WHEN event_name = 'rental_purchase_cancel' THEN  COUNT(*) ELSE NULL END AS "rental_purchase_cancel",
              CASE WHEN event_name = 'button_tap_search' THEN  COUNT(*) ELSE NULL END AS "button_tap_search",
              CASE WHEN event_name = 'button_tap_start_trial' THEN  COUNT(*) ELSE NULL END AS "button_tap_start_trial", --should be 1 for one customer
              CASE WHEN event_name = 'button_tap_vouchercodesuccess' THEN  COUNT(*) ELSE NULL END AS "button_tap_vouchercodesuccess", --give an idea if customer might be interested in TVOD
              CASE WHEN event_name = 'button_tap_watchlater' THEN  COUNT(*) ELSE NULL END AS "button_tap_watchlater", --compare the count if watchlater is related with many content played
              CASE WHEN event_name = 'discover_10' THEN  COUNT(*) ELSE NULL END AS "discover_10",
              CASE WHEN event_name = 'discover_50' THEN  COUNT(*) ELSE NULL END AS "discover_50",
              CASE WHEN event_name = 'notification_download_error' THEN  COUNT(*) ELSE NULL END AS "notification_download_error",
              CASE WHEN event_name = 'over21_access' THEN  COUNT(*) ELSE NULL END AS "over21_access",
              CASE WHEN event_name = 'over21_no_access' THEN  COUNT(*) ELSE NULL END AS "over21_no_access",
              CASE WHEN event_name = 'Page_load_404' THEN  COUNT(*) ELSE NULL END AS "Page_load_404", --errors got in page load
              CASE WHEN event_name = 'page_load_profile' THEN  COUNT(*) ELSE NULL END AS "profile_load", --times user opened profile
              CASE WHEN event_name = 'page_load_browse' THEN  COUNT(*) ELSE NULL END AS "Browse",
              CASE WHEN event_name = 'page_load_livetv' THEN  COUNT(*) ELSE NULL END AS "LiveTV_load",
              CASE WHEN event_name = 'page_load_rental' THEN  COUNT(*) ELSE NULL END AS "TVOD_load",

              CASE WHEN source_event_name='button_tap_downloadstart' AND (event_name='error' OR event_name='status_activate_output_error'
                  OR event_name='notification_errormessage' OR event_name='status_empty_device_signature_error' OR event_name='notification_download_error'
                  OR event_name='status_downloaderror') THEN  COUNT(*) ELSE NULL END AS "download_click_error",

              CASE WHEN source_event_name='button_tap_playdetails' AND (event_name='error' OR event_name='status_activate_output_error'
                  OR event_name='notification_errormessage' OR event_name='status_empty_device_signature_error' OR event_name='notification_download_error'
                  OR event_name='status_downloaderror') THEN  COUNT(*) ELSE NULL END AS "playback_error",

              CASE WHEN source_event_name='button_tap_castconnect' AND (event_name='error' OR event_name='status_activate_output_error'
                  OR event_name='notification_errormessage' OR event_name='status_empty_device_signature_error' OR event_name='notification_download_error'
                  OR event_name='status_downloaderror') THEN  COUNT(*) ELSE NULL END AS "liveTV_click_error"

      from Segmentevents_Account_Joied_Table
      GROUP BY  acc_bb_acct_id, event_name,activity_date,source_event_name)

         SELECT Single_value_Table.acc_bb_acct_id,Single_value_Table.activity_date,
                  sum(Chomecast_connect) AS chomecast_connect,
                  sum(App_Login) AS app_login,
                  sum(Discover) AS discover,
                  sum(Browse) AS browse,
                  sum(LiveTV_load) AS liveTV,
                  sum(TVOD_load) AS TVOD,
                  sum(profile_load) AS profile_load,
                  sum(Apply_Filter) AS filter_apply,
                  sum(channel_liveTV) AS liveTV_channel_click,
                  sum(clicked_recommended) AS clicked_recommended,
                  sum(download) AS download_click,
                  sum(button_tap_playdiscover) AS discover_play_click,
                  sum(button_tap_playdownload) AS download_play_click,
                  sum(button_tap_playepisode) AS episode_play_click,
                  sum(button_tap_playlastwatched) AS playlastwatched_click,
                  sum(button_tap_rental_playdetails) AS TVOD_play_click,
                  sum(rental_purchase) AS rental_purchase,
                  sum(rental_purchase_cancel) AS rental_purchase_cancel,
                  sum(button_tap_search) AS search_click,
                  sum(button_tap_start_trial) AS start_trial_click,
                  sum(button_tap_vouchercodesuccess) AS ticketvoucher_click_success,
                  sum(button_tap_watchlater) AS watchlater_click,
                  sum(discover_10) AS discover_10,
                  sum(discover_50) AS discover_50,
                  sum(notification_download_error) AS notification_download_error,
                  sum(over21_access) AS over21_access,
                  sum(over21_no_access) AS over21_no_access,
                  sum(Page_load_404) AS page_load_404,
                  sum(download_quality) AS download_quality_click,
                  sum(favorite) AS favorite_click,
                  sum(remove_favorite) AS remove_favorite,
                  sum(button_tap_genre) AS genre_click,
                  sum(button_tap_paywall_plan) AS paywall_plan_click,
                  sum(button_tap_playcollection) AS collection_click,
                  sum(play_content) AS play_content,
                  sum(download_click_error) AS download_click_error,
                  sum(playback_error) AS playback_error,
                  sum(liveTV_click_error) AS liveTV_click_error,
               properties_filter_genre,properties_filter_content_type

         FROM Single_value_Table left join Filter_Type on (Single_value_Table.activity_date=Filter_Type.activity_date and Single_value_Table.acc_bb_acct_id=Filter_Type.acc_bb_acct_id)
         left join Genre_Type on (Single_value_Table.activity_date=Genre_Type.event_date and Single_value_Table.acc_bb_acct_id=Genre_Type.bb_acct_id)

   --where Single_value_Table.acc_bb_acct_id='17050200212687679039067'
    GROUP BY Single_value_Table.acc_bb_acct_id,Single_value_Table.activity_date
         ,properties_filter_genre,properties_filter_content_type
                     LIMIT 100;


