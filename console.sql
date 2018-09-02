
/*First Table-1 -Integration of vod  detials with content ID on TV show leave  */
with account_info as(
select vod.bb_acct_id , vod.country  , vod.con_contact_activation_date , vod.event_date , vod.content_id,
con.series_page,con.content_type
from summary.dly_base_vod_usage_vw as vod
INNER JOIN summary.dim_content_detail_vw con
ON vod.content_id=con.content_id
where vod.con_contact_activation_date> current_date-180
and vod.con_email not like '%@hooq.tv'
LIMIT 200),
newtable as (
select*,
CASE WHEN content_type ='EPISODE' THEN series_page
WHEN content_type='MOTIONPICTURE' THEN content_id END  id
from account_info)
select * from newtable;

/*2nd Table - Conversion of subtitles ,audio language ,genres, region  into 0 and 1*/
select
content_id, content_title, content_type,
CASE  audiolanguage_english when 'Y' THEN 1 ELSE 0 END Audio_english,
CASE  audiolanguage_hindi when 'Y' THEN 1 ELSE 0 END Audio_hindi,
CASE  audiolanguage_tamil when 'Y' THEN 1 ELSE 0 END Audio_tamil,
CASE  audiolanguage_telegu when 'Y' THEN 1 ELSE 0 END Audio_telegu,
CASE  audiolanguage_gujrati when 'Y' THEN 1 ELSE 0 END Audio_gujrati,
CASE  audiolanguage_marathi when 'Y' THEN 1 ELSE 0 END Audio_marathi,
CASE  audiolanguage_punjabi when 'Y' THEN 1 ELSE 0 END Audio_punjabi,
CASE  audiolanguage_bhojpuri when 'Y' THEN 1 ELSE 0 END Audio_bhojpuri,
CASE  audiolanguage_kannada when 'Y' THEN 1 ELSE 0 END Audio_kannada,
CASE  audiolanguage_malayalam when 'Y' THEN 1 ELSE 0 END Audio_malyalam,
CASE  audiolanguage_bengali when 'Y' THEN 1 ELSE 0 END Audio_bengali,
CASE  audiolanguage_thai when 'Y' THEN 1 ELSE 0 END Audio_thai,
CASE  audiolanguage_tagalog when 'Y' THEN 1 ELSE 0 END Audio_tagalog,
CASE  audiolanguage_japanese when 'Y' THEN 1 ELSE 0 END Audio_japanese,
CASE  audiolanguage_korean when 'Y' THEN 1 ELSE 0 END Audio_korean,
CASE  audiolanguage_bahasa when 'Y' THEN 1 ELSE 0 END Audio_bahasa,
CASE  subtitle_english when 'Y' THEN 1 else 0 end Subtitle_english,
CASE  subtitle_hindi when 'Y' THEN 1 else 0 end Subtitle_Hindi,
CASE  subtitle_tamil when 'Y' THEN 1 else 0 end Subtitle_tamil,
CASE  subtitle_telegu when 'Y' THEN 1 else 0 end Subtitle_telegu,
CASE  subtitle_gujrati when 'Y' THEN 1 else 0 end Subtitle_gujrati,
CASE  subtitle_marathi when 'Y' THEN 1 else 0 end Subtitle_marathi,
CASE  subtitle_punjabi when 'Y' THEN 1 else 0 end Subtitle_punjabi,
CASE  subtitle_bhojpuri when 'Y' THEN 1 else 0 end Subtitle_bhojpuri,
CASE  subtitle_kannada when 'Y' THEN 1 else 0 end Subtitle_kannada,
CASE  subtitle_malayalam when 'Y' THEN 1 else 0 end Subtitle_malyalam,
CASE  subtitle_bengali when 'Y' THEN 1 else 0 end Subtitle_bengali,
CASE  subtitle_thai when 'Y' THEN 1 else 0 end Subtitle_Thai,
CASE  subtitle_tagalog when 'Y' THEN 1 else 0 end Subtitle_tagalog,
CASE  subtitle_japanese when 'Y' THEN 1 else 0 end Subtitle_japanese,
CASE  subtitle_korean when 'Y' THEN 1 else 0 end Subtitle_korean,
CASE  subtitle_bahasa when 'Y' THEN 1 else 0 end Subtitle_bahasa,
CASE  subtitle_english when 'Y' THEN 1 else 0 end Subtitle_english,
CASE  vod_type when 'SVOD' THEN 1 ELSE 0 END  SVOD,
CASE  vod_type when 'TVOD' THEN 1 ELSE 0 END  TVOD,
CASE  content_provider_group when 'REGIONAL' then 1 else 0 END Regional,
       CASE  content_provider_group when 'IN LOCAL' then 1 else 0 END INDIA_Local,
       CASE  content_provider_group when 'PH LOCAL' then 1 else 0 END PH_Local,
       CASE  content_provider_group when 'ID LOCAL' then 1 else 0 END ID_Local,
       CASE  content_provider_group when 'TH LOCAL' then 1 else 0 END TH_Local,
       CASE  content_provider_group when 'HOOQ' then 1 else 0 END TH_LOCAL,
       CASE  content_provider_group when 'HOLLYWOOD' then 1 else 0 END HOLLYWOOD,
       CASE  WHEN A.genre_1='Action' OR A.genre_2='Action' OR A.genre_3='Action'
                  OR A.genre_4='Action' OR A.genre_5='Action' THEN 1 ELSE 0 END AS  action,
       CASE  WHEN A.genre_1='Anime' OR A.genre_2='Anime' OR A.genre_3='Anime'
                  OR A.genre_4='Anime' OR A.genre_5='Anime' THEN 1 ELSE 0 END AS  Anime,
       CASE  WHEN A.genre_1='Mystery' OR A.genre_2='Mystery' OR A.genre_3='Mystery'
                  OR A.genre_4='Mystery' OR A.genre_5='Mystery' THEN 1 ELSE 0 END AS  Mystery,
       CASE  WHEN A.genre_1='Variety Entertainment' OR A.genre_2='Variety Entertainment' OR A.genre_3='Variety Entertainment'
                  OR A.genre_4='Variety Entertainment' OR A.genre_5='Variety Entertainment' THEN 1 ELSE 0 END AS Variety_Entertainment,
        CASE  WHEN A.genre_1='Religion' OR A.genre_2='Religion' OR A.genre_3='Religion'
                  OR A.genre_4='Religion' OR A.genre_5='Religion' THEN 1 ELSE 0 END AS  Religion,
       CASE  WHEN A.genre_1='Action & adventure' OR A.genre_2='Action & adventure' OR A.genre_3='Action & adventure'
                  OR A.genre_4='Action & adventure' OR A.genre_5='Action & adventure' THEN 1 ELSE 0 END AS  Action_adventure,
         CASE  WHEN A.genre_1='Independent' OR A.genre_2='Independent' OR A.genre_3='Independent'
                  OR A.genre_4='Independent' OR A.genre_5='Independent' THEN 1 ELSE 0 END AS  Independent,
           CASE  WHEN A.genre_1='Animation' OR A.genre_2='Animation' OR A.genre_3='Animation'
                  OR A.genre_4='Animation' OR A.genre_5='Animation' THEN 1 ELSE 0 END AS  Animation,
          CASE  WHEN A.genre_1='Classic' OR A.genre_2='Classic' OR A.genre_3='Classic'
                  OR A.genre_4='Classic' OR A.genre_5='Classic' THEN 1 ELSE 0 END AS  Classic,
       CASE  WHEN A.genre_1='Kids' OR A.genre_2='Kids' OR A.genre_3='Kids'
                  OR A.genre_4='Kids' OR A.genre_5='Kids' THEN 1 ELSE 0 END AS  Kids,
       CASE  WHEN A.genre_1='Hollywood' OR A.genre_2='Hollywood' OR A.genre_3='Hollywood'
                  OR A.genre_4='Hollywood' OR A.genre_5='Hollywood' THEN 1 ELSE 0 END AS  Hollywood,
       CASE  WHEN A.genre_1='Biography' OR A.genre_2='Biography' OR A.genre_3='Biography'
                  OR A.genre_4='Biography' OR A.genre_5='Biography' THEN 1 ELSE 0 END AS  Biography,
       CASE  WHEN A.genre_1='War' OR A.genre_2='War' OR A.genre_3='War'
                  OR A.genre_4='War' OR A.genre_5='War' THEN 1 ELSE 0 END AS  War,
         CASE  WHEN A.genre_1='Game Shows' OR A.genre_2='Game Shows' OR A.genre_3='Game Shows'
                  OR A.genre_4='Game Shows' OR A.genre_5='Game Shows' THEN 1 ELSE 0 END AS GameShows,
       CASE  WHEN A.genre_1='Reality TV' OR A.genre_2='Reality TV' OR A.genre_3='Reality TV'
                  OR A.genre_4='Reality TV' OR A.genre_5='Reality TV' THEN 1 ELSE 0 END AS Reality_TV,
       CASE  WHEN A.genre_1='Thriller' OR A.genre_2='Thriller' OR A.genre_3='Thriller'
                  OR A.genre_4='Thriller' OR A.genre_5='Thriller' THEN 1 ELSE 0 END AS Thriller,
       CASE  WHEN A.genre_1='Variety Entertainment' OR A.genre_2='Variety Entertainment' OR A.genre_3='Variety Entertainment'
                  OR A.genre_4='Variety Entertainment' OR A.genre_5='Variety Entertainment' THEN 1 ELSE 0 END AS Variety_Entertainment,
       CASE  WHEN A.genre_1='Sports' OR A.genre_2='Sports' OR A.genre_3='Sports'
                  OR A.genre_4='Sports' OR A.genre_5='Sports' THEN 1 ELSE 0 END AS Sports,
        CASE  WHEN A.genre_1='Superhero' OR A.genre_2='Superhero' OR A.genre_3='Superhero'
                  OR A.genre_4='Superhero' OR A.genre_5='Superhero' THEN 1 ELSE 0 END AS Sports,
        CASE  WHEN A.genre_1='Romance' OR A.genre_2='Romance' OR A.genre_3='Romance'
                  OR A.genre_4='Romance' OR A.genre_5='Romance' THEN 1 ELSE 0 END AS Romance,
       CASE  WHEN A.genre_1='Martial Arts & Sports' OR A.genre_2='Martial Arts & Sports' OR A.genre_3='Martial Arts & Sports'
                  OR A.genre_4='Martial Arts & Sports' OR A.genre_5='Martial Arts & Sports' THEN 1 ELSE 0 END AS Martial_Arts_Sports,
        CASE  WHEN A.genre_1='Kids & Family' OR A.genre_2='Kids & Family' OR A.genre_3='Kids & Family'
                  OR A.genre_4='Kids & Family' OR A.genre_5='Kids & Family' THEN 1 ELSE 0 END AS Kids_Family,
       CASE  WHEN A.genre_1='Crime' OR A.genre_2='Crime' OR A.genre_3='Crime'
                  OR A.genre_4='Crime' OR A.genre_5='Crime' THEN 1 ELSE 0 END AS Crime,
       CASE  WHEN A.genre_1='Sci-Fi & Fantasy' OR A.genre_2='Sci-Fi & Fantasy' OR A.genre_3='Sci-Fi & Fantasy'
                  OR A.genre_4='Sci-Fi & Fantasy' OR A.genre_5='Sci-Fi & Fantasy' THEN 1 ELSE 0 END AS SciFiFantasy,
    CASE  WHEN A.genre_1='Family' OR A.genre_2='Family' OR A.genre_3='Family'
                  OR A.genre_4='Family' OR A.genre_5='Family' THEN 1 ELSE 0 END AS Family,
       CASE  WHEN A.genre_1='History' OR A.genre_2='History' OR A.genre_3='History'
                  OR A.genre_4='History' OR A.genre_5='History' THEN 1 ELSE 0 END AS History,
       CASE  WHEN A.genre_1='Documentary' OR A.genre_2='Documentary' OR A.genre_3='Documentary'
                  OR A.genre_4='Documentary' OR A.genre_5='Documentary' THEN 1 ELSE 0 END AS Documentary,
       CASE  WHEN A.genre_1='Comedy' OR A.genre_2='Comedy' OR A.genre_3='Comdey'
                  OR A.genre_4='Comdey' OR A.genre_5='Comdey' THEN 1 ELSE 0 END AS Comdey,
       CASE  WHEN A.genre_1='Critically Acclaimed' OR A.genre_2='Critically Acclaimed' OR A.genre_3='Critically Acclaimed'
                  OR A.genre_4='Critically Acclaimed' OR A.genre_5='Critically Acclaimed' THEN 1 ELSE 0 END AS CriticallyAcclaimed,
       CASE  WHEN A.genre_1='Horror' OR A.genre_2='Horror' OR A.genre_3='Horror'
                  OR A.genre_4='Horror' OR A.genre_5='Horror' THEN 1 ELSE 0 END AS Horror,
        CASE  WHEN A.genre_1='Martial Arts' OR A.genre_2='Martial Arts' OR A.genre_3='Martial Arts'
                  OR A.genre_4='Martial Arts' OR A.genre_5='Martial Arts' THEN 1 ELSE 0 END AS MartialArts,
       CASE  WHEN A.genre_1='Musical' OR A.genre_2='Musical' OR A.genre_3='Musical'
                  OR A.genre_4='Musical' OR A.genre_5='Musical' THEN 1 ELSE 0 END AS Musical,
       CASE  WHEN A.genre_1='Drama' OR A.genre_2='Drama' OR A.genre_3='Drama'
                  OR A.genre_4='Drama' OR A.genre_5='Drama' THEN 1 ELSE 0 END AS Drama
FROM summary.dim_content_detail_vw as A ;