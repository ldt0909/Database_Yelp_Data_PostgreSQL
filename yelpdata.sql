./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/yelp_academic_dataset_business.csv 
./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/yelp_academic_dataset_tip.csv
./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/yelp_academic_dataset_user.csv 
./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/yelp_academic_dataset_checkin.csv
./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/yelp_academic_dataset_review.csv  

./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/converted_business_data.csv 
./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/user_without_month.csv
./pgfutter --dbname yelpdata --username dtl csv ~/Desktop/yelpdata/yelp_academic_dataset_review_test_polarity_WI.csv

-----------------create table-------------------

---CREATE DATABASE YELPDATA;

create table business(
	business_id varchar(1000) not null ,
	name varchar(1000) not null,
	full_address varchar(1000) not null,
	city varchar(1000) not null,
	state varchar(1000) not null,
	latitude double precision not null default 0::double precision,
	longitude double precision not null default 0::double precision,
	stars double precision not null default 0::double precision,
    attributes_parking_lot varchar(100) not null,
    attributes_price_range varchar(1000) not null,
    attributes_wi_fi varchar(100) not null,
    type varchar(1000) not null,
    attributes_waiter_service varchar(1000) not null,
    attributes_dogs_allowed varchar(1000) not null,
    categories varchar(1000) not null,
    attributes_good_for_groups varchar(1000) not null,
    attributes_noise_level varchar(1000) not null,
    attributes_smoking varchar(1000) not null,
    attributes_has_tv varchar(1000) not null,
    attributes_alcohol varchar(1000) not null,
    attributes_good_for_kids varchar(1000) not null, 
    attributes_good_for_dancing varchar(1000) not null,
    attributes_happy_hour varchar(1000) not null,
	primary key(business_id)
);

create table new_business(
	business_id varchar(1000) not null ,
	name varchar(1000) not null,
	stars double precision not null default 0::double precision,    
    categories varchar(1000) not null,
    attributes_price_range double precision
    
);

create table users(
	user_id varchar(1000) not null,
	name varchar(1000) not null,
	yelping_since text not null,
	fans integer not null default 0,
	review_count integer not null,
	friends text not null,
	elite varchar(1000) not null,
	votes_cool integer ,
	votes_funny integer ,
	votes_useful  integer,
	average_stars double precision not null default 0::double precision,
	primary key(user_id)
);

create table new_users(
	user_id varchar(1000) not null,
	name varchar(1000) not null,
	yelping_since text not null,
	primary key(user_id)
);


create table reviews(
	review_id varchar(1000) not null,
	business_id varchar(1000) not null,
	user_id varchar(1000) not null,
	stars double precision not null default 0::double precision,
	text text not null,
	date text not null,
	votes_cool integer ,
	votes_funny integer ,
	votes_useful  integer,
	primary key(review_id)
);


create table tips(
	business_id varchar(1000) not null,
	user_id varchar(1000) not null, 
	date text not null,
	text text not null
);

create table polarity( 
	business_id varchar(1000) not null,
	review_id varchar(1000) not null,
	text text not null,
	polarity double precision not null default 0::double precision

);




------------------insert data-------------------


insert into tips
select business_id as business_id,
       user_id as user_id,      
       text as text,
       date as date
from import.yelp_academic_dataset_tip;


insert into reviews
select review_id as review_id,
       business_id as business_id,
       user_id as user_id,
       cast(stars as decimal) as stars,
       text as text,
       date as date,
       cast(votes_cool as integer) as votes_cool,
	   cast(votes_funny as integer) as votes_funny,
	   cast(votes_useful as integer) as votes_useful
from import.yelp_academic_dataset_review;


insert into users
select user_id as user_id,
       name as name, 
       yelping_since as yelping_since, 
	   cast(fans as integer) as fans,
	   cast(review_count as integer) as review_count,
	   friends as friends,
	   elite as elite,
	   cast(votes_cool as integer) as votes_cool,
	   cast(votes_funny as integer) as votes_funny,
	   cast(votes_useful as integer) as votes_useful,
	   ("average_stars")::double precision as average_stars
from import.yelp_academic_dataset_user;

insert into new_users
select user_id as user_id,
       name as name, 
       yelping_since as yelping_since,
       ("average_stars")::double precision as average_stars	   
from import.user_without_month;

	
insert into business
select business_id as business_id,
   name as name, 
   full_address as full_address, 
   city as city, 
   state as state, 
   ("latitude")::double precision as latitude, 
   ("longitude")::double precision as longitude, 
   ("stars")::double precision as stars,
   attributes_parking_lot as attributes_parking_lot ,
   attributes_price_range as attributes_price_range,
   attributes_wi_fi as attriqbutes_wi_fi,
   type as type,
   attributes_waiter_service as attributes_waiter_service, 
   attributes_dogs_allowed as attributes_dogs_allowed,
   categories as categories,
   attributes_good_for_groups as attributes_good_for_groups,
   attributes_noise_level as attributes_noise_level,
attributes_smoking as attributes_smoking,
attributes_has_tv as attributes_has_tv,
attributes_alcohol as attributes_alcohol,
attributes_good_for_kids as attributes_good_for_kids,
attributes_good_for_dancing as attributes_good_for_dancing,
attributes_happy_hour as attributes_happy_hour
from import.yelp_academic_dataset_business;


insert into new_business
select business_id as business_id ,
       name as name, 
       ("stars")::double precision as stars,
       categories as categories,
       case when textregexeq(attributes_price_range, 
       '^[[:digit:]]+(\.[[:digit:]]+)?$') 
       then cast(attributes_price_range as double precision) 
       else NULL 
       end as attributes_price_range
from import.converted_business_data;


insert into polarity
select business_id as business_id,
	review_id as review_id,
	text as text,
	("polarity")::double precision as polarity
from import.yelp_academic_dataset_review_test_polarity_wi;



----------------------Queries---------------------
#####################business table
1.
select categories, avg(stars), count(*)
from business 
where categories LIKE '%Restaurants%' 
group by categories
having count(*) > 100 and avg(stars) > 3.5
order by avg(stars) DESC;

select categories, avg(stars), count(*)
from business 
where categories LIKE '%Restaurants%' 
group by categories
having count(*) > 100 and avg(stars) <= 3.0
order by avg(stars) asc;

2.
select state,avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by state
having count(*) > 20 
order by avg(stars) DESC;

Copy (select state,avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by state
having count(*) > 20 
order by avg(stars) DESC
)
To '/Users/dantili/desktop/yelpdata/state_rating.csv' With CSV DELIMITER ',';


3.
# Table business 
select attributes_waiter_service, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_waiter_service
;
# avg stars True slightly higher than False

select attributes_dogs_allowed, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_dogs_allowed
;
# avg stars True slightly higher than False

select attributes_wi_fi, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_wi_fi
;
# avg stars Free slightly higher than no
# much higher than paid

select attributes_good_for_groups, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_good_for_groups
;
# avg stars True slightly higher than False

select attributes_price_range, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_price_range
order by avg(stars) desc;
# higher price range, higher avg rating

select attributes_parking_lot, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_parking_lot
;
# True slightly higher than False

select attributes_smoking, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_smoking
order by avg(stars) DESC;
# True slightly higher than False

select attributes_has_tv, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_has_tv
order by avg(stars) DESC;

select attributes_alcohol, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_alcohol
order by avg(stars) DESC;

select attributes_good_for_kids, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_good_for_kids
order by avg(stars) DESC;

select attributes_good_for_dancing, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_good_for_dancing
order by avg(stars) DESC;

select attributes_happy_hour, avg(stars)
from business
where categories LIKE '%Restaurants%' 
group by attributes_happy_hour
order by avg(stars) ;

4.
select n1.categories, avg(n1.stars), count(*),avg(n1.attributes_price_range) 
from new_business n1, new_business n2
where n1.business_id = n2.business_id 
and n2.categories = 'Restaurants' 
and n1.categories != 'Restaurants'
group by n1.categories, n1.attributes_price_range
having count(*) > 20
order by count desc;


5.
with tmp as (
	select name, state, avg(stars) as stars , count(*) as count 
    from business 
    where categories 
    LIKE '%Restaurants%'  
    group by name, state 
    order by name, state
    )
select name, state, stars, count 
from tmp t1 where exists(
	select 1 
	from tmp t2 
	where t1.name = t2.name and t1.state != t2.state and (
		t2.count > 5 or t1.count > 5
		));

################### users table queries
6.
# rating yearly change
select yelping_since, avg(average_stars) 
from new_users
group by yelping_since
order by yelping_since desc;

# rating monthly change
select yelping_since, avg(average_stars),count(average_stars)  
from users
group by yelping_since
order by yelping_since asc;

Copy (select yelping_since, avg(average_stars) 
from new_users
group by yelping_since
order by yelping_since asc
)
To '/Users/dantili/desktop/yelpdata/users_rating_yearly.csv' With CSV DELIMITER ',';

Copy (select yelping_since, avg(average_stars),count(average_stars) 
from users
group by yelping_since
order by yelping_since asc
)
To '/Users/dantili/desktop/yelpdata/users_rating_monthly.csv' With CSV DELIMITER ',';



7.# visualization
# users with higher average rating have more fans??
select fans, average_stars
from users
order by fans asc;

Copy (
select fans, average_stars
from users
order by fans desc)
To '/Users/dantili/desktop/yelpdata/users_fans.csv' With CSV DELIMITER ',';

############## review table queries
8.# visualization
select date,
       count(votes_funny) as votes_funny,
       count(votes_useful) as votes_useful,
       count(votes_useful) votes_cool
from reviews
group by date
order by date asc;


Copy (
select date,
       count(votes_funny) as votes_funny,
       count(votes_useful) as votes_useful,
       count(votes_useful) votes_cool
from reviews
group by date
order by date asc)
To '/Users/dantili/desktop/yelpdata/review_votes.csv' With CSV DELIMITER ',';








Copy (
select a.business_id,b.review_id,b.text
from business a,reviews b
where a.business_id = b.business_id 
and a.categories LIKE '%Restaurants%')
To '/Users/dantili/desktop/yelpdata/review_sentiment.csv' With CSV DELIMITER ',';

Copy (
select a.business_id,b.review_id,b.text
from business a,reviews b
where a.business_id = b.business_id 
and a.categories LIKE '%Restaurants%' and a.state = 'WI')
To '/Users/dantili/desktop/yelpdata/review_sentiment_WI.csv' With CSV DELIMITER ',';

Copy (select date,
       count(votes_funny) as votes_funny,
       count(votes_useful) as votes_useful,
       count(votes_useful) votes_cool
from reviews
group by date
order by date asc) To '/desktop' With CSV DELIMITER ',';

# seasonality avg(rating) change
# yearly avg(rating) change
# each state average rating different


Copy (
select a.stars,
       a.attributes_parking_lot,
       a.attributes_price_range,
       a.attributes_wi_fi,
       a.attributes_waiter_service,
       a.attributes_smoking,
       a.attributes_dogs_allowed,
       a.attributes_has_tv,
       a.attributes_alcohol,
       a.attributes_good_for_kids,
       a.attributes_good_for_dancing,
       a.attributes_happy_hour, 
       a.attributes_noise_level,                     
       avg(c.polarity),
       count(b.review_id),
       sum(b.votes_useful),
       sum(b.votes_funny),
       sum(b.votes_cool)
from business a,reviews b,polarity c
where a.business_id = b.business_id 
and   a.business_id = c.business_id
and   b.business_id = c.business_id
and a.state= 'WI' and a.categories LIKE '%Restaurants%'
group by a.business_id)
To '/desktop/yelpdata/Model_WIS.csv' With CSV DELIMITER ',';







