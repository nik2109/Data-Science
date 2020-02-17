*loads poincare on 18055146@poincare.mmu.ac.uk and pyspark*

#First the initial check to see the locations of files on the system #

In [1]: !hadoop fs -ls /user/msc/datasets/yelp

Found 6 items
-rw-r--r--   3 luciano msc18  145175074 2018-03-08 16:19 /user/msc/datasets/yelp/business.json
-rw-r--r--   3 luciano msc18   63274733 2018-03-08 16:20 /user/msc/datasets/yelp/checkin.json
-rw-r--r--   3 luciano msc18   26888701 2018-03-08 16:20 /user/msc/datasets/yelp/photos.json
-rw-r--r--   3 luciano msc18 4198268919 2018-03-08 16:19 /user/msc/datasets/yelp/review.json
-rw-r--r--   3 luciano msc18  197557967 2018-03-08 16:20 /user/msc/datasets/yelp/tip.json
-rw-r--r--   3 luciano msc18 1891399764 2018-03-08 16:20 /user/msc/datasets/yelp/user.json



#Import json module to read the files#

In [2]: import json

#Reading the Review file#

In [3]: rev= sc.textFile('/user/msc/datasets/yelp/review.json').map(lambda x: json.loads(x))

#Check the first instance

In [4]: rev.first()

# Q- Which reviews have been rated as useful by more than 30 users and funny by more than 20 users? Obtain total number and show a couple of example instances 

In [5]: rev.filter(lambda x: x['useful']>30).filter(lambda x: x['funny']>20).take(2)

Out[5]: 
[{'business_id': 'upgjUq616Yz1IvAvysDLWA',
  'cool': 40,
  'date': '2017-09-08',
  'funny': 25,
  'review_id': 'und_IbRFr2Rwpizt-kfZuw',
  'stars': 5,
  'text': 'Great food!\n\nThis restaurant isn\'t located on the Strip... this place isn\'t Hash House A Go Go, Momofuku, Wicked Spoon nor is it Eggslut (although I\'ve bookmarked those places for future visits) but because of that it doesn\'t have the food prices you pay when dining at the Strip. Get it? For a little over $20 we were able to fill our tummies with delicious food.\n\nI had Ely\'s Burger. Beef, cheese, grilled onions, mushrooms, lettuce, tomato and mayo... hold the pickles and red onion, please. The meal came with crinkle-cut fries. I love the mushrooms in this burger. Tasty and delicious!  Hubby had the Bacon (or Sausage) Skillet. He scarfed it down... no need to ask him if he enjoyed his meal. \n\nThis place has a casual dining atmosphere, nothing fancy. They serve classic American food, breakfast, lunch, and dinner. Great, affordable prices. Our table was well taken care of. Very attentive servers but not overly. They were friendly and nice. Hubby loves wearing his GS Warriors shirts and it always gets attention from sports fans when we go places. Had engaged in a convo with the server who also processed our bill (Lakers fan). Then, I\'m not sure if it was the owner that said to me as we were leaving... "review us in Yelp" (now how did he know I Yelp??). Worth coming back to when we\'re in town again :)\n\nReview #0835',
  'useful': 47,
  'user_id': 'Hm0diOkWwpo9zotlJlqMUQ'},
 {'business_id': 'I6ICIIZfZsp_J9hHmfhWhQ',
  'cool': 43,
  'date': '2017-09-09',
  'funny': 25,
  'review_id': 'yD_zR37iwd_zqonHEhnjTg',
  'stars': 4,
  'text': "Parking fees in casinos on the Strip\n\nLove coming to Vegas. We used to frequent visiting Sin City, sometimes three times a year, but lately have slowed down and this year, 2017, was our first time back in Vegas since Feb 2016! wow! has it been that long... no wonder I was longing for it in this last mini getaway! Anyway, found out that many of the casinos have done away with the free parking! The good old days of plentiful free parking at casinos ended in 2016. Bummer! Wait, I take that back... I think some casinos offer 0-60 mins free parking. It's true that parking garages here are so abundant but unless you're staying at or patronizing the resort you have to fork out $ for parking fees. Now it's all about parking in one spot, pay the fees, and walk, walk, walk... I suppose one can take the monorails too. \n\nWhile I'm done ranting about the newly imposed parking fees (new to me, at least) I do enjoy walking around the Strip. I'm not talking about from one end to another. I enjoyed it best during Pacquiao's fights held at the MGM (held usually in May). Hubby and I come to Vegas to celebrate our birthdays around that time. Loved the hype it brought. I also enjoyed the outdoor shows... Treasure Island, The Mirage and Bellagio. \n\nAnd damn those Las Vegas card flickers. If you've ever been to Vegas, you know what I'm talking about. Although, I didn't see any this time while walking around Treasure Island and crossing over to Wynn. Are they gone now?\n\nIf flying in Vegas one can see the Strip with all the lights as your plane is about to land. I always look for either the Stratosphere (The tallest observation tower in the U.S.) or the light beam from the Luxor on the southern end (the Luxor Sky Beam is the strongest beam of light in the world). I look for these landmarks when our plane is about to descend at McCarran Int'l Airport. Lots of car traffic as well as foot traffic on the Strip. Lots of lights. Lots of tourists. Lots of shops, restaurants, bars and lounges. This place comes alive on weekends.  What happens in Vegas stays in... Facebook, IG, and Snapchat (nowadays). Be mindful :)\n\nReview #0838",
  'useful': 49,
  'user_id': 'Hm0diOkWwpo9zotlJlqMUQ'}]

#total number

In [6]: rev.filter(lambda x: x['useful']>30).filter(lambda x: x['funny']>20).count()
Out[6]: 4011  

# Q- Which businesses based in Las Vegas that are identified as Nightlife have been rated 4.5 stars or higher? #

#loads data
In [7]: business= sc.textFile('/user/msc/datasets/yelp/business.json').map(lambda x: json.loads(x))

#assigns business_rated as the RDD which contains selected businesses
In [8]: business_las=business.filter(lambda x: x['city']=='Las Vegas')

In [9]: business_rated=business_las.filter(lambda x: x['stars']>=4.5)

In [10]: business_lv_4n= business_rated.filter(lambda x: 'Nightlife' in x['categories'])

#count to see number of instances
In [11]: business_lv_4n.count()
Out[11]: 382    

#First instance 

In [12]: business_lv_4n.first()
Out[12]: 
{'address': '5006 S Maryland Pkwy, Ste 17',
 'attributes': {},
 'business_id': 'WfB_SsYeKy83QQsqAAyGVQ',
 'categories': ['Karaoke',
  'Bars',
  'Mexican',
  'Restaurants',
  'Nightlife',
  'Dance Clubs'],
 'city': 'Las Vegas',
 'hours': {'Friday': '8:00-0:00',
  'Monday': '8:00-0:00',
  'Saturday': '8:00-0:00',
  'Sunday': '8:00-0:00',
  'Thursday': '8:00-0:00',
  'Tuesday': '8:00-0:00',
  'Wednesday': '8:00-0:00'},
 'is_open': 1,
 'latitude': 36.0986321,
 'longitude': -115.1360793,
 'name': 'Cancun Bar & Grill',
 'neighborhood': 'Southeast',
 'postal_code': '89119',
 'review_count': 5,
 'stars': 4.5,
 'state': 'NV'}


# what are the top-10 reviewers, in terms of the absolute number of reviews marked as useful by
# other users, of Nightlife businesses in Urbana-Champaign?

# target values are for Nightlife in 'categories', analysis of and ranking of useful set by threshold for relevant area

#Error is seen when 'Urbana-Champaign'is searched under 'city', therefore individual searches yield:


In [13]: business.filter(lambda x: x['city']=='Champaign').first()
Out[13]: 
{'address': '313 E Green St, Ste 5',
 'attributes': {'Alcohol': 'none',
  'BusinessAcceptsCreditCards': True,
  'BusinessParking': {'garage': False,
   'lot': False,
   'street': False,
   'valet': False,
   'validated': False},
  'GoodForKids': False,
  'GoodForMeal': {'breakfast': False,
   'brunch': False,
   'dessert': False,
   'dinner': False,
   'latenight': False,
   'lunch': False},
  'OutdoorSeating': False,
  'RestaurantsAttire': 'casual',
  'RestaurantsDelivery': False,
  'RestaurantsGoodForGroups': False,
  'RestaurantsPriceRange2': 1,
  'RestaurantsReservations': False,
  'RestaurantsTableService': True,
  'RestaurantsTakeOut': True,
  'WheelchairAccessible': False},
 'business_id': 'zV_aclADLjx2KOql9F_FTw',
 'categories': ['Restaurants', 'Creperies'],
 'city': 'Champaign',
 'hours': {},
 'is_open': 0,
 'latitude': 40.109986,
 'longitude': -88.233777,
 'name': 'Crepe Cafe',
 'neighborhood': '',
 'postal_code': '61820',
 'review_count': 4,
 'stars': 3.0,
 'state': 'IL'}

In [14]: business.filter(lambda x: x['city']=='Urbana').first()
Out[14]: 
{'address': '',
 'attributes': {'BusinessAcceptsBitcoin': False,
  'BusinessAcceptsCreditCards': True,
  'ByAppointmentOnly': True},
 'business_id': 'r6Jw8oRCeumxu7Y1WRxT7A',
 'categories': ['Home Cleaning', 'Home Services', 'Window Washing'],
 'city': 'Urbana',
 'hours': {'Friday': '9:00-20:00',
  'Monday': '9:00-20:00',
  'Saturday': '9:00-18:00',
  'Thursday': '9:00-20:00',
  'Tuesday': '9:00-20:00',
  'Wednesday': '9:00-20:00'},
 'is_open': 0,
 'latitude': 40.1105875,
 'longitude': -88.2072697,
 'name': 'D&D Cleaning',
 'neighborhood': '',
 'postal_code': '61802',
 'review_count': 4,
 'stars': 5.0,
 'state': 'IL'}

#confirmation that they are available seperately, next step is to filter through to select those that classify as 'Nightlife'

In [15]: busurb=business.filter(lambda x: x['city']=='Urbana')

#seperate RDD created and then Nightlife is filtered through, and checked using .first()

In [16]: busurb.filter(lambda x: 'Nightlife' in x['categories']).first()
Out[16]: 
{'address': '214 W Main St',
 'attributes': {'Alcohol': 'full_bar',
  'BikeParking': True,
  'BusinessAcceptsBitcoin': False,
  'BusinessAcceptsCreditCards': True,
  'BusinessParking': {'garage': False,
   'lot': False,
   'street': True,
   'valet': False,
   'validated': False},
  'CoatCheck': True,
  'DogsAllowed': False,
  'GoodForDancing': False,
  'GoodForKids': False,
  'HappyHour': True,
  'HasTV': True,
  'Music': {'background_music': False,
   'dj': False,
   'jukebox': True,
   'karaoke': False,
   'live': False,
   'no_music': False,
   'video': False},
  'NoiseLevel': 'quiet',
  'OutdoorSeating': False,
  'RestaurantsDelivery': False,
  'RestaurantsGoodForGroups': True,
  'RestaurantsPriceRange2': 2,
  'RestaurantsReservations': True,
  'RestaurantsTableService': True,
  'RestaurantsTakeOut': False,
  'Smoking': 'outdoor',
  'WheelchairAccessible': True,
  'WiFi': 'free'},
 'business_id': 'sOPBbhDCi7bJlW7dNBuITg',
 'categories': ['Restaurants',
  'Venues & Event Spaces',
  'Cafes',
  'Lounges',
  'Nightlife',
  'Pool Halls',
  'Event Planning & Services',
  'Pool & Billiards',
  'Dance Clubs',
  'Bars',
  'Shopping',
  'Karaoke'],
 'city': 'Urbana',
 'hours': {'Friday': '18:00-2:00',
  'Saturday': '18:00-2:00',
  'Sunday': '18:00-1:00',
  'Thursday': '18:00-1:00',
  'Wednesday': '18:00-1:00'},
 'is_open': 1,
 'latitude': 40.112708,
 'longitude': -88.209562,
 'name': 'A Plus VIP Lounge',
 'neighborhood': '',
 'postal_code': '61801',
 'review_count': 16,
 'stars': 3.5,
 'state': 'IL'}

In [17]: bus_night_urb=busurb.filter(lambda x: 'Nightlife' in x['categories'])

# similarly for Champaign instances

In [18]: buscham=business.filter(lambda x: x['city']=='Champaign')

In [19]: buscham.filter(lambda x: 'Nightlife' in x['categories']).first()
Out[19]: 
{'address': '105 N Market St',
 'attributes': {'Alcohol': 'full_bar',
  'Ambience': {'casual': True,
   'classy': False,
   'divey': False,
   'hipster': False,
   'intimate': False,
   'romantic': False,
   'touristy': False,
   'trendy': False,
   'upscale': False},
  'BestNights': {'friday': True,
   'monday': True,
   'saturday': True,
   'sunday': False,
   'thursday': False,
   'tuesday': False,
   'wednesday': False},
  'BikeParking': True,
  'BusinessAcceptsCreditCards': True,
  'BusinessParking': {'garage': False,
   'lot': False,
   'street': True,
   'valet': False,
   'validated': False},
  'CoatCheck': False,
  'GoodForDancing': False,
  'HappyHour': True,
  'HasTV': True,
  'Music': {'background_music': False,
   'dj': True,
   'jukebox': True,
   'karaoke': False,
   'live': True,
   'no_music': False,
   'video': False},
  'NoiseLevel': 'loud',
  'OutdoorSeating': True,
  'RestaurantsGoodForGroups': True,
  'RestaurantsPriceRange2': 1,
  'RestaurantsReservations': False,
  'Smoking': 'outdoor',
  'WheelchairAccessible': True},
 'business_id': 'OkD42cWX6WjgDeLFdWSgXA',
 'categories': ['Pubs', 'Nightlife', 'Bars'],
 'city': 'Champaign',
 'hours': {'Friday': '16:00-2:00',
  'Monday': '16:00-2:00',
  'Saturday': '16:00-2:00',
  'Sunday': '16:00-2:00',
  'Thursday': '16:00-2:00',
  'Tuesday': '16:00-2:00',
  'Wednesday': '16:00-2:00'},
 'is_open': 0,
 'latitude': 40.1165811599,
 'longitude': -88.2418391481,
 'name': "Mike N Molly's",
 'neighborhood': '',
 'postal_code': '61820',
 'review_count': 32,
 'stars': 4.0,
 'state': 'IL'}

In [20]: bus_night_ch=buscham.filter(lambda x: 'Nightlife' in x['categories'])

#Business RDD's are prepared, next to prepare review which contains key 'useful'

In [21]: rev.first()
Out[21]: 
{'business_id': '0W4lkclzZThpx3V65bVgig',
 'cool': 0,
 'date': '2016-05-28',
 'funny': 0,
 'review_id': 'v0i_UHJMo_hPBq9bxWvW4w',
 'stars': 5,
 'text': "Love the staff, love the meat, love the place. Prepare for a long line around lunch or dinner hours. \n\nThey ask you how you want you meat, lean or something maybe, I can't remember. Just say you don't want it too fatty. \n\nGet a half sour pickle and a hot pepper. Hand cut french fries too.",
 'useful': 0,
 'user_id': 'bv2nCi5Qv5vroFiqKGopiw'}

# useful is filtered and set with a threshold of greater than 1 as a single value could be an error and therefore with 2 is less likely to be an
#error because the usefulness rating is validated by another user.

In [22]: rev.filter(lambda x: x['useful']>1).first()
Out[22]: 
{'business_id': 'fDF_o2JPU8BR1Gya--jRIA',
 'cool': 0,
 'date': '2016-04-06',
 'funny': 0,
 'review_id': 'WF_QTN3p-thD74hqpp2j-Q',
 'stars': 5,
 'text': 'Love this place!\n\nPeggy is great with dogs and does a great job! She is very patience with him and will make any adjustments you need before you leave the store. My little guy has no problems coming here.\n\nThey also have very good bully sticks (the non-smelly ones) that my little guy and his friends love!',
 'useful': 3,
 'user_id': 'u0LXt3Uea_GidxRW1xcsfg'}                                                            

In [23]: revuseful =rev.filter(lambda x: x['useful']>1)

# Re-map key value pairs for businesses in Urbana 

In [24]: busur= bus_night_urb.map(lambda x: (x['business_id'], {'address': x['address'], 'attributes': x['attributes'],'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'],'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'],'postal_code': x['postal_code'], 'review_count': x['review_count'], 'stars': x['stars'], 'state': x['state']}))

In [25]: busur.first()
Out[25]: 
('sOPBbhDCi7bJlW7dNBuITg',
 {'address': '214 W Main St',
  'attributes': {'Alcohol': 'full_bar',
   'BikeParking': True,
   'BusinessAcceptsBitcoin': False,
   'BusinessAcceptsCreditCards': True,
   'BusinessParking': {'garage': False,
    'lot': False,
    'street': True,
    'valet': False,
    'validated': False},
   'CoatCheck': True,
   'DogsAllowed': False,
   'GoodForDancing': False,
   'GoodForKids': False,
   'HappyHour': True,
   'HasTV': True,
   'Music': {'background_music': False,
    'dj': False,
    'jukebox': True,
    'karaoke': False,
    'live': False,
    'no_music': False,
    'video': False},
   'NoiseLevel': 'quiet',
   'OutdoorSeating': False,
   'RestaurantsDelivery': False,
   'RestaurantsGoodForGroups': True,
   'RestaurantsPriceRange2': 2,
   'RestaurantsReservations': True,
   'RestaurantsTableService': True,
   'RestaurantsTakeOut': False,
   'Smoking': 'outdoor',
   'WheelchairAccessible': True,
   'WiFi': 'free'},
  'categories': ['Restaurants',
   'Venues & Event Spaces',
   'Cafes',
   'Lounges',
   'Nightlife',
   'Pool Halls',
   'Event Planning & Services',
   'Pool & Billiards',
   'Dance Clubs',
   'Bars',
   'Shopping',
   'Karaoke'],
  'city': 'Urbana',
  'hours': {'Friday': '18:00-2:00',
   'Saturday': '18:00-2:00',
   'Sunday': '18:00-1:00',
   'Thursday': '18:00-1:00',
   'Wednesday': '18:00-1:00'},
  'is_open': 1,
  'latitude': 40.112708,
  'longitude': -88.209562,
  'name': 'A Plus VIP Lounge',
  'neighborhood': '',
  'postal_code': '61801',
  'review_count': 16,
  'stars': 3.5,
  'state': 'IL'})

#the 'business_id' is now set as the new key, same will be done for Champaign 

In [26]: busch= bus_night_ch.map(lambda x: (x['business_id'], {'address': x['address'], 'attributes': x['attributes'],'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'],'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'],'postal_code': x['postal_code'], 'review_count': x['review_count'], 'stars': x['stars'], 'state': x['state']}))

In [27]: busch.first()
Out[27]: 
('OkD42cWX6WjgDeLFdWSgXA',
 {'address': '105 N Market St',
  'attributes': {'Alcohol': 'full_bar',
   'Ambience': {'casual': True,
    'classy': False,
    'divey': False,
    'hipster': False,
    'intimate': False,
    'romantic': False,
    'touristy': False,
    'trendy': False,
    'upscale': False},
   'BestNights': {'friday': True,
    'monday': True,
    'saturday': True,
    'sunday': False,
    'thursday': False,
    'tuesday': False,
    'wednesday': False},
   'BikeParking': True,
   'BusinessAcceptsCreditCards': True,
   'BusinessParking': {'garage': False,
    'lot': False,
    'street': True,
    'valet': False,
    'validated': False},
   'CoatCheck': False,
   'GoodForDancing': False,
   'HappyHour': True,
   'HasTV': True,
   'Music': {'background_music': False,
    'dj': True,
    'jukebox': True,
    'karaoke': False,
    'live': True,
    'no_music': False,
    'video': False},
   'NoiseLevel': 'loud',
   'OutdoorSeating': True,
   'RestaurantsGoodForGroups': True,
   'RestaurantsPriceRange2': 1,
   'RestaurantsReservations': False,
   'Smoking': 'outdoor',
   'WheelchairAccessible': True},
  'categories': ['Pubs', 'Nightlife', 'Bars'],
  'city': 'Champaign',
  'hours': {'Friday': '16:00-2:00',
   'Monday': '16:00-2:00',
   'Saturday': '16:00-2:00',
   'Sunday': '16:00-2:00',
   'Thursday': '16:00-2:00',
   'Tuesday': '16:00-2:00',
   'Wednesday': '16:00-2:00'},
  'is_open': 0,
  'latitude': 40.1165811599,
  'longitude': -88.2418391481,
  'name': "Mike N Molly's",
  'neighborhood': '',
  'postal_code': '61820',
  'review_count': 32,
  'stars': 4.0,
  'state': 'IL'})

#Now to append Urbana set and Champaign set using union function instead of join

In [28]: busur.union(busch).take(2)
Out[28]: 
[('sOPBbhDCi7bJlW7dNBuITg',
  {'address': '214 W Main St',
   'attributes': {'Alcohol': 'full_bar',
    'BikeParking': True,
    'BusinessAcceptsBitcoin': False,
    'BusinessAcceptsCreditCards': True,
    'BusinessParking': {'garage': False,
     'lot': False,
     'street': True,
     'valet': False,
     'validated': False},
    'CoatCheck': True,
    'DogsAllowed': False,
    'GoodForDancing': False,
    'GoodForKids': False,
    'HappyHour': True,
    'HasTV': True,
    'Music': {'background_music': False,
     'dj': False,
     'jukebox': True,
     'karaoke': False,
     'live': False,
     'no_music': False,
     'video': False},
    'NoiseLevel': 'quiet',
    'OutdoorSeating': False,
    'RestaurantsDelivery': False,
    'RestaurantsGoodForGroups': True,
    'RestaurantsPriceRange2': 2,
    'RestaurantsReservations': True,
    'RestaurantsTableService': True,
    'RestaurantsTakeOut': False,
    'Smoking': 'outdoor',
    'WheelchairAccessible': True,
    'WiFi': 'free'},
   'categories': ['Restaurants',
    'Venues & Event Spaces',
    'Cafes',
    'Lounges',
    'Nightlife',
    'Pool Halls',
    'Event Planning & Services',
    'Pool & Billiards',
    'Dance Clubs',
    'Bars',
    'Shopping',
    'Karaoke'],
   'city': 'Urbana',
   'hours': {'Friday': '18:00-2:00',
    'Saturday': '18:00-2:00',
    'Sunday': '18:00-1:00',
    'Thursday': '18:00-1:00',
    'Wednesday': '18:00-1:00'},
   'is_open': 1,
   'latitude': 40.112708,
   'longitude': -88.209562,
   'name': 'A Plus VIP Lounge',
   'neighborhood': '',
   'postal_code': '61801',
   'review_count': 16,
   'stars': 3.5,
   'state': 'IL'}),
 ('Lwb6bG1Qu3BbW7FJj5suLw',
  {'address': '123 W Main St, Ste 104',
   'attributes': {'BikeParking': True,
    'BusinessAcceptsCreditCards': True,
    'BusinessParking': {'garage': True,
     'lot': False,
     'street': False,
     'valet': False,
     'validated': False},
    'ByAppointmentOnly': True,
    'GoodForKids': True},
   'categories': ['Arts & Entertainment',
    'Nightlife',
    'Active Life',
    'Escape Games'],
   'city': 'Urbana',
   'hours': {'Friday': '9:00-22:00',
    'Monday': '9:00-22:00',
    'Saturday': '10:00-0:00',
    'Sunday': '11:00-22:00',
    'Thursday': '9:00-22:00',
    'Tuesday': '9:00-18:00',
    'Wednesday': '9:00-22:00'},
   'is_open': 1,
   'latitude': 40.1121494,
   'longitude': -88.2082725,
   'name': 'Champaign-Urbana Adventures in Time & Space',
   'neighborhood': '',
   'postal_code': '61801',
   'review_count': 36,
   'stars': 5.0,
   'state': 'IL'})]

In [29]: business_uc=busur.union(busch)

#Next the usefulness is ranked in order from most useful votes to least, take 3 verifies this

In [30]: revuseful.sortBy(lambda x: x['useful'], ascending=False ).take(3)

In [31]: rev_use_sorted=revuseful.sortBy(lambda x: x['useful'], ascending=False )

#Mutual key of business_id is formed by creating a new RDD

In [32]: rev_ordered=rev_use_sorted.map(lambda x: (x['business_id'], {'funny': x['funny'], 'cool': x['cool'], 'date': x['date'], 'review_id': x['review_id'],'stars': x['stars'], 'text': x['text'], 'useful': x['useful'], 'user_id': x['user_id']}))

In [33]: final_RDD=rev_ordered.join(business_uc)

# to find the top 10

In [34]: final_RDD.take(10)

In [35]: final_RDD.count()
Out[35]: 1451                                                                   

# 1451 instances of those reviews with top 10 useful rating
# To obtain the top 10 reviewers names, the useful and business rdds must be joined

In [36]: user= sc.textFile('/user/msc/datasets/yelp/user.json').map(lambda x: json.loads(x))

#ranking of userful

In [37]: user_sorted=user.sortBy(lambda x: x['useful'], ascending=False )

#setting review count as new key

In [38]: user_rdd=user_sorted.map(lambda x: (x['review_count'], {
    ...: 'average_stars': x['average_stars'], 'compliment_cool': x['compliment_cool'], 'compliment_cute': x['compliment_cute'], 'compliment_funny': x['compliment_funny'], 'compliment_hot': x['compliment_hot'], 'compliment_list': x['compliment_list'], 'compliment_more': x['compliment_more'],'compliment_note': x['compliment_note'], 'compliment_photos': x['compliment_photos'],'compliment_plain': x['compliment_plain'], 'compliment_profile': x['compliment_profile'],'compliment_writer': x['compliment_writer'], 'cool': x['cool'], 'elite': x['elite'], 'fans': x['fans'], 'friends': x['friends'], 'funny': x['funny'], 'name': x['name'], 'useful': x['useful'],'user_id': x['user_id'], 'yelping_since': x['yelping_since']}))

In [39]: user_rdd.first()

#setting business,nightlife,champaign & urbana keys to review count

In [40]: busch_rev= bus_night_ch.map(lambda x: (x['review_count'], {'business_id': x['business_id'],'address': x['address'], 'attributes': x['attributes'],'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'],'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'],'postal_code': x['postal_code'], 'stars': x['stars'], 'state': x['state']}))

In [41]: busch_rev.first()

In [42]: busur_rev= bus_night_urb.map(lambda x: (x['review_count'], {'business_id': x['business_id'],'address': x['address'], 'attributes': x['attributes'],'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'],'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'],'postal_code': x['postal_code'], 'stars': x['stars'], 'state': x['state']}))

In [43]: busur_rev.first()

#merging urbana and champaign

In [44]: bus_rev=busur_rev.union(busch_rev)

In [45]: finished_rdd=user_rdd.join(bus_rev)

In [46]: finished_rdd.first()

# final showing top 10 reviewers

In [47]: finished_rdd.take(10)


# 2 - Exploratory Data Science Question
# Which State out of Arizona and Wisconsin has the greatest percentage of users that have liked the tips given for restaurants that are also rated greater than 4 stars?

In [48]: business.filter(lambda x: 'Restaurants' in x['categories']).count()
Out[48]: 54618    

#shows the total number of businesses that are considered as Restaurants

In [49]: business.filter(lambda x: x['stars']>4).filter(lambda x: x['state']=='AZ').filter(lambda x: 'Restaurants' in x['categories']).count()
Out[49]: 1412 

# 1412 instances from Arizona state with over 4 star rating

In [50]: bus_az=business.filter(lambda x: x['stars']>4).filter(lambda x: x['state']=='AZ').filter(lambda x: 'Restaurants' in x['categories'])

In [51]: bus_wi=business.filter(lambda x: x['stars']>4).filter(lambda x: x['state']=='WI').filter(lambda x: 'Restaurants' in x['categories'])

In [52]: bus_wi.count()
Out[52]: 200   

# Comparitively only 200 from Wisconsin

In [53]: business_az= bus_az.map(lambda x: ( x['business_id'], {'address': x['address'], 'attributes': x['attributes'],'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'],'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'],'postal_code': x['postal_code'], 'review_count': x['review_count'], 'stars': x['stars'], 'state': x['state']}))

In [54]: business_az.first()
Out[54]: 
('YhV93k9uiMdr3FlV4FHjwA',
 {'address': '',
  'attributes': {'BusinessAcceptsBitcoin': False,
   'BusinessAcceptsCreditCards': True,
   'RestaurantsDelivery': False,
   'RestaurantsReservations': False},
  'categories': ['Marketing',
   "Men's Clothing",
   'Restaurants',
   'Graphic Design',
   "Women's Clothing",
   'Screen Printing',
   'Advertising',
   'Pizza',
   'Shopping',
   'Web Design',
   'Fashion',
   'Local Services',
   'Screen Printing/T-Shirt Printing',
   'Professional Services'],
  'city': 'Phoenix',
  'hours': {'Friday': '8:00-17:00',
   'Monday': '8:00-17:00',
   'Saturday': '9:00-15:00',
   'Thursday': '8:00-17:00',
   'Tuesday': '8:00-17:00',
   'Wednesday': '8:00-17:00'},
  'is_open': 1,
  'latitude': 33.4499672,
  'longitude': -112.0702225,
  'name': 'Caviness Studio',
  'neighborhood': '',
  'postal_code': '85001',
  'review_count': 4,
  'stars': 5.0,
  'state': 'AZ'})

# Setting the mutual joining attribute as business_id

In [55]: business_wi= bus_wi.map(lambda x: ( x['business_id'], {'address': x['address'], 'attributes': x['attributes'],'categories': x['categories'], 'city': x['city'], 'hours': x['hours'], 'is_open': x['is_open'],'latitude': x['latitude'], 'longitude': x['longitude'], 'name': x['name'], 'neighborhood': x['neighborhood'],'postal_code': x['postal_code'], 'review_count': x['review_count'], 'stars': x['stars'], 'state': x['state']}))

In [56]: business_wi.first()
Out[56]: 
('rnu3iUdqqNwcQS5uSeZFTQ',
 {'address': '135 S Main St, Ste A',
  'attributes': {'Alcohol': 'none',
   'BusinessAcceptsCreditCards': True,
   'BusinessParking': {'garage': False,
    'lot': False,
    'street': False,
    'valet': False,
    'validated': False},
   'Caters': True,
   'GoodForKids': True,
   'GoodForMeal': {'breakfast': False,
    'brunch': False,
    'dessert': False,
    'dinner': False,
    'latenight': False,
    'lunch': False},
   'HasTV': False,
   'NoiseLevel': 'quiet',
   'OutdoorSeating': False,
   'RestaurantsAttire': 'casual',
   'RestaurantsDelivery': False,
   'RestaurantsPriceRange2': 1,
   'RestaurantsReservations': False,
   'RestaurantsTableService': False,
   'RestaurantsTakeOut': True,
   'WiFi': 'no'},
  'categories': ['Restaurants', 'Italian', 'Delis'],
  'city': 'Oregon',
  'hours': {'Friday': '10:00-18:00',
   'Monday': '10:00-19:00',
   'Saturday': '10:00-16:00',
   'Thursday': '10:00-19:00',
   'Tuesday': '10:00-19:00',
   'Wednesday': '10:00-19:00'},
  'is_open': 0,
  'latitude': 42.9256,
  'longitude': -89.384996,
  'name': "Alberici's Delicatezza",
  'neighborhood': '',
  'postal_code': '53575',
  'review_count': 3,
  'stars': 4.5,
  'state': 'WI'})

# next to read the tips file

In [57]: tip= sc.textFile('/user/msc/datasets/yelp/tip.json').map(lambda x: json.loads(x))

In [58]: tip.first()
Out[58]: 
{'business_id': 'tJRDll5yqpZwehenzE2cSg',
 'date': '2012-07-15',
 'likes': 0,
 'text': 'Get here early enough to have dinner.',
 'user_id': 'zcTZk7OG8ovAmh_fenH21g'}

#inspection shows business_id can be merged on using join

In [59]: tip.filter(lambda x: x['likes']>0).count()
Out[59]: 16284

# identification of instances of likes 

In [60]: tiplike=tip.filter(lambda x: x['likes']>0)

In [61]: tipl_rdd=tiplike.map(lambda x: (x['business_id'], {'date': x['date'], 'likes': x['likes'], 'text': x['text'], 'user_id': x['user_id']}))

#setting the new key

In [62]: tipl_rdd.first()
Out[62]: 
('lkpoSg7xf60BsrOjdm0CBA',
 {'date': '2015-07-25',
  'likes': 1,
  'text': 'Even if their hours say its closed, sometimes they stay open late at night.  When you get out of a movie at midnight and need a donut, this could be your fix!',
  'user_id': 'QGgWWhEi5R4SLAKN-xwtNQ'})

#next is to join (while checking instances that satisfy the new joined RDD)

In [63]: business_wi.join(tipl_rdd).count()
Out[63]: 21                                                                    

In [64]: bus_wi_tip=business_wi.join(tipl_rdd)

In [65]: business_az.join(tipl_rdd).count()
Out[65]: 359                                                                   

In [66]: bus_az_tip=business_az.join(tipl_rdd)

# Calculation of the percentages

In [67]: per_wi=(bus_wi_tip.count()/business_wi.count())*100
                                                                                
In [68]: per_wi
Out[68]: 10.5

In [69]: per_az=(bus_az_tip.count()/business_az.count())*100
                                                                                
In [70]: per_az
Out[70]: 25.424929178470258

##therefore arizona has a greater percentage of people who liked their reviewed restaurant with tip provided which 

## had over a 4* review (25.4%), compared to wisconsin (10.5%) this could indicate for a better restaurant experience 

#an individual could identify restaurants to be more popular in Arizona which could be a reason their tips gained more likes.






