# Guide to save data in collections

## clearTweet:
    
* ```id_str``` (tweet's id)
* isQuote (calculated for me)
* quote to (in ```quoted_status_id_str```)
* isReply (calculated for me)
* reply to (in ```in_reply_to_status_id_str``` --> **CHECK IF IT IS OK**)
* full_text
* number of terms
* number of characters (```display_text_range[1]``` + ```extended_tweet.display_text_range[1]```)
* hashtags (saved in ```entities.hashtags```)
* mensions (saves in ```entities.user_mentions```):
    * ```screen_name```
    * ```id_str (user's id)```
    * ```indices``` (what is it???)
    * ```id``` (user's id like a number)
    * ```name```
* symbols (saved in ```entities.symbols```)
* media (saved in ```entities.media```):
    * list of medias:
        * ```expanded_url```
        * ```indices```
        * ```sizes```
        * ```media_url```
        * ```type (example --> photo)```
        * ```display_url```
        * ```media_url_https```
        * ```url```
        * ```id```
        * ```id_str```
* ```reply_count```
* ```retweet_count```
* ```quote_count```
* ```favorite_count```
* ```lang```
* isLong (in truncated)
* ```coordinates```
* date (in ```created_at``` --> **check format**)
* ```user```:
    * Many data, look documentation, some usefull things:
        * ```friends_count```
        * ```geo_enabled``` (true or false)
        * ```listed_count```
        * ```location```
        * ```followers_count```
        * ```verified``` (true or false)
        * ```id_str```
        * ```lang```
        * ```screen_name```
        * ```statuses_count```
            * ```created_at``` --> **check format**
        
    