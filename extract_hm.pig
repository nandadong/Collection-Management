-- Data loading
tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data' USING PigStorage('\t') AS  (id:CHARARRAY, text:CHARARRAY);

-- Remove invalid rows
tweets = FILTER tweets BY (id MATCHES '541-.*' OR id MATCHES '602-.*' OR id MATCHES '668-.*' OR id MATCHES '686-.*' OR id MATCHES '694-.*' OR id MATCHES '700-.*');

-- Remove non-characters except for # and @
tweets = FOREACH tweets GENERATE id, REPLACE(text, '([^a-zA-Z0-9#@\\s]+)', '') AS text;

-- First finding hashtags(#)
-- Tokenize each tweet message
tweetwords = FOREACH tweets GENERATE id, FLATTEN( TOKENIZE(text) ) AS word;

-- Search for only hashtags in the tweets
hashtags = FILTER tweetwords BY UPPER(word) MATCHES '#\\s*(\\w+)';

STORE hashtags INTO 'hashtags';

-- Same process as hashtags, finding mentions(@) 
mentions = FILTER tweetwords BY UPPER(word) MATCHES '@\\s*(\\w+)';

STORE mentions INTO 'mentions';
