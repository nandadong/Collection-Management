-- Data loading
tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data/z_700/part-m-00000' USING PigStorage('\t') AS  (id:INT, text:CHARARRAY);

-- First finding hashtags(#)
-- Tokenize each tweet message
tweetwords = FOREACH tweets GENERATE FLATTEN( TOKENIZE(text) ) AS word;

-- Search for only hash tags in the tw
hashtags = FILTER tweetwords BY UPPER(word) MATCHES '#\\s*(\\w+)';

-- Groups each hash tag
taggroups = group hashtags by word;

-- Count the occurrence of each hash tag
tagcount = FOREACH taggroups GENERATE group AS tags, COUNT( hashtags ) AS count;

-- Order by no of occurrence of each hash tag
tagorder = ORDER tagcount BY count DESC;

STORE tagorder INTO 'hashtags';

-- Same process as hashtags, finding mentions(@) 
mentions = FILTER tweetwords BY UPPER(word) MATCHES '@\\s*(\\w+)';

mentiongroups = group mentions by word;

mentioncount = FOREACH mentiongroups GENERATE group AS mens, COUNT( mentions) AS count;

mentionorder = ORDER mentioncount BY count DESC;

STORE mentionorder INTO 'mentions';

