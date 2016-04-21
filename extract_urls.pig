/*
 * run by "$pig -x local extract_urls.pig" if file in local file system.
 */

-- Data loading
tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data' USING PigStorage('\t') AS  (id:CHARARRAY, text:CHARARRAY);

-- Remove invalid rows
tweets = FILTER tweets BY (id MATCHES '541-.*' OR id MATCHES '602-.*' OR id MATCHES '668-.*' OR id MATCHES '686-.*' OR id MATCHES '694-.*' OR id MATCHES '700-.*');

-- Extract URL using regular expression
tweetwords = FOREACH tweets GENERATE id, FLATTEN( TOKENIZE(text) ) AS word;
urls = FILTER tweetwords BY word MATCHES '(http://|https://)\\S+';

-- Trim unnecessary characters
urls_clean = FOREACH urls GENERATE id, REPLACE(word, '([^a-zA-Z0-9_.#@&:/\\s]+)', '...') AS url;

-- Store clean URLs
STORE urls_clean INTO 'urls';

double_urls = FOREACH urls_clean GENERATE id, REGEX_EXTRACT(url, '(http.*)(http\\S+)', 1) AS url1, REGEX_EXTRACT(url, '(http.*)(http\\S+)', 2) AS url2;
double_urls = FILTER double_urls BY ($1 IS NOT NULL);

-- Store double URLs 
STORE double_urls INTO 'double_urls';

