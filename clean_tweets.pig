/*
 * run by "$pig -x local clean_tweets.pig" if file in local node instead of HDFS.
 */

tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data/z_700' USING PigStorage('\t') AS  (id:CHARARRAY, text:CHARARRAY);

-- Remove URLs
urls_clean = FOREACH tweets GENERATE id, REPLACE(text, '(http://\\S+)', '') AS url_clean;

-- Remove non-characters
nonchars_clean = FOREACH urls_clean GENERATE id, REPLACE(url_clean, '([^a-zA-Z0-9\\s]+)', '') AS nonchar_clean;

-- Remove profanity words)
profanities_clean = FOREACH nonchars_clean GENERATE id, 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE( 
	REPLACE(nonchar_clean, 'shit', ''),
	'fuck', '' ), 
	'damn', ''), 
	'bitch', ''), 
	'crap', ''), 
	'piss', ''), 
	'dick', ''), 
	'darn', ''), 
	'cock', ''), 
	'pussy', ''), 
	'asshole', ''), 
	'fag', ''), 
	'bastard', ''), 
	'slut', ''), 
	'douche', '' 
	) AS tweet_clean;

-- Remove empty rows
empty_rows_clean = FILTER profanities_clean BY (id MATCHES '541-.*' OR id MATCHES '602-.*' OR id MATCHES '668-.*' OR id MATCHES '686-.*' OR id MATCHES '694-.*' OR id MATCHES '700-.*');
empty_rows_clean = DISTINCT empty_rows_clean;

STORE empty_rows_clean INTO 'cleaned_tweets';

