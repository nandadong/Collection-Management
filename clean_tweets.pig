/*
 * un by "$pig -x local clean_tweets.pig" if file in local node instead of HDFS.
 */

tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data/z_541/part-m-00000' USING PigStorage('\t') AS  (id:CHARARRAY, text:CHARARRAY);

-- Remove urls
urls = FOREACH tweets GENERATE id, REPLACE(text, '(http://\\S+)', '');

-- Remove non-characters
nonchar_clean = FOREACH urls_clean GENERATE id, REPLACE(url_clean, '([^a-zA-Z0-9\\s]+)', '');

-- Remove profanity words (currently just top 15 in collection)
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
	) AS profanity_clean;

-- Remove empty rows
empty_rows_clean = FILTER profanities_clean BY id MATCHES '700-.*';

STORE empty_rows_clean INTO 'cleaned_tweets';
																														
