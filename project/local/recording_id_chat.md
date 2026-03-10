PROMPT:

Can you explain to me where and how the recording_id in our track_info table is generated if the raw data dump does not contain one? There are a lot of values with underscores at the start of the key, which suggests to me the first part of whatever we were concatenating is simply missing. Not sure if this is something we need to remedy or not

RESPONSE:

I’m tracing how `recording_id` is populated from the ingest code and whether the underscore-prefixed values are expected or a bug. I’ll inspect the parser and upsert path first, then I’ll map that back to what ends up in `track_info`.

Searched for regex `recording_id|track_info|parse_dump|mbid|concat|join` (`**/project/local/**`), 200 results

Searched for files matching `**/project/local/**/*.py`, 6 matches

Read memory

I found the current parser line that creates `recording_id` synthetically, and it does explain the leading underscores. I’m reading the surrounding code and the earlier ingestion version to determine whether this was an intentional fallback or an accidental replacement of a real MusicBrainz recording ID.

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/pipeline.py#100-100), lines 100 to 150

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/pipeline.py#1-1), lines 1 to 110

Retrieving Notebook summary.

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/listenbrainz_aws_db_ingest.py#248-248), lines 248 to 272

The current code is not extracting a real recording MBID at all. I’m checking the schema/upsert boundary and the earlier notebook version to confirm whether `recording_id` was repurposed from `recording_mbid` during a refactor.

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/pipeline.py#235-235), lines 235 to 285

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/listenbrainz_local_ingest.ipynb#605-605), lines 605 to 646

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/listenbrainz_aws_db_ingest.py#228-228), lines 228 to 290

I confirmed both active ingestion paths use the same synthetic key. One more pass on the older notebook artifact will show whether you originally had a real `recording_mbid` extractor and later dropped it.

Searched for regex `recording_mbid|Create recording_id from artist_mbids|missing_recording_mbid` (`**/project/local/listenbrainz_local_ingest.ipynb`), 5 results

Searched for regex `recording_mbid|Recording_mbid` (`**/project/local/listenbrainz_ingest-week7.html`), 23 results

Read [](file:///c%3A/Users/Natha/source/repos/Classes/DE_300/project/local/pipeline.py#60-60), lines 60 to 160

Yes. In the current ingest code, recording_id is not coming from the raw dump at all. It is being synthesized inside parse_dump as:

f"{'_'.join(artist_mbids)}_{track_name}"

You can see that in pipeline.py through pipeline.py. The flow is:

1. Read track_metadata.additional_info.artist_mbids
2. Read track_name
3. Build recording_id by joining the artist MBIDs with underscores and then appending an underscore plus the track name
4. Use that synthetic value as the primary key for both track_daily and track_info
5. Upsert it into track_info as the table key in pipeline.py through pipeline.py

That explains the underscore-prefixed keys. When artist_mbids is empty, this part:

'_'.join(artist_mbids)

becomes the empty string, so the generated key becomes:

_{track_name}

So a value like _Some Song means there was no artist MBID available for that listen, and the code still created a track key from the separator plus the track name. If track_name were also missing, you could even end up with values like _None.

More importantly, this looks like a regression from your earlier version, not a deliberate use of a canonical track identifier. The older ingest artifact shows the parser originally tried to read a real MusicBrainz recording ID from additional_info:

recording_mbid = get_any(add, "recording_mbid", "Recording_mbid")

and then only used that when present. You can see that in listenbrainz_ingest-week7.html through listenbrainz_ingest-week7.html. That same older version also tracked missing_recording_mbid, which is another sign that the original design expected a real recording MBID and treated missing values as a data quality issue rather than fabricating a replacement key.

So to your “do we need to remedy this?” question: probably yes, if you care about track identity being correct and stable.

The current synthetic key has a few problems:
- It is not canonical. It is just a heuristic string.
- It can collide. Different recordings can share the same artist MBIDs plus track name.
- It can fragment the same recording. Small variations in artist_mbids ordering, missing artists, punctuation, or track title normalization will produce different keys.
- The underscore-prefixed rows specifically tell you the artist side of the synthetic key is incomplete.

Practically, I would treat the current recording_id column as a fallback track_key, not as a true recording ID. The safer model is:

1. Use recording_mbid from the raw dump when present.
2. Only fall back to a synthetic key when recording_mbid is missing.
3. Store both:
   - recording_mbid: nullable canonical ID
   - track_key or synthetic_recording_id: fallback grouping key
4. Optionally add a boolean like is_synthetic_key so you can measure how much of the table is inferred rather than sourced.

If you want, I can next trace whether the raw ListenBrainz dumps you are ingesting still contain recording_mbid in additional_info and show exactly how to patch the parser to prefer that real ID while keeping a fallback for missing cases.