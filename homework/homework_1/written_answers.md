# Written Answers
Nathaniel Stall
alt5629

As a note, much of my reasoning is also in the jupyter notebook containing my code, for easier association between my code and rationale, but my reasoning is condensed and clearer here.

## 1. Imputation choices
- `CARRIER`
    - Some `CARRIER` values for North American Airlines were NaN, some were "NA". "NA" is presumably the correct value, so I chose to impute the missing columns with a constant value.

- `CARRIER_NAME`
    - There are some missing `CARRIER_NAME` fields for rows where `CARRIER` equals `L4` or `OH`. Upon inspecting the data, `L4` corresponds exclusively to the carrier name `"Lynx Aviation d/b/a Frontier Airlines"`, so I imputed that constant value. However, `OH` carriers had 2 possible values: `Comair Inc.` and `PSA Airlines Inc.`. 
    - `PSA Airlines Inc.` rows had predominantly `UNIQUE_CARRIER_CODE` = `OH`, and `Comair Inc.` had some codes equal to `OH (1)`. I decided to standardize all `Comair Inc.` unique carrier codes to be `OH (1)` to differentiate them from `PSA Airlines` unique codes, although this wasn't used later in the assignment.
    - I opted not to impute the remaining missing `CARRIER_NAME` entries because I could not determine a reliable way to decide between which rows should be considered `Comair Inc.` and which should be `PSA Airlines Inc.` and did not want to fabricate data.

- `MANUFACTURE_YEAR`
    - There are only 3 missing `MANUFACTURE_YEAR` values. I attempted to correlate `YEAR` and `MANUFACTURE_YEAR` with a simple linear regression to see if I could estimate the `MANUFACTURE_YEAR`, but there was far too much variance from the estimated line of best fit, so I opted to drop the 3 rows instead since they were negligible compared to the size of the full dataset.

- `NUMBER_OF_SEATS`
    - There are 7 missing `NUMBER_OF_SEAT` entries, all associated with Boeing. The model names of the aircraft included "CARGO", hinting at a seat count of 0. Cross referencing the model online (https://flycraft.com/aircraft/n317cm) also listed 0 seats. I decided to impute a constant value of 0 for those fields.

- `CAPACITY_IN_POUNDS`
    - There are 101 missing `CAPACITY_IN_POUNDS` entries. I decided to find all the models which had at least one missing capacity entry. For almost every model, the mean and the median `CAPACITY_IN_POUNDS` were very similar, implying models were by majority the median capacity. Because of this, it felt reasonable to me to impute missing values by using the median per-model capacity value. 

- `AIRLINE_ID`
    - For similar reasons as `CARRIER_NAME`, I was able to impute `AIRLINE_ID` = 21217 for entries with a `CARRIER` of `L4`, but unable to impute `AIRLINE_ID` for `OH` carriers due to uncertainty between `Comair` and `PSA` aircraft. 

## 2. Data Transformation
- `MANUFACTURER`
    - Upon inspecting the data, there were tons of inconsistencies. For example:
        - boeing, Boeing, BOEING, TheBoeingCompany, THEBOEINGCOMPANY, etc.. --> BOEING
        - airbus, Airbus, AIRBUS, AirbusIndustrie, AIRBUSINDUSTRIES, etc.. --> AIRBUS
    - I standardized this by making strings case insensitive, unifying as many aliases as possible by defining some functions for searching for substrings and replacing the matching columns, and removing special characters. This took care of all of the big names, and a few of the small ones, but there are probably smaller manufacturers with only 1 or 2 aircraft per variation of the manufacturer name that I missed.

- `MODEL`
    - I found `MODEL` to be much messier when it came to identifying what model names should be unified, so I again decided to remove special characters and make values case insensitive, which did reduce the number of values.
    - Besides that standardization, it was difficult to tell whether similar names were typos or truly distinct models, so I chose to err on the side of caution and leave the resulting model names as is. 

- `AIRCRAFT_STATUS`
    - I standardized `AIRCRAFT_STATUS` by making values case insensitive. This seemed reasonable as there were only 4 distinct letters.

- `OPERATING_STATUS`
    - `OPERATING_STATUS` presents as a binary decision variable, so I transformed `y` -> `Y` and dropped the single row that had an empty string as the status. This resulted in a clear `Y\N` binary value.

## 3. Dropping Remaining Values

After dropping the remaining values post-imputation and standardization:
- `101,292` entries remaining out of the initial `132,313`
- `76.55%` of the data was retained.

## 4. Transforming and Derivative Variables
- The pre and post-transformation histograms can be found under the "Task 4" section of the jupyter notebook.
- Note: Boxcox requires positive values in order to apply the transformation. Since there are a large number of values of 0 (~19k out of ~115k total entries), I chose to shift them by 1 rather than drop the rows so that they could still be visualized.
- Before transforming, both columns have a right-skewed distribution. After transforming, `NUMBER_OF_SEATS` is still rather right-skewed, especially when including the high volume of `0` seat values likely associated with non-commercial aircraft. Meanwhile, `CAPACITY_IN_POUNDS` becomes fairly normalized both with and without including `0` values. 

## 5. Feature Engineering
- The final barplot for operating status can be found under the "Plot operating vs. not operating per size group" section in the jupyter notebook.
- Findings: More than 95% of aircraft in each `SIZE` bin are active. There is also a small correlation between the size of the aircraft and the proportion that are active. In order of least to greatest proportion of active aircraft: 
    - `SMALL` ~ 95.3%
    - `MEDIUM` ~ 96.0%
    - `LARGE` ~ 96.7%
    - `XLARGE` ~ 96.9%

- The final stacked barplot for aircraft status can be found under the "Plot aircraft status per size group" section under "Task 5" in the jupyter notebook. 
- Findings:
    - Status `O` is the most prevalent overall, followed by `B`, `A`, and then `L`. `XLARGE` aircraft have the highest proportion of status `O` at 72.6%, `MEDIUM` aircraft have the highest proportion of `B` at 41.2%, and `LARGE` aircraft have the highest proportion of `A` at 11.6%. Each bin has less than 0.02% of their aircrafts with status `L`, with `SMALL` aircrafts having none at all. 
    - `MEDIUM` and `XLARGE` have very similar proportions of status `A` (~7%), and `LARGE` and `XLARGE` aircraft have very similar proportions of status `B` (~19.5%).
