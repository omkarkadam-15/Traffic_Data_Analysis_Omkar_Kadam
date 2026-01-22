# Traffic_Data_Analysis_Omkar_Kadam (PySpark)
This project performs a complete ETL (Extract–Transform–Load) workflow and Exploratory Data Analysis (EDA) on California statewide traffic collision data. Using PySpark, the analysis focuses on collision severity, weather and lighting conditions, victim demographics, geographic patterns, and time-based trends to support data-driven decisions for traffic safety and urban planning.
## Objectives
* Clean, transform, and standardize collision, parties, and victims data.
* Handle missing values, inconsistent types, duplicates, and outliers.
* Encode categorical features for analytics and ML readiness.
* Analyze collision severity, environmental factors, and victim impact.
* Explore temporal, spatial, and demographic patterns.
* Provide insights for policy, public safety, and infrastructure improvement.

## Dataset
The project uses multiple datasets:
1. collisions_df
2. parties_df
3. victims_df
4. case_id_df

Each dataset contains structured information on collisions, vehicle parties involved, victims, and case identifiers.

## ETL Workflow
1. Data Loading & Type Consistency
* Ensured correct formatting for large integer IDs (e.g., case_id set to DecimalType(22,0)) to avoid type inference issues.
* Converted date, numeric, and string fields to appropriate types. 

2. Missing Value Treatment
* Dropped columns with >50% missing values (e.g., reporting_district, caltrans_county, postmile).
* Imputed missing values:
* Numeric → 0
* String → 'Unknown'
* Timestamp → earliest time value. 

3. Data Cleaning
* Removed duplicates across all dataframes.
* Applied IQR-based outlier detection on victims, parties, vehicle attributes, and collision metrics.
* Standardized critical fields such as weather, lighting, and collision types. 

4. Encoding

Applied StringIndexer on categorical fields to prepare data for ML or statistical modeling (weather, collision severity, road conditions, etc.). 

## Exploratory Data Analysis (EDA)
1. Collision Severity
* Most collisions result in property damage only (570,479).
* Severe injuries, though fewer (22,138), represent critical safety concerns.

2. Weather Conditions
* Majority of collisions happen in clear weather (769,926), showing human/traffic factors dominate.
* Cloudy and rainy conditions contribute significantly to injury-related collisions.

3. Victim Demographics
* Victim ages 17–30 form the highest-risk group.
* High vulnerability observed among infants aged 0–1 (38,704).

4. Lighting Effects
Most collisions occur in daylight, but severe injuries are more common in dark conditions with/without streetlights.

5. Temporal Patterns
* Friday has the highest collision count (154,543).
* Peak hours are 3 PM – 6 PM, with 5 PM being the most dangerous.
* October, March, and December show higher collision frequencies.

6. Geospatial Insights
* High-density collision zones include:
Los Angeles, Orange County, San Bernardino, San Diego, Riverside, Bay Area counties also show elevated collision activity.

Long-Term Trends

Collisions declined from 2006 to 2013, increased until 2018, and dipped during 2020 (COVID).

## ETL Querying Examples

### Key analytical queries include:
* Top 5 counties by collisions
* Month with highest collision count (October – 83,274)
* Weather conditions most associated with collisions (clear weather)
* Most dangerous hour (5 PM – 73,255 collisions)

## Tools & Technologies
* PySpark (ETL, cleaning, joining, transformations)
* Python (EDA, visualizations)
* Matplotlib / Seaborn
* Tableau / Power BI dashboards
* Geo-spatial mapping (for county-level and statewide analysis)

## Key Insights

Collisions are highest in dense urban counties and along major freeways.

Environmental factors (rain, darkness) increase injury severity even if frequency is lower.

Younger age groups are disproportionately affected.

Rush hours and Fridays are the highest-risk time periods.
