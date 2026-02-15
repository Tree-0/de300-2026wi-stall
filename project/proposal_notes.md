figure out details for *how* you will do partial updates

- we care about metadata, not the raw music data itself
- when doing analyses, 
    - How will you define popularity? spotify has a metric, but we need other measures or cross-references

- start with database design
    - rules of inclusion and exclusion
    - how do you reconcile between data sets? naming conventions, different languages, etc

- lastfm does have data dump
    - double check for newer data dump -> make sure it's not the 2011 data

- when specifying metrics, building model for what is trending...
    - specify how those indicators are calculated and stored BEFORE you start the model

- grab 5000 songs, and do the whole thing end-to-end BEFORE scaling


- think about WHEN pyspark is appropriate -> some of these things may not need PySpark