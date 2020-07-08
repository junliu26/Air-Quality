# Wildfire Impact
Living in the Bay Area, we suffered from bad air quality when wildfire happened. According to news report, the lives we lost because of smokes caused by wildfire is more than the lives lost in wildfire itself.This project aim to estimate the populations impacted by high level of PM2.5 caused by wildfire.

***

 National Wild Fire Impact                             | California Wild Fire Impact
:-----------------------------------------------------:|:------------------------------------------------:
![population_impacted_by_wildfire](/img/national.png)  |  ![population_impacted_by_wildfire](/img/CA.png)

***

## Table of Contents
1. [Overview](README.md#Overview)
1. [Data Sets](README.md#Data Sets)
1. [Pseudo code](README.md#Pseudo code)
1. [Tech Stack](README.md#Tech-Stack)
1. [Engineering Details](README.md#Engineering-Details)
1. [Next Step](README.md#Next Step)

## Overview
According to wikipeida, there are more than 35 times of wildfires that burned more than 1,000 acres (400 ha), or produced significant structural damage or casualties in California in 2019.

[2019 California wildfires](https://en.wikipedia.org/wiki/2019_California_wildfires)

Smoke of wildfire not only cause death directly as linked, but also increase the hopitalization and has long term impact on people health.

[US wildfire smoke deaths could double by 2100](https://www.sciencedaily.com/releases/2018/09/180910142417.htm)

[The hidden cost of wildfires: Economic valuation of health effects of wildfire smoke exposure in Southern California](https://lynceans.org/wp-content/uploads/2020/01/Richardson-2012-Fire-smoke-in-future.pdf)

Among all the particle pollution produced by wildfire, PM 2.5 is the most important and harmful to human health, that is the reason I pick it as parameter in this project. 

[Particle Pollution From Wildfires Is a Big Problem for California](https://www.sierraclub.org/sierra/particle-pollution-wildfires-big-problem-for-california)

[Wildfire-specific Fine Particulate Matter and Risk of Hospital Admissions in Urban and Rural Counties](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5130603/)

<hr/>

![PM 2.5](/img/PM2_5_enter.png)

<hr/>

By tracking the time ships spend in ports and scaling by their size, Smoke Stack provides an estimate of ship emissions and the basis for tailored models to match with ship protocols and port regulations. Coupled with local air quality sensor data, Smoke Stack is a powerful platform for tracking emissions, evaluating mitigation efforts, and quantifying results.

[Demo Presentation Slides](https://docs.google.com/presentation/d/11Y9bXy7SWRwu1QKCRNuUOAwn7pQW8PY_hR8nzHXBRkM/edit#slide=id.g81d6115fab_0_0)


## Data Sets



## Pseudo code


## Tech Stack


## Engineering Details
The project was implemented in stages that reflect the directory structure of this repository: [ingestion](/ingestion), [data processing](/data-processing), [database build](/database-scripts), and [data exposure/front-end](/app). See README.md in those directories for more details. Also see [presentation slides](https://docs.google.com/presentation/d/1q7Qm1ukmDi7Bal3UjiNw1xl4ZdLYwZme4oiog2zXNJY/edit#slide=id.p).

Spark did the distributed processing needed to extract the data from its home server (marinecadastre.gov), store it in an S3 data lake, and apply geo-hashing and processing steps. Intermediate data structures were stored in S3 as parquet files awaiting appends from the roughly five years of earlier data that is accessible only in ESRI geodatabase (.gdb) format. Looping back to retrieve that data and parse it is on the to-do list.

The processed data were stored in a PostgreSQL database hosted on AWS EC2 and pre-processed tables for the app were built using SQL queries.

Flask, a python microserver framework, was used to build the API and GUI to expose the data to end users.

## Next Step
