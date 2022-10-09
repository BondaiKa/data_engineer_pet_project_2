# data_engineer_pet_project_2

data engineer pet project 2 for a company

## Installation

Download yelp dataset:

## Implementation part with my comments and thoughts.

### How to implement weekly basis report using review dataset

- I have decided to split review dataset on day. Because the size of the dataset much more than others.     
  Then, if user writes particular date like 08.10.2022 we should clarify,
  that is the definition of `week` (is it last 7 days or from Monday to Sunday) etc.
  So as a solution the user writes year and number of start (starts from January)

- Another issue would be saving weekly report if user write random date like 5.10.2022 or 9.10.2022. So how can we store
  on datalake weekly reports. That's why splitting by days is the best colution
  Probably in real project the size of dateset would be more.
  So splitting by date is very convenient to create `daily`,`weekly`,`monthly` report

### How I have cleaned the data.

- During my experience I know 2 ways how to clean data. Firstly, just **remove rows with missing data**.
  It has advantages and disadvantages. On the one hand, it is the fastest way of implementing. On the other hand, We
  could miss important data and find wrong correlation etc.
  Yelp dataset files are related to each other, so removing on `user` dataset one row could influence on cleaning `tip`
  , `review`. To solve this we should implement join and filter by the values.

- Another solution is **finding avg/mean value according to type of distribution** (poisson, gaussian etc). However,
  missed values could be anomaly is it is not precise solution.

To simplify the cleaning stage, I use first approach. 


