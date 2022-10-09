# data_engineer_pet_project_2
data engineer pet project 2 for a company


## Installation
Download yelp dataset:

##

### 
I have decided to split review on day. Because the size of the dataset much more than others.   
Then, if user writes particular date like 08.10.2022 we should clarify,
that is the definition of `week` (is it last 7 days or from Monday to Sunday) etc. So as a solution the user writes year and number of start (starts from January)
Another issue would be saving weekly report if user write random date like 5.10.2022 or 9.10.2022. So how can we store on datalake weekly reports. That's why splitting by days is the best colution 
Probably in real project the size of dateset would be more.
So splitting by date is very convenient to create `daily`,`weekly`,`monthly` report
