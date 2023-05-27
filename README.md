## Problem Statement
Explore Restaurant Food Inspections results over the period of 10 years to derive insights on

- Inspection over the years
- Inspection results
- Which restaurants were most inspected
- Which restaurants were involved in most violation
- Other inferences as observed during the course of visualization

## Dataset
Chicago Food Inspection - [OpenData](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5)

## Architecture Diagram
![architecture_diagram](https://github.com/dev-kudli/Food-Inspection-USA/assets/53204171/05ed1d01-f9e5-4ca9-97b2-898b26855c88)

## Data Modelling

| Facts (Pink) | Dimensions (Blue) |
| ------------- | ------------- |
| fact_food_inspection | dim_facility_type |
| fact_inspection_violation  | dim_geography |
|  | dim_inspection_result |
|  | dim_inspection_result |
|  | dim_inspection_type |
|  | dim_inspection_restaurant |
|  | dim_risk_category |

![data model](https://github.com/dev-kudli/Food-Inspection-USA/assets/53204171/95ec58e2-8baf-467d-a0ae-073fe14dad11)

## DBT Workflow
![dag](https://github.com/dev-kudli/Food-Inspection-USA/assets/53204171/2f3821b2-5b9e-4126-871d-82bbbadf56e5)

## Visualization
- Dashboard A
![Dashboard_A](https://github.com/dev-kudli/Food-Inspection-USA/assets/53204171/3762eea4-b265-44a3-8521-d5110df2d161)

- Dashboard B
![Dashboard_B](https://github.com/dev-kudli/Food-Inspection-USA/assets/53204171/97d2d586-7c2a-4251-89d6-e429a5440ceb)

- Dashboard C
![Dashboard_C](https://github.com/dev-kudli/Food-Inspection-USA/assets/53204171/9da65495-7f16-48cb-853b-88a5838bc29c)
